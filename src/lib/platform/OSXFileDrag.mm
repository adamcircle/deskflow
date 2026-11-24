/*
 * Deskflow -- mouse and keyboard sharing utility
 * SPDX-FileCopyrightText: (C) 2026 Deskflow Developers
 * SPDX-License-Identifier: GPL-2.0-only WITH LicenseRef-OpenSSL-Exception
 */

// The macOS side of dragging files between computers. See OSXFileDrag.h for the design and
// deskflow/FileDragTypes.h for the protocol. Compiled with ARC (see CMakeLists.txt).
//
// Threads:
// - core thread: the OSXFileDrag methods (called by OSXComputer), CoreTask events;
// - main thread: every window, view and the AppKit drag (QApplication runs NSApp there);
// - writer thread (destination): staging writes and acks, in the order the messages came;
// - sender threads (source): one FileDragSender per drag that is being sent;
// - promise queue (destination): Finder's writePromiseToURL calls, which wait for the data.
//
// Both roles run on either computer: server to client and client to server.

#include "platform/OSXFileDrag.h"

#include "base/Event.h"
#include "base/EventTypes.h"
#include "base/IEventQueue.h"
#include "base/Log.h"
#include "deskflow/FileDragCodec.h"
#include "deskflow/FileDragEvent.h"
#include "deskflow/FileDragGeometry.h"
#include "deskflow/FileDragPolicy.h"
#include "deskflow/FileDragSender.h"
#include "deskflow/FileDragStaging.h"

#import <AppKit/AppKit.h>
#include <ApplicationServices/ApplicationServices.h>
#import <UniformTypeIdentifiers/UniformTypeIdentifiers.h>
#include <pthread.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <filesystem>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#if !__has_feature(objc_arc)
#error "OSXFileDrag.mm must be compiled with -fobjc-arc"
#endif

namespace fs = std::filesystem;
namespace fd = deskflow::filedrag;

using State = OSXFileDrag::State;
using StatePtr = std::shared_ptr<State>;
using WeakState = std::weak_ptr<State>;

namespace {

//! Side of the window under the cursor that starts the drag on a secondary
constexpr int32_t kLauncherSide = 64;
//! Side of the window that holds a parked local drag on the primary
constexpr int32_t kCatcherSide = 160;
//! How far from the edges of its display a local drag is parked (clear of the Dock)
constexpr int32_t kParkInset = 200;
//! How long the start window waits for its button press
constexpr double kLauncherTimeoutSeconds = 5.0;
//! Delay between showing the start window and pressing on it
constexpr double kLauncherPressDelaySeconds = 0.03;
//! After a drop, how long the target may take before it asks for the first item
constexpr double kNoPromiseTimeoutSeconds = 30.0;
//! A promised item whose data stops arriving for this long fails the drag
constexpr auto kStallTimeout = std::chrono::seconds(120);
//! Drops larger than this (or containing any folder) publish Finder progress while arriving
constexpr uint64_t kFinderProgressMinBytes = 50ull * 1024 * 1024;
//! How often Finder progress is updated while bytes arrive (at most ~10 times per second)
constexpr auto kProgressUpdateInterval = std::chrono::milliseconds(100);
//! How often a parked drag that is not over the catcher yet is moved there again, and how many times
constexpr CGKeyCode kEscapeKeyCode = 53;
constexpr double kParkRetrySeconds = 0.03;
constexpr int kParkTries = 10;
//! Image size of a dragged item, in points
constexpr CGFloat kDragImageSide = 64;
//! Pixel size of the icon sent to the other computer
constexpr int kIconPixels = 96;
//! Pixels per point of a picture of the whole drag, if it fits fd::kMaxIconSide like that
constexpr CGFloat kPreviewPixelsPerPoint = kIconPixels / kDragImageSide;
//! A larger drag gets a coarser picture, down to this; what is farther from the cursor is cut off
constexpr CGFloat kPreviewMinPixelsPerPoint = 0.75;

enum class DestPhase : int
{
  Waiting, ///< start window shown, waiting for the button press
  Looping, ///< the AppKit drag runs
  Dropped, ///< dropped; the target may still be asking for the items
  Done
};

//! A function run on the core thread (posted from another thread as an event)
class CoreTask : public EventData
{
public:
  explicit CoreTask(std::function<void()> task) : m_task(std::move(task))
  {
  }
  ~CoreTask() override = default;

  void run() const
  {
    if (m_task) {
      m_task();
    }
  }

private:
  std::function<void()> m_task;
};

//! What the primary is dragging, ready to send
struct Offer
{
  std::vector<fs::path> paths;
  NSInteger changeCount = -1; ///< of the drag pasteboard, tells one drag from the next
  fd::ItemList items;
  std::vector<fs::path> sources;
  fd::Icon icon;
  bool ready = false;
  double scanMs = 0; ///< how long building the item list and the icon took
};

//! Streams the files of one drag on its own thread (primary)
class SourceSession
{
public:
  using Send = std::function<void(fd::Message &&)>;

  SourceSession(uint32_t dragId, const Offer &offer, const Send &send)
      : m_dragId(dragId),
        m_items(offer.items.size()),
        m_send(send),
        m_sender(dragId, offer.items, offer.sources, send)
  {
  }

  ~SourceSession()
  {
    stop();
    if (m_thread.joinable()) {
      m_thread.join();
    }
  }

  SourceSession(const SourceSession &) = delete;
  SourceSession &operator=(const SourceSession &) = delete;

  //! Starts sending; false if it already was (or was stopped first)
  bool start()
  {
    std::scoped_lock lock(m_mutex);
    if (m_started || m_stopping) {
      return false;
    }
    m_started = true;
    m_thread = std::thread([this] { run(); });
    return true;
  }

  void onAck(uint32_t seq)
  {
    {
      std::scoped_lock lock(m_mutex);
      m_acks.push_back(seq);
    }
    m_cond.notify_one();
  }

  void stop()
  {
    {
      std::scoped_lock lock(m_mutex);
      m_stopping = true;
    }
    m_cond.notify_one();
  }

  bool isDone() const
  {
    return m_done;
  }

  size_t itemCount() const
  {
    return m_items;
  }

private:
  void run()
  {
    pthread_setname_np("deskflow file drag sender");
    m_sender.pump();
    while (!m_sender.hasFailed() && !m_sender.isFinished()) {
      std::deque<uint32_t> acks;
      {
        std::unique_lock lock(m_mutex);
        m_cond.wait_for(lock, std::chrono::milliseconds(100), [this] { return m_stopping || !m_acks.empty(); });
        if (m_stopping) {
          break;
        }
        acks.swap(m_acks);
      }
      for (uint32_t seq : acks) {
        m_sender.onAck(seq);
      }
      m_sender.pump();
    }
    if (m_sender.hasFailed()) {
      LOG_WARN("file drag %u: sending failed: %s", m_dragId, fd::cancelReasonName(m_sender.failReason()));
      m_send(fd::Message::cancel(m_dragId, m_sender.failReason()));
    } else if (m_sender.isFinished()) {
      LOG_INFO("file drag %u: all %llu bytes sent", m_dragId, static_cast<unsigned long long>(m_sender.bytesSent()));
    } else {
      LOG_DEBUG("file drag %u: sending stopped", m_dragId);
    }
    m_done = true;
  }

  uint32_t m_dragId;
  size_t m_items;
  Send m_send;
  fd::FileDragSender m_sender;
  std::thread m_thread;
  std::mutex m_mutex;
  std::condition_variable m_cond;
  std::deque<uint32_t> m_acks;
  bool m_started = false;
  bool m_stopping = false;
  std::atomic<bool> m_done{false};
};

//! One drag being received (secondary)
struct DestSession
{
  explicit DestSession(const fs::path &root) : staging(root)
  {
  }

  uint32_t id = 0;
  fd::FileDragStaging staging;
  fd::Icon icon;
  std::vector<uint32_t> tops; ///< top-level items: one file promise each

  std::atomic<DestPhase> phase{DestPhase::Waiting};
  std::atomic<bool> cancelled{false};
  std::atomic<fd::CancelReason> reason{fd::CancelReason::Unknown};
  std::atomic<bool> told{false}; ///< the server knows how it ended
  std::atomic<bool> finished{false};
  std::atomic<bool> promiseAsked{false};
  std::atomic<bool> droppedSent{false}; ///< the source was told to send the files
  CFAbsoluteTime begunAt = 0;           ///< when the item list arrived (timing in the log)
  int promisesLeft = 0;                 ///< main thread only
};

using DestPtr = std::shared_ptr<DestSession>;

} // namespace

@class DFFileDragController;

struct OSXFileDrag::State : public std::enable_shared_from_this<OSXFileDrag::State>
{
  IEventQueue *events = nullptr;
  void *eventTarget = nullptr;
  bool primary = true;
  Hooks hooks;

  std::atomic<bool> available{false};
  std::atomic<bool> stopping{false};

  //! main thread only
  DFFileDragController *controller = nil;

  // primary (source) ---------------------------------------------------------------
  //! the next left button release passes to the system (core and event tap threads)
  std::atomic<bool> parkedRelease{false};
  //! the cursor is on the other computer with a drag handed off (core thread writes, main thread reads)
  std::atomic<bool> away{false};
  //! the parked local drag is over a window of this app, which refuses it: letting go of it
  //! there drops nothing (main thread writes; core and event tap threads read)
  std::atomic<bool> parkedOver{false};
  //! the computer the next drag goes to waits for the files until the drop (kCapLazy; core
  //! thread writes before each hand-off, main thread reads)
  std::atomic<bool> peerLazy{false};
  //! that computer shows a picture of the whole drag (kCapPreview; same threads)
  std::atomic<bool> peerPreview{false};
  //! parking the drag moved the cursor by parkJumpX, parkJumpY; the mouse motion after it
  //! still contains that distance (main thread writes, event tap thread takes it)
  std::atomic<bool> parkJumped{false};
  std::mutex parkJumpMutex;
  int32_t parkJumpX = 0;
  int32_t parkJumpY = 0;
  //! the drag that is being handed off or sent (core thread writes, main thread reads)
  std::atomic<uint32_t> currentDragId{0};

  std::mutex sessionsMutex;
  std::map<uint32_t, std::unique_ptr<SourceSession>> sessions;

  // main thread only
  std::shared_ptr<Offer> offer;
  NSInteger previewSequence = -1; ///< the drag (its dragging sequence number) that preview shows
  fd::Icon preview;               ///< what the drag at the edge window looks like; empty if unknown
  bool overStrip = false;
  bool offered = false;
  bool parked = false;
  uint64_t scanGeneration = 0;
  uint64_t stripGeneration = 0;
  uint64_t parkGeneration = 0;
  bool watching = false;       ///< the edge windows are up
  unsigned watchEntered = 0;   ///< drags that reached an edge window during this watch
  unsigned watchUpdated = 0;   ///< draggingUpdated calls on an edge window during this watch
  bool catcherEntered = false; ///< the parked drag reached the catcher
  bool stripsParked = false;   ///< the edge windows stay up under a parked drag
  uint32_t parkedDragId = 0;
  int32_t parkAtX = 0; ///< where the parked drag waits
  int32_t parkAtY = 0;
  //! change count of the drag pasteboard last read ahead of the drag reaching an edge window
  NSInteger prebuiltChangeCount = -1;
  CFAbsoluteTime edgeReachedAt = 0; ///< when the files reached an edge window (timing in the log)

  // secondary (destination) --------------------------------------------------------
  std::mutex destMutex;
  std::map<uint32_t, DestPtr> dests;
  fd::Icon pendingIcon;
  uint32_t pendingIconId = 0;

  std::thread writer;
  std::mutex writerMutex;
  std::condition_variable writerCond;
  std::deque<fd::Message> writerQueue;
  bool writerStop = false;

  // main thread only
  DestPtr launcherSession;
  uint64_t launcherGeneration = 0;
  DestPtr loopSession;
  std::vector<DestPtr> pendingRemoval;
  int removalTries = 0;

  //! Runs \p task on the core thread
  void postToCore(std::function<void()> task)
  {
    if (stopping) {
      return;
    }
    events->addEvent(Event(EventTypes::FileDrag, this, static_cast<EventData *>(new CoreTask(std::move(task)))));
  }

  //! Posts a FileDragEvent for the server or client
  void postEvent(fd::FileDragEvent *data)
  {
    if (stopping) {
      delete data;
      return;
    }
    events->addEvent(Event(EventTypes::FileDrag, eventTarget, static_cast<EventData *>(data)));
  }

  //! Sends a message to the network peer (through the server or client)
  void sendPeer(const fd::Message &message)
  {
    postEvent(new fd::FileDragEvent(fd::FileDragEvent::Kind::Send, message));
  }
};

namespace {

//
// helpers
//

CGPoint currentCursor()
{
  CGEventRef event = CGEventCreate(nullptr);
  const CGPoint point = event != nullptr ? CGEventGetLocation(event) : CGPointZero;
  if (event != nullptr) {
    CFRelease(event);
  }
  return point;
}

//! Active displays in global display coordinates (points, top left origin)
std::vector<fd::EdgeRect> activeDisplays()
{
  std::vector<fd::EdgeRect> displays;
  uint32_t count = 0;
  if (CGGetActiveDisplayList(0, nullptr, &count) != kCGErrorSuccess || count == 0) {
    return displays;
  }
  std::vector<CGDirectDisplayID> ids(count);
  if (CGGetActiveDisplayList(count, ids.data(), &count) != kCGErrorSuccess) {
    return displays;
  }
  for (uint32_t i = 0; i < count; ++i) {
    const CGRect b = CGDisplayBounds(ids[i]);
    displays.push_back(
        {static_cast<int32_t>(b.origin.x), static_cast<int32_t>(b.origin.y), static_cast<int32_t>(b.size.width),
         static_cast<int32_t>(b.size.height)}
    );
  }
  return displays;
}

//! Window frame (bottom left origin) for a rectangle in global display coordinates
NSRect cocoaFrame(const fd::EdgeRect &r)
{
  const auto mainHeight = static_cast<int32_t>(CGDisplayBounds(CGMainDisplayID()).size.height);
  const auto f = fd::flipVertical(r, mainHeight);
  return NSMakeRect(f.x, f.y, f.w, f.h);
}

void dispatchMain(dispatch_block_t block)
{
  dispatch_async(dispatch_get_main_queue(), block);
}

void dispatchMainAfter(double seconds, dispatch_block_t block)
{
  dispatch_after(
      dispatch_time(DISPATCH_TIME_NOW, static_cast<int64_t>(seconds * NSEC_PER_SEC)), dispatch_get_main_queue(), block
  );
}

/**
 * Level of a window that takes a drag. The system looks for the window under a drag below
 * the drag image, which is a window at the dragging level: a window at or above that level
 * (a screen saver level one, say) is never offered a drag. \p below is how many levels
 * under the drag image the window goes; still above the Dock, the menu bar and menus.
 */
NSInteger levelBelowDragImage(int below)
{
  return CGWindowLevelForKey(kCGDraggingWindowLevelKey) - below;
}

//! Level of the edge windows
NSInteger edgeWindowLevel()
{
  return levelBelowDragImage(2);
}

//! Level of the windows that hold or refuse a drag: above the edge windows
NSInteger catcherWindowLevel()
{
  return levelBelowDragImage(1);
}

const char *activationPolicyName(NSApplicationActivationPolicy policy)
{
  switch (policy) {
  case NSApplicationActivationPolicyRegular:
    return "regular";
  case NSApplicationActivationPolicyAccessory:
    return "accessory";
  case NSApplicationActivationPolicyProhibited:
    return "prohibited";
  }
  return "?";
}

std::string toUtf8(NSString *s)
{
  const char *utf8 = s.UTF8String;
  return utf8 != nullptr ? std::string(utf8) : std::string();
}

NSString *toNSString(const std::string &s)
{
  NSString *result = [NSString stringWithUTF8String:s.c_str()];
  return result != nil ? result : @"";
}

/**
 * The files and folders on a drag pasteboard, as paths. Finder may put file reference URLs
 * ("file:///.file/id=...") there; they are resolved, and skipped if that fails.
 */
std::vector<fs::path> filePathsOnPasteboard(NSPasteboard *pasteboard)
{
  std::vector<fs::path> paths;
  NSArray<NSURL *> *urls = [pasteboard readObjectsForClasses:@[ NSURL.class ]
                                                     options:@{
                                                       NSPasteboardURLReadingFileURLsOnlyKey : @YES
                                                     }];
  for (NSURL *url in urls) {
    NSURL *resolved = url.filePathURL;
    if (resolved == nil) {
      LOG_WARN("a dragged item's file reference cannot be resolved; it is left out");
      continue;
    }
    const char *fsPath = resolved.fileSystemRepresentation;
    if (fsPath == nullptr || *fsPath == '\0') {
      continue;
    }
    const std::string path(fsPath);
    if (fd::isFileReferencePath(path)) {
      LOG_WARN("a dragged item is still a file reference after resolving; it is left out");
      continue;
    }
    paths.emplace_back(path);
  }
  return paths;
}

//! Draw into the straight-alpha, top-row-first bitmap used by the Icon message
fd::Icon renderIcon(uint16_t width, uint16_t height, void (^draw)(CGContextRef))
{
  std::string rgba(static_cast<size_t>(width) * height * 4, '\0');
  CGColorSpaceRef space = CGColorSpaceCreateWithName(kCGColorSpaceSRGB);
  CGContextRef context = CGBitmapContextCreate(
      rgba.data(), width, height, 8, width * 4, space,
      static_cast<uint32_t>(kCGImageAlphaPremultipliedLast) | static_cast<uint32_t>(kCGBitmapByteOrder32Big)
  );
  CGColorSpaceRelease(space);
  if (context == nullptr) {
    return {};
  }
  CGContextSetInterpolationQuality(context, kCGInterpolationHigh);
  draw(context);
  CGContextRelease(context);
  fd::unpremultiplyRgba(rgba);
  return {width, height, std::move(rgba)};
}

//! Drag image of a file or folder, RGBA with straight alpha (as the wire carries it)
fd::Icon iconForPath(const fs::path &path)
{
  NSImage *image = [[NSWorkspace sharedWorkspace] iconForFile:toNSString(path.string())];
  if (image == nil) {
    return {};
  }
  NSRect rect = NSMakeRect(0, 0, kIconPixels, kIconPixels);
  CGImageRef cgImage = [image CGImageForProposedRect:&rect context:nil hints:nil];
  if (cgImage == nullptr) {
    return {};
  }
  return renderIcon(kIconPixels, kIconPixels, ^(CGContextRef context) {
    CGContextDrawImage(context, CGRectMake(0, 0, kIconPixels, kIconPixels), cgImage);
  });
}

/**
 * What the drag over \p view looks like, as a picture with the cursor at its centre (the
 * margins that takes are transparent), marked with its width in points. Empty if the drag
 * shows no images or drawing them takes too long.
 */
fd::Icon captureDragImage(id<NSDraggingInfo> sender, NSView *view, NSUInteger &itemCount)
{
  const NSPoint cursor = [view convertPoint:sender.draggingLocation fromView:nil];
  struct Layer
  {
    NSImage *image;
    NSRect frame;
  };
  __block std::vector<Layer> layers;
  __block NSRect bounds = NSZeroRect;
  // Finish small stacks even if AppKit is warming up. For large drags, check the budget between items and layers.
  constexpr size_t kSmallStack = 8;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(25);
  __block bool timedOut = false;
  [sender enumerateDraggingItemsWithOptions:0
      forView:view
      classes:@[ NSPasteboardItem.class, NSURL.class ]
      searchOptions:@{}
      usingBlock:^(NSDraggingItem *item, NSInteger index, BOOL *stop) {
        if (index >= static_cast<NSInteger>(kSmallStack) && std::chrono::steady_clock::now() >= deadline) {
          timedOut = true;
          *stop = YES;
          return;
        }
        const NSRect frame = item.draggingFrame;
        if (NSIsEmptyRect(frame)) {
          return;
        }
        const auto before = layers.size();
        for (NSDraggingImageComponent *component in item.imageComponents) {
          const NSRect r = NSOffsetRect(component.frame, frame.origin.x, frame.origin.y);
          if (NSIsEmptyRect(r) || !std::isfinite(NSMinX(r)) || !std::isfinite(NSMaxX(r)) || !std::isfinite(NSMinY(r)) ||
              !std::isfinite(NSMaxY(r))) {
            continue;
          }
          id contents = component.contents;
          NSImage *image = nil;
          if ([contents isKindOfClass:NSImage.class]) {
            image = contents;
          } else if (contents && CFGetTypeID((__bridge CFTypeRef)contents) == CGImageGetTypeID()) {
            image = [[NSImage alloc] initWithCGImage:(__bridge CGImageRef)contents size:r.size];
          }
          if (image != nil) {
            bounds = NSUnionRect(bounds, r);
            layers.push_back({image, r});
          }
        }
        if (layers.size() > before) {
          ++itemCount;
        }
      }];
  if (layers.empty() || timedOut) {
    return {};
  }
  const CGFloat w = 2 * std::max(cursor.x - NSMinX(bounds), NSMaxX(bounds) - cursor.x);
  const CGFloat h = 2 * std::max(cursor.y - NSMinY(bounds), NSMaxY(bounds) - cursor.y);
  if (!(w > 0 && h > 0 && std::isfinite(w) && std::isfinite(h))) {
    return {};
  }
  const CGFloat scale =
      std::clamp(fd::kMaxIconSide / std::max(w, h), kPreviewMinPixelsPerPoint, kPreviewPixelsPerPoint);
  const auto pixels = [scale](CGFloat points) {
    return static_cast<uint16_t>(std::clamp<CGFloat>(std::ceil(points * scale), 1, fd::kMaxIconSide));
  };
  const uint16_t width = pixels(w);
  const uint16_t height = pixels(h);
  auto icon = renderIcon(width, height, ^(CGContextRef context) {
    CGContextTranslateCTM(context, width / 2.0, height / 2.0);
    CGContextScaleCTM(context, scale, scale);
    CGContextTranslateCTM(context, -cursor.x, -cursor.y);
    [NSGraphicsContext saveGraphicsState];
    NSGraphicsContext.currentContext = [NSGraphicsContext graphicsContextWithCGContext:context flipped:NO];
    for (size_t i = 0; i < layers.size(); ++i) {
      if (i >= kSmallStack && std::chrono::steady_clock::now() >= deadline) {
        timedOut = true;
        break;
      }
      const auto &layer = layers[i];
      [layer.image drawInRect:layer.frame fromRect:NSZeroRect operation:NSCompositingOperationSourceOver fraction:1.0];
    }
    [NSGraphicsContext restoreGraphicsState];
  });
  if (timedOut) {
    return {};
  }
  fd::setIconPointWidth(icon, static_cast<uint16_t>(std::lround(width / scale)));
  return icon;
}

//! The icon the other computer sent, as a drag image; nil if there is none
NSImage *imageFromIcon(const fd::Icon &icon)
{
  if (icon.isEmpty() || !icon.isValid()) {
    return nil;
  }
  NSBitmapImageRep *rep = [[NSBitmapImageRep alloc] initWithBitmapDataPlanes:nullptr
                                                                  pixelsWide:icon.width
                                                                  pixelsHigh:icon.height
                                                               bitsPerSample:8
                                                             samplesPerPixel:4
                                                                    hasAlpha:YES
                                                                    isPlanar:NO
                                                              colorSpaceName:NSDeviceRGBColorSpace
                                                                bitmapFormat:NSBitmapFormatAlphaNonpremultiplied
                                                                 bytesPerRow:icon.width * 4
                                                                bitsPerPixel:32];
  if (rep == nil || rep.bitmapData == nullptr) {
    return nil;
  }
  memcpy(rep.bitmapData, icon.rgba.data(), static_cast<size_t>(icon.width) * icon.height * 4);
  NSSize size = NSMakeSize(kDragImageSide, kDragImageSide);
  if (const uint16_t points = fd::iconPointWidth(icon); points > 0) {
    // a picture of the whole drag: as large as on the computer it comes from
    size = NSMakeSize(points, static_cast<CGFloat>(points) * icon.height / icon.width);
    memset(rep.bitmapData, 0, 4); // the pixel that says so
  }
  NSImage *image = [[NSImage alloc] initWithSize:size];
  [image addRepresentation:rep];
  return image;
}

//! The system's generic file or folder icon, until (or if no) real icon is known
NSImage *placeholderImage(bool folder)
{
  NSImage *image = [[[NSWorkspace sharedWorkspace] iconForContentType:folder ? UTTypeFolder : UTTypeData] copy];
  image.size = NSMakeSize(kDragImageSide, kDragImageSide);
  return image;
}

//! The type a file promise announces for a staged item
NSString *typeIdentifierFor(const fd::FileDragStaging::Entry &entry)
{
  if (entry.isDirectory) {
    return UTTypeFolder.identifier;
  }
  NSString *extension = toNSString(entry.localPath).pathExtension;
  UTType *type = extension.length > 0 ? [UTType typeWithFilenameExtension:extension] : nil;
  if (type == nil || ![type conformsToType:UTTypeData]) {
    type = UTTypeData;
  }
  return type.identifier;
}

NSError *promiseError(NSString *what)
{
  return [NSError errorWithDomain:@"org.deskflow.filedrag" code:1 userInfo:@{NSLocalizedDescriptionKey : what}];
}

// forward declarations (main thread unless noted)
void offerReady(StatePtr s);
void offerLost(StatePtr s);
void onFilesEntered(StatePtr s, std::vector<fs::path> paths, id<NSDraggingInfo> sender, NSView *view);
void checkParked(StatePtr s, uint64_t generation, int attempt);
void unpark(StatePtr s, const char *why);
void startDrag(StatePtr s, DestPtr session, NSView *view, NSEvent *event);
void loopEnded(StatePtr s, NSDragOperation operation);
void cancelSession(StatePtr s, DestPtr session, fd::CancelReason reason, bool tellServer); // any thread
void writePromise(
    StatePtr s, uint32_t dragId, uint32_t index, NSURL *url, void (^completion)(NSError *)
);                                         // promise queue
DestPtr findDest(StatePtr s, uint32_t id); // any thread

int64_t toProgressUnits(uint64_t bytes)
{
  return static_cast<int64_t>(std::min<uint64_t>(bytes, static_cast<uint64_t>(INT64_MAX)));
}

bool shouldShowFinderProgress(const fd::FileDragStaging &staging)
{
  if (staging.totalBytes() > kFinderProgressMinBytes) {
    return true;
  }
  return std::any_of(staging.entries().begin(), staging.entries().end(), [](const auto &entry) {
    return entry.isDirectory;
  });
}

//! Publishes Finder progress on a reserved placeholder while a promised item arrives
struct FinderProgress
{
  uint32_t dragId = 0;
  std::string displayName;
  fs::path placeholder;
  bool isDirectory = false;
  bool hasPlaceholder = false;
  NSProgress *progress = nil;
  uint64_t lastReported = 0;

  ~FinderProgress()
  {
    end("cancelled", true);
  }

  void begin(
      const StatePtr &s, const DestPtr &session, uint32_t index, const fs::path &folder, const std::string &name,
      bool directory
  )
  {
    dragId = session->id;
    isDirectory = directory;
    std::string error;
    if (!fd::FileDragStaging::reservePlaceholder(folder, name, directory, placeholder, &error)) {
      LOG_WARN(
          "file drag %u: cannot reserve \"%s\" in %s for Finder progress: %s", dragId, name.c_str(), folder.c_str(),
          error.c_str()
      );
      return;
    }
    hasPlaceholder = true;
    displayName = placeholder.filename().string();

    const uint64_t total = session->staging.itemTotalBytes(index);
    const uint64_t received = session->staging.itemReceivedBytes(index);
    lastReported = received;

    NSURL *itemUrl = [[NSURL fileURLWithPath:toNSString(placeholder.string())
                                 isDirectory:directory] URLByStandardizingPath];
    progress = [[NSProgress alloc] initWithParent:nil
                                         userInfo:@{
                                           NSProgressFileOperationKindKey : NSProgressFileOperationKindCopying,
                                           NSProgressFileURLKey : itemUrl,
                                         }];
    progress.kind = NSProgressKindFile;
    progress.totalUnitCount = toProgressUnits(total);
    progress.completedUnitCount = toProgressUnits(received);
    progress.cancellable = YES;
    progress.pausable = NO;
    WeakState weakState = s;
    std::weak_ptr<DestSession> weakSession = session;
    progress.cancellationHandler = ^{
      auto st = weakState.lock();
      auto sess = weakSession.lock();
      if (st && sess) {
        cancelSession(st, sess, fd::CancelReason::User, true);
      }
    };
    [progress publish];
    LOG_INFO(
        "file drag %u: showing progress in Finder for \"%s\" (%llu bytes)", dragId, displayName.c_str(),
        static_cast<unsigned long long>(total)
    );
  }

  void update(uint64_t received)
  {
    if (progress == nil || received == lastReported) {
      return;
    }
    lastReported = received;
    progress.completedUnitCount = toProgressUnits(received);
  }

  void finishUnits()
  {
    if (progress != nil) {
      progress.completedUnitCount = progress.totalUnitCount;
    }
  }

  void end(const char *outcome, bool removeIfPlaceholder)
  {
    if (progress != nil) {
      progress.cancellationHandler = nil;
      [progress unpublish];
      progress = nil;
      LOG_INFO("file drag %u: Finder progress ended for \"%s\" (%s)", dragId, displayName.c_str(), outcome);
    }
    if (removeIfPlaceholder && hasPlaceholder) {
      fd::FileDragStaging::removePlaceholder(placeholder, isDirectory);
      hasPlaceholder = false;
    }
  }
};

} // namespace

//
// windows
//

//! Borderless, never key, on every Space (full screen ones too), above the Dock and menu bar
@interface DFOverlayPanel : NSPanel
- (instancetype)initWithView:(NSView *)view level:(NSInteger)level;
- (void)placeAt:(const fd::EdgeRect &)rect;
- (void)logAs:(const char *)what;
@end

@implementation DFOverlayPanel

- (instancetype)initWithView:(NSView *)view level:(NSInteger)level
{
  self = [super initWithContentRect:NSMakeRect(0, 0, 4, 4)
                          styleMask:NSWindowStyleMaskBorderless | NSWindowStyleMaskNonactivatingPanel
                            backing:NSBackingStoreBuffered
                              defer:NO];
  if (self != nil) {
    self.releasedWhenClosed = NO;
    self.opaque = NO;
    self.hasShadow = NO;
    // all but invisible, yet not fully transparent: the window server passes clicks and
    // drags through pixels whose alpha is exactly 0
    self.backgroundColor = [NSColor colorWithCalibratedWhite:0.5 alpha:0.005];
    self.collectionBehavior = NSWindowCollectionBehaviorCanJoinAllSpaces |
                              NSWindowCollectionBehaviorFullScreenAuxiliary | NSWindowCollectionBehaviorStationary |
                              NSWindowCollectionBehaviorIgnoresCycle;
    // panels hide when their app is not active by default, and this app never is
    self.hidesOnDeactivate = NO;
    self.becomesKeyOnlyIfNeeded = YES;
    // set last: floatingPanel and friends reset the level
    self.level = level;
    self.animationBehavior = NSWindowAnimationBehaviorNone;
    self.ignoresMouseEvents = NO;
    self.contentView = view;
  }
  return self;
}

- (BOOL)canBecomeKeyWindow
{
  return NO;
}

- (BOOL)canBecomeMainWindow
{
  return NO;
}

// A drag that stays over a window which is neither key nor main gets "spring loaded": after
// about 0.75 s AppKit brings that window to the front and activates its app. A parked drag
// stays over these windows for as long as the cursor is on the other computer, so this app
// came forward during every drag (taking the focus from the user's app) and an edge window
// that was hidden for the hand-off showed again. AppKit does not spring a main window.
- (BOOL)isMainWindow
{
  return YES;
}

- (void)makeKeyAndOrderFront:(id)sender
{
  // these windows show with placeAt: only. This is the spring loading above: seen in the
  // log, it means the system still tries to bring the window under a drag forward
  LOG_INFO("file drag: window %ld was asked to come to the front; it stays", static_cast<long>(self.windowNumber));
}

- (void)placeAt:(const fd::EdgeRect &)rect
{
  [self setFrame:cocoaFrame(rect) display:NO];
  self.alphaValue = 1.0;
  self.ignoresMouseEvents = NO;
  [self orderFrontRegardless];
}

- (void)logAs:(const char *)what
{
  // the window number tells whether a drag ever was over this window: the system log names the
  // window under a drag (log show --predicate 'category == "coredraglistener"': "Drag did
  // change target, pid=..., to=<window number>")
  const NSRect f = self.frame;
  NSString *screen = self.screen != nil ? self.screen.localizedName : @"none";
  LOG_INFO(
      "file drag: %s: window %ld, frame %.0f,%.0f %.0fx%.0f (from the bottom left), level %ld, visible %d, on the "
      "active space %d, takes the pointer %d, screen \"%s\"",
      what, static_cast<long>(self.windowNumber), f.origin.x, f.origin.y, f.size.width, f.size.height,
      static_cast<long>(self.level), self.isVisible, self.isOnActiveSpace, !self.ignoresMouseEvents,
      toUtf8(screen).c_str()
  );
}

@end

/**
 * Primary: an edge window (notices a drag of files at a switch edge) or the catcher (holds
 * the local drag while the cursor is on another computer). Neither ever takes a drop.
 */
@interface DFDropView : NSView
- (instancetype)initWithState:(const StatePtr &)state catcher:(BOOL)catcher;
- (void)dragLeft:(const char *)why;
@end

@implementation DFDropView {
  WeakState _state;
  BOOL _catcher;
  BOOL _files;
}

- (instancetype)initWithState:(const StatePtr &)state catcher:(BOOL)catcher
{
  self = [super initWithFrame:NSMakeRect(0, 0, 4, 4)];
  if (self != nil) {
    _state = state;
    _catcher = catcher;
    [self registerForDraggedTypes:@[ NSPasteboardTypeFileURL ]];
  }
  return self;
}

- (NSDragOperation)draggingEntered:(id<NSDraggingInfo>)sender
{
  auto s = _state.lock();
  if (!s || _catcher) {
    // the parked local drag must never drop anything here
    if (s) {
      s->parkedOver = true;
    }
    if (s && !s->catcherEntered) {
      s->catcherEntered = true;
      LOG_INFO(
          "file drag: the parked drag is over the catcher (window %ld)", static_cast<long>(self.window.windowNumber)
      );
    }
    return NSDragOperationNone;
  }
  if (s->parked) {
    // the parked local drag is (back) at the edge: nothing is offered and nothing drops here
    s->parkedOver = true;
    return NSDragOperationNone;
  }
  if (sender.draggingSource != nil) {
    // a drag this app started shows files that came from another computer: it never
    // goes back there (or on to a third computer) as a new drag
    LOG_INFO("file drag: a drag of files from another computer reached the edge window; it stays here");
    return NSDragOperationNone;
  }
  NSPasteboard *pasteboard = sender.draggingPasteboard;
  ++s->watchEntered;
  LOG_INFO(
      "file drag: a drag entered edge window %ld (%lu types on its pasteboard, file URLs %s, source allows 0x%lx)",
      static_cast<long>(self.window.windowNumber), static_cast<unsigned long>(pasteboard.types.count),
      [pasteboard.types containsObject:NSPasteboardTypeFileURL] ? "yes" : "no",
      static_cast<unsigned long>(sender.draggingSourceOperationMask)
  );
  auto paths = filePathsOnPasteboard(pasteboard);
  _files = !paths.empty();
  if (!_files) {
    // pictures, text, links, promised files...: only files may cross, so this drag stays here
    LOG_INFO("a drag without files reached the edge window; it stays on this computer");
    return NSDragOperationNone;
  }
  LOG_INFO("a drag of %zu files or folders reached the edge window", paths.size());
  onFilesEntered(s, std::move(paths), sender, self);
  return sender.draggingSourceOperationMask & NSDragOperationCopy;
}

- (NSDragOperation)draggingUpdated:(id<NSDraggingInfo>)sender
{
  auto s = _state.lock();
  if (s && s->parked) {
    s->parkedOver = true;
    return NSDragOperationNone;
  }
  const NSDragOperation answer =
      (!_catcher && _files) ? (sender.draggingSourceOperationMask & NSDragOperationCopy) : NSDragOperationNone;
  if (s && !_catcher && s->watchUpdated++ == 0) {
    // once per watch: the drag stays over the edge window and is asked again as it moves
    LOG_INFO("file drag: the drag moves over the edge window, answer 0x%lx", static_cast<unsigned long>(answer));
  }
  return answer;
}

- (void)draggingExited:(id<NSDraggingInfo>)sender
{
  if (auto s = _state.lock(); s && s->parked) {
    // a parked drag moves from the edge window to the catcher, or something moved it off
    // the catcher: checkParked() puts it back
    s->parkedOver = false;
    if (_catcher) {
      s->catcherEntered = false;
      const uint64_t generation = s->parkGeneration;
      dispatchMainAfter(kParkRetrySeconds, ^{
        checkParked(s, generation, 0);
      });
    }
    _files = NO;
    return;
  }
  [self dragLeft:"left the edge window"];
}

- (BOOL)prepareForDragOperation:(id<NSDraggingInfo>)sender
{
  // the source must never move or copy anything because of us
  [self dragLeft:"let go"];
  return NO;
}

- (void)draggingEnded:(id<NSDraggingInfo>)sender
{
  [self dragLeft:"drag ended"];
}

- (void)dragLeft:(const char *)why
{
  auto s = _state.lock();
  if (!s) {
    return;
  }
  if (_catcher || s->parked) {
    // the parked drag ended, over the catcher or over the edge window it was offered from
    unpark(s, why);
    _files = NO;
    return;
  }
  if (_files) {
    s->overStrip = false;
    offerLost(s);
  }
  _files = NO;
}

@end

//! Secondary: under the cursor, gets the synthetic press and starts the drag from it
@interface DFLauncherView : NSView
- (instancetype)initWithState:(const StatePtr &)state;
@end

@implementation DFLauncherView {
  WeakState _state;
}

- (instancetype)initWithState:(const StatePtr &)state
{
  self = [super initWithFrame:NSMakeRect(0, 0, kLauncherSide, kLauncherSide)];
  if (self != nil) {
    _state = state;
  }
  return self;
}

- (BOOL)acceptsFirstMouse:(NSEvent *)event
{
  return YES;
}

- (BOOL)mouseDownCanMoveWindow
{
  return NO;
}

- (void)mouseDown:(NSEvent *)event
{
  auto s = _state.lock();
  if (!s) {
    return;
  }
  DestPtr session = std::move(s->launcherSession);
  s->launcherSession.reset();
  ++s->launcherGeneration;
  if (!session || session->phase != DestPhase::Waiting || session->cancelled) {
    LOG_DEBUG("file drag: the start window was pressed with no drag waiting");
    [self.window orderOut:nil];
    return;
  }
  LOG_INFO("file drag %u: the press reached the start window", session->id);
  startDrag(s, session, self, event);
}

@end

//! Secondary: ends a cancelled drag by refusing it, so it drops nowhere
@interface DFRefuseView : NSView
@end

@implementation DFRefuseView

- (instancetype)init
{
  self = [super initWithFrame:NSMakeRect(0, 0, kLauncherSide, kLauncherSide)];
  if (self != nil) {
    NSArray<NSPasteboardType> *types =
        [NSFilePromiseReceiver.readableDraggedTypes arrayByAddingObject:NSPasteboardTypeFileURL];
    [self registerForDraggedTypes:types];
  }
  return self;
}

- (NSDragOperation)draggingEntered:(id<NSDraggingInfo>)sender
{
  return NSDragOperationNone;
}

- (NSDragOperation)draggingUpdated:(id<NSDraggingInfo>)sender
{
  return NSDragOperationNone;
}

- (BOOL)prepareForDragOperation:(id<NSDraggingInfo>)sender
{
  return NO;
}

@end

//
// controller: owns the windows, is the drag source and the file promise delegate
//

@interface DFFileDragController : NSObject <NSDraggingSource, NSFilePromiseProviderDelegate>
- (instancetype)initWithState:(const StatePtr &)state;
- (void)placeStrips:(const std::vector<fd::EdgeRect> &)strips;
- (void)logStrips;
- (void)hideStrips;
- (void)showCatcher:(const fd::EdgeRect &)rect;
- (void)hideCatcher;
- (void)showLauncher:(const fd::EdgeRect &)rect;
- (void)launcherOutOfTheWay;
- (void)hideLauncher;
- (void)showRefuser:(const fd::EdgeRect &)rect;
- (void)hideRefuser;
- (void)teardown;
@end

@implementation DFFileDragController {
  WeakState _state;
  NSMutableArray<DFOverlayPanel *> *_strips;
  DFOverlayPanel *_catcher;
  DFOverlayPanel *_launcher;
  DFOverlayPanel *_refuser;
  NSOperationQueue *_promiseQueue;
}

- (instancetype)initWithState:(const StatePtr &)state
{
  self = [super init];
  if (self != nil) {
    _state = state;
    _strips = [NSMutableArray array];
    _promiseQueue = [[NSOperationQueue alloc] init];
    _promiseQueue.name = @"org.deskflow.filedrag.promises";
    _promiseQueue.qualityOfService = NSQualityOfServiceUserInitiated;
    _promiseQueue.maxConcurrentOperationCount = 4;
  }
  return self;
}

// primary windows

- (void)placeStrips:(const std::vector<fd::EdgeRect> &)strips
{
  auto s = _state.lock();
  if (!s) {
    return;
  }
  while (_strips.count < strips.size()) {
    DFDropView *view = [[DFDropView alloc] initWithState:s catcher:NO];
    [_strips addObject:[[DFOverlayPanel alloc] initWithView:view level:edgeWindowLevel()]];
  }
  for (NSUInteger i = 0; i < _strips.count; ++i) {
    if (i < strips.size()) {
      [_strips[i] placeAt:strips[i]];
    } else {
      [_strips[i] orderOut:nil];
    }
  }
}

- (void)logStrips
{
  for (DFOverlayPanel *strip in _strips) {
    if (strip.isVisible) {
      [strip logAs:"edge window"];
    }
  }
}

- (void)hideStrips
{
  for (DFOverlayPanel *strip in _strips) {
    [strip orderOut:nil];
  }
}

- (void)showCatcher:(const fd::EdgeRect &)rect
{
  auto s = _state.lock();
  if (!s) {
    return;
  }
  if (_catcher == nil) {
    // above the edge windows, so the parked drag is over the catcher
    _catcher = [[DFOverlayPanel alloc] initWithView:[[DFDropView alloc] initWithState:s catcher:YES]
                                              level:catcherWindowLevel()];
  }
  s->catcherEntered = false;
  [_catcher placeAt:rect];
  [_catcher logAs:"catcher for the parked drag"];
}

- (void)hideCatcher
{
  [_catcher orderOut:nil];
}

// secondary windows

- (void)showLauncher:(const fd::EdgeRect &)rect
{
  auto s = _state.lock();
  if (!s) {
    return;
  }
  if (_launcher == nil) {
    // takes a press, not a drag: above everything, so nothing else gets the press
    _launcher = [[DFOverlayPanel alloc] initWithView:[[DFLauncherView alloc] initWithState:s]
                                               level:NSScreenSaverWindowLevel];
  }
  [_launcher placeAt:rect];
}

- (void)launcherOutOfTheWay
{
  // the drag started from this window, so it stays until the drag ends, but neither the
  // pointer nor a drop may land on it
  _launcher.ignoresMouseEvents = YES;
  _launcher.alphaValue = 0.0;
  const NSRect corner = NSMakeRect(NSMinX(_launcher.frame), NSMinY(_launcher.frame), 1, 1);
  [_launcher setFrame:corner display:NO];
}

- (void)hideLauncher
{
  [_launcher orderOut:nil];
}

- (void)showRefuser:(const fd::EdgeRect &)rect
{
  if (_refuser == nil) {
    _refuser = [[DFOverlayPanel alloc] initWithView:[[DFRefuseView alloc] init] level:catcherWindowLevel()];
  }
  [_refuser placeAt:rect];
}

- (void)hideRefuser
{
  [_refuser orderOut:nil];
}

- (void)teardown
{
  [self hideStrips];
  [_strips removeAllObjects];
  [_catcher orderOut:nil];
  _catcher = nil;
  [_launcher orderOut:nil];
  _launcher = nil;
  [_refuser orderOut:nil];
  _refuser = nil;
  [_promiseQueue cancelAllOperations];
}

// NSDraggingSource

- (NSDragOperation)draggingSession:(NSDraggingSession *)session
    sourceOperationMaskForDraggingContext:(NSDraggingContext)context
{
  return NSDragOperationCopy;
}

- (void)draggingSession:(NSDraggingSession *)session willBeginAtPoint:(NSPoint)screenPoint
{
  [self launcherOutOfTheWay];
}

- (void)draggingSession:(NSDraggingSession *)session
           endedAtPoint:(NSPoint)screenPoint
              operation:(NSDragOperation)operation
{
  auto s = _state.lock();
  [self hideLauncher];
  [self hideRefuser];
  if (s) {
    loopEnded(s, operation);
  }
}

- (BOOL)ignoreModifierKeysForDraggingSession:(NSDraggingSession *)session
{
  return YES;
}

// NSFilePromiseProviderDelegate

- (NSString *)filePromiseProvider:(NSFilePromiseProvider *)filePromiseProvider fileNameForType:(NSString *)fileType
{
  auto s = _state.lock();
  NSDictionary *info = filePromiseProvider.userInfo;
  const auto dragId = static_cast<uint32_t>([info[@"drag"] unsignedIntValue]);
  const auto index = static_cast<uint32_t>([info[@"index"] unsignedIntValue]);
  DestPtr session = s ? findDest(s, dragId) : nullptr;
  if (!session || index >= session->staging.entries().size()) {
    return @"Deskflow item";
  }
  NSString *name = toNSString(session->staging.entries()[index].localPath);
  return name.length > 0 ? name : @"Deskflow item";
}

- (NSOperationQueue *)operationQueueForFilePromiseProvider:(NSFilePromiseProvider *)filePromiseProvider
{
  return _promiseQueue;
}

- (void)filePromiseProvider:(NSFilePromiseProvider *)filePromiseProvider
          writePromiseToURL:(NSURL *)url
          completionHandler:(void (^)(NSError *))completionHandler
{
  auto s = _state.lock();
  if (!s) {
    completionHandler(promiseError(@"Deskflow stopped before the files arrived."));
    return;
  }
  NSDictionary *info = filePromiseProvider.userInfo;
  const auto dragId = static_cast<uint32_t>([info[@"drag"] unsignedIntValue]);
  const auto index = static_cast<uint32_t>([info[@"index"] unsignedIntValue]);
  writePromise(s, dragId, index, url, completionHandler);
}

@end

namespace {

DFFileDragController *controllerOf(StatePtr s)
{
  if (s->controller == nil && !s->stopping) {
    s->controller = [[DFFileDragController alloc] initWithState:s];
  }
  return s->controller;
}

void postLoopEnded(StatePtr s, uint32_t dragId, bool dropped)
{
  s->postToCore([s, dragId, dropped] {
    if (s->hooks.loopEnded) {
      s->hooks.loopEnded(dragId, dropped);
    }
    fd::Message message;
    message.dragId = dragId;
    s->postEvent(new fd::FileDragEvent(fd::FileDragEvent::Kind::LoopEnded, std::move(message), dropped));
  });
}

//! Sending \p dragId stopped here; its cancel has already been posted
void postSourceStopped(StatePtr s, uint32_t dragId)
{
  s->postToCore([s, dragId] {
    if (s->hooks.sourceStopped) {
      s->hooks.sourceStopped(dragId);
    }
  });
}

//
// source (files leave this computer), main thread
//

void offerReady(StatePtr s)
{
  if (!s->offered) {
    s->offered = true;
    LOG_INFO(
        "file drag offer ready (%zu items, %.0f ms after they reached the edge window; the list took %.0f ms)",
        s->offer ? s->offer->items.size() : size_t{0}, (CFAbsoluteTimeGetCurrent() - s->edgeReachedAt) * 1000.0,
        s->offer ? s->offer->scanMs : 0.0
    );
    s->postEvent(new fd::FileDragEvent(fd::FileDragEvent::Kind::OfferReady));
  }
}

void offerLost(StatePtr s)
{
  if (s->offered) {
    s->offered = false;
    s->postEvent(new fd::FileDragEvent(fd::FileDragEvent::Kind::OfferLost));
  }
}

//! Builds the item list of a drag of \p paths (on a worker thread) unless it is this drag's already
void prepareOffer(StatePtr s, std::vector<fs::path> paths, NSInteger changeCount)
{
  if (s->offer && s->offer->changeCount == changeCount && s->offer->paths == paths) {
    if (s->offer->ready && s->overStrip) {
      offerReady(s);
    }
    return; // the same drag; still scanning
  }

  auto offer = std::make_shared<Offer>();
  offer->paths = std::move(paths);
  offer->changeCount = changeCount;
  s->offer = offer;
  const uint64_t generation = ++s->scanGeneration;
  const CFAbsoluteTime started = CFAbsoluteTimeGetCurrent();
  WeakState weak = s;
  dispatch_async(dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0), ^{
    auto scanned = std::make_shared<Offer>();
    std::string error;
    const bool ok = fd::scanItems(offer->paths, scanned->items, scanned->sources, &error);
    dispatchMain(^{
      auto state = weak.lock();
      if (!state || generation != state->scanGeneration || state->offer != offer) {
        return;
      }
      if (!ok) {
        LOG_WARN("cannot carry these files to another computer: %s", error.c_str());
        state->offer.reset();
        return;
      }
      offer->items = std::move(scanned->items);
      offer->sources = std::move(scanned->sources);
      offer->icon = offer->paths.empty() ? fd::Icon{} : iconForPath(offer->paths.front());
      offer->scanMs = (CFAbsoluteTimeGetCurrent() - started) * 1000.0;
      offer->ready = true;
      if (state->overStrip) {
        offerReady(state);
      }
    });
  });
}

//! A drag of files reached an edge window: offer them as soon as their item list is built
void onFilesEntered(StatePtr s, std::vector<fs::path> paths, id<NSDraggingInfo> sender, NSView *view)
{
  s->overStrip = true;
  s->edgeReachedAt = CFAbsoluteTimeGetCurrent();
  prepareOffer(s, std::move(paths), sender.draggingPasteboard.changeCount);

  // what the drag looks like, once per drag: a computer with kCapPreview shows the same under
  // its cursor. The hand-off comes after this, on this thread
  if (s->previewSequence != sender.draggingSequenceNumber) {
    const auto started = std::chrono::steady_clock::now();
    NSUInteger items = 0;
    s->preview = captureDragImage(sender, view, items);
    s->previewSequence = sender.draggingSequenceNumber;
    const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started).count();
    if (s->preview.isEmpty()) {
      LOG_INFO("file drag: the drag shows no image to copy, the file's icon stands for it (%.1f ms)", ms);
    } else {
      LOG_INFO(
          "file drag: drag image captured: %ux%u px, %u points wide, from %lu items in %.1f ms", s->preview.width,
          s->preview.height, fd::iconPointWidth(s->preview), static_cast<unsigned long>(items), ms
      );
    }
  }
}

/**
 * The edge windows just went up for a held button: if a drag of files is running, build its
 * item list now, while the cursor is still on its way to the edge. The offer is then ready
 * when the drag gets there and the cursor does not wait at the edge. The drag pasteboard
 * keeps what the last drag left on it, so each of its contents is read ahead once only; the
 * files are offered only when a drag really reaches an edge window with them.
 */
void prepareOfferAhead(StatePtr s)
{
  NSPasteboard *pasteboard = [NSPasteboard pasteboardWithName:NSPasteboardNameDrag];
  const NSInteger changeCount = pasteboard.changeCount;
  if (changeCount == s->prebuiltChangeCount) {
    return;
  }
  s->prebuiltChangeCount = changeCount;
  auto paths = filePathsOnPasteboard(pasteboard);
  if (!paths.empty()) {
    LOG_DEBUG("file drag: reading %zu dragged items ahead of the edge", paths.size());
    prepareOffer(s, std::move(paths), changeCount);
  }
}

//! What a watch that ended saw, so a drag that never reached the edge windows shows in the log
void logWatchEnd(StatePtr s)
{
  if (!s->watching) {
    return;
  }
  s->watching = false;
  LOG_INFO(
      "file drag: edge windows go (this watch: %u drags entered them, %u moves over them; this app is %s)",
      s->watchEntered, s->watchUpdated,
      NSWorkspace.sharedWorkspace.frontmostApplication.processIdentifier == getpid() ? "frontmost" : "in the background"
  );
}

void watchEdgesMain(StatePtr s, uint32_t sides, const fd::EdgeRect &desktop)
{
  DFFileDragController *c = controllerOf(s);
  const uint64_t generation = ++s->stripGeneration;
  if (sides == 0) {
    logWatchEnd(s);
    if (s->stripsParked) {
      return; // they stay under the parked drag, see handOffMain()
    }
    if (s->overStrip && s->offered) {
      // the files are over an edge window and a hand-off usually follows at once: keep the
      // window under them until it does (the hand-off puts the catcher there first)
      dispatchMainAfter(0.5, ^{
        if (generation == s->stripGeneration) {
          [s->controller hideStrips];
        }
      });
      return;
    }
    [c hideStrips];
    return;
  }

  const auto displays = activeDisplays();
  std::vector<fd::EdgeRect> strips;
  for (const Direction side : {Direction::Left, Direction::Right, Direction::Top, Direction::Bottom}) {
    const auto part = fd::edgeStripsOnDisplays(side, sides, desktop, displays);
    strips.insert(strips.end(), part.begin(), part.end());
  }
  [c placeStrips:strips];
  s->stripsParked = false;

  if (!s->watching) {
    s->watching = true;
    s->watchEntered = 0;
    s->watchUpdated = 0;
    prepareOfferAhead(s);
  }
  const CGPoint cursor = currentCursor();
  LOG_INFO(
      "file drag: %zu edge windows for sides 0x%x (main thread %d, app %s, running %d, active %d, hidden %d; drag "
      "image level %d; cursor %.0f,%.0f, left button %s)",
      strips.size(), sides, pthread_main_np(), activationPolicyName(NSApp.activationPolicy), NSApp.isRunning,
      NSApp.isActive, NSApp.isHidden, static_cast<int>(CGWindowLevelForKey(kCGDraggingWindowLevelKey)), cursor.x,
      cursor.y, CGEventSourceButtonState(kCGEventSourceStateCombinedSessionState, kCGMouseButtonLeft) ? "down" : "up"
  );
  [c logStrips];
}

void checkParkedButton(StatePtr s, uint64_t generation)
{
  if (!s->parked || generation != s->parkGeneration) {
    return;
  }
  // the combined session state only sees the release once it reached the system, i.e. once
  // it was let through to end the parked drag over the catcher
  if (!CGEventSourceButtonState(kCGEventSourceStateCombinedSessionState, kCGMouseButtonLeft)) {
    // let the drag finish over the catcher before it goes
    dispatchMainAfter(1.0, ^{
      if (generation == s->parkGeneration) {
        unpark(s, "button is up");
      }
    });
    return;
  }
  dispatchMainAfter(0.5, ^{
    checkParkedButton(s, generation);
  });
}

void unpark(StatePtr s, const char *why)
{
  if (s->parked) {
    LOG_DEBUG("file drag: local drag released from the catcher: %s", why);
  }
  s->parked = false;
  s->parkedOver = false;
  ++s->parkGeneration;
  [s->controller hideCatcher];
  if (s->stripsParked) {
    s->stripsParked = false;
    [s->controller hideStrips];
  }
}

/**
 * Moves the cursor, and the local drag with it, to where the drag is parked; the drag then
 * looks at what is under it (the catcher). \p attempt varies how: the first try does not
 * always reach the drag.
 */
void moveLocalDrag(StatePtr s, int attempt)
{
  // a drag only looks for the window under it when it moves: one point aside every other time
  const CGPoint to = CGPointMake(s->parkAtX + (attempt % 2), s->parkAtY);
  const CGPoint from = currentCursor();
  if (std::abs(from.x - to.x) > 2 || std::abs(from.y - to.y) > 2) {
    if (s->primary) {
      // before the move, so no motion with the jump in it is missed
      std::scoped_lock lock(s->parkJumpMutex);
      s->parkJumpX = static_cast<int32_t>(std::lround(to.x - from.x));
      s->parkJumpY = static_cast<int32_t>(std::lround(to.y - from.y));
      s->parkJumped = s->parkJumpX != 0 || s->parkJumpY != 0;
    }
    // the warp makes no event of its own, and no motion reaches the drag while the cursor is
    // on the other computer
    CGWarpMouseCursorPosition(to);
  }
  CGEventRef event = CGEventCreateMouseEvent(nullptr, kCGEventLeftMouseDragged, to, kCGMouseButtonLeft);
  if (event != nullptr) {
    // marked as this app's own, so the core's event tap lets it pass; posted after that tap
    // or, every other time, through it
    CGEventSetIntegerValueField(event, kCGEventSourceUserData, OSXFileDrag::kEventTag);
    CGEventPost(attempt % 2 == 0 ? kCGSessionEventTap : kCGHIDEventTap, event);
    CFRelease(event);
  }
}

void checkParked(StatePtr s, uint64_t generation, int attempt)
{
  if (!s->parked || !s->away || generation != s->parkGeneration || s->catcherEntered) {
    return;
  }
  if (attempt >= kParkTries) {
    LOG_WARN(
        "file drag %u: the local drag does not reach the catcher; it %s", s->parkedDragId,
        s->parkedOver ? "stays over the edge window, which refuses it too"
                      : "ends with Escape when the button is let go"
    );
    return;
  }
  LOG_INFO(
      "file drag %u: the local drag is not over the catcher yet, moving it there again (%d)", s->parkedDragId,
      attempt + 1
  );
  moveLocalDrag(s, attempt + 1);
  dispatchMainAfter(kParkRetrySeconds, ^{
    checkParked(s, generation, attempt + 1);
  });
}

void stopSourceSession(StatePtr s, uint32_t dragId)
{
  uint32_t expected = dragId;
  s->currentDragId.compare_exchange_strong(expected, 0);
  std::unique_ptr<SourceSession> session;
  {
    std::scoped_lock lock(s->sessionsMutex);
    auto it = s->sessions.find(dragId);
    if (it == s->sessions.end()) {
      return;
    }
    session = std::move(it->second);
    s->sessions.erase(it);
  }
  // joins the sender thread outside the lock
  session.reset();
}

void pruneSourceSessions(StatePtr s)
{
  std::vector<std::unique_ptr<SourceSession>> done;
  std::scoped_lock lock(s->sessionsMutex);
  for (auto it = s->sessions.begin(); it != s->sessions.end();) {
    if (it->second->isDone()) {
      done.push_back(std::move(it->second));
      it = s->sessions.erase(it);
    } else {
      ++it;
    }
  }
}

void handOffMain(StatePtr s, uint32_t dragId, int32_t parkX, int32_t parkY)
{
  DFFileDragController *c = controllerOf(s);
  pruneSourceSessions(s);

  // park the local drag over the catcher first: letting go there drops nothing. Not where
  // the cursor crossed: a drag held at the edge of a display is where the Dock, the hot
  // corners and Mission Control react to it (the desktop changed to another Space), and a
  // drag that does not move keeps the edge window as its target and never sees the catcher
  const auto displays = activeDisplays();
  const auto display = fd::displayAt(parkX, parkY, displays);
  const fd::EdgeRect clear = fd::squareAround(parkX, parkY, 2 * kParkInset, display);
  const int32_t x = clear.x + clear.w / 2;
  const int32_t y = clear.y + clear.h / 2;
  [c showCatcher:fd::squareAround(x, y, kCatcherSide, display)];
  // the edge window stays under the drag for as long as it is parked: a drag that does not
  // get to the catcher is still over a window that refuses it when the button is let go.
  // Without it the app at the edge of the screen got the files then
  s->parked = true;
  s->parkedOver = s->overStrip;
  s->stripsParked = true;
  s->parkedDragId = dragId;
  s->parkAtX = x;
  s->parkAtY = y;
  const uint64_t generation = ++s->parkGeneration;
  ++s->stripGeneration;
  logWatchEnd(s);
  if (s->away) {
    // not when the cursor already came back: it stays where the user has it
    LOG_INFO("file drag %u: the local drag waits at %d,%d (the cursor crossed at %d,%d)", dragId, x, y, parkX, parkY);
    moveLocalDrag(s, 0);
    dispatchMainAfter(kParkRetrySeconds, ^{
      checkParked(s, generation, 0);
    });
  }
  dispatchMainAfter(0.5, ^{
    checkParkedButton(s, generation);
  });
  s->offered = false;
  s->overStrip = false;

  if (s->currentDragId != dragId) {
    LOG_INFO("file drag %u: taken back before it was sent", dragId);
    return;
  }
  if (!s->offer || !s->offer->ready) {
    LOG_WARN("file drag %u: nothing to send", dragId);
    s->sendPeer(fd::Message::cancel(dragId, fd::CancelReason::Unsupported));
    postSourceStopped(s, dragId);
    return;
  }

  const Offer &offer = *s->offer;
  const bool lazy = s->peerLazy;
  if (lazy) {
    LOG_INFO("file drag %u: showing %zu items there; nothing is sent before the drop", dragId, offer.items.size());
  } else {
    LOG_INFO("file drag %u: sending %zu items", dragId, offer.items.size());
  }
  // a picture of the whole drag for a computer that shows one, else the first file's icon
  const bool preview = s->peerPreview && !s->preview.isEmpty();
  if (preview || !offer.icon.isEmpty()) {
    fd::Message icon;
    icon.type = fd::Message::Type::Icon;
    icon.dragId = dragId;
    icon.icon = preview ? s->preview : offer.icon;
    s->sendPeer(icon);
  }
  fd::Message begin;
  begin.type = fd::Message::Type::Begin;
  begin.dragId = dragId;
  begin.items = offer.items;
  s->sendPeer(begin);

  WeakState weak = s;
  auto session = std::make_unique<SourceSession>(dragId, offer, [weak](fd::Message &&message) {
    if (auto state = weak.lock()) {
      const bool stopped = message.type == fd::Message::Type::Cancel;
      const uint32_t id = message.dragId;
      state->sendPeer(message);
      if (stopped) {
        postSourceStopped(state, id);
      }
    }
  });
  SourceSession *raw = session.get();
  {
    std::scoped_lock lock(s->sessionsMutex);
    s->sessions[dragId] = std::move(session);
  }
  if (!lazy) {
    // a destination that cannot wait for the files gets them from now on
    raw->start();
  }
  // the next drag scans afresh (the files may change in between)
  s->offer.reset();
}

//
// destination (files come to this computer)
//

DestPtr findDest(StatePtr s, uint32_t id)
{
  std::scoped_lock lock(s->destMutex);
  auto it = s->dests.find(id);
  return it == s->dests.end() ? nullptr : it->second;
}

void eraseDest(StatePtr s, uint32_t id)
{
  std::scoped_lock lock(s->destMutex);
  s->dests.erase(id);
}

void retryRemoval(StatePtr s)
{
  for (auto it = s->pendingRemoval.begin(); it != s->pendingRemoval.end();) {
    if ((*it)->staging.removeStaged()) {
      it = s->pendingRemoval.erase(it);
    } else {
      ++it;
    }
  }
  if (!s->pendingRemoval.empty() && ++s->removalTries <= 30) {
    dispatchMainAfter(2.0, ^{
      retryRemoval(s);
    });
  }
}

//! Main thread: every promised item is in place (or nothing was promised)
void finishSession(StatePtr s, DestPtr session)
{
  if (session->finished.exchange(true)) {
    return;
  }
  session->phase = DestPhase::Done;
  if (!session->told.exchange(true)) {
    s->sendPeer(fd::Message::finished(session->id));
  }
  LOG_INFO("file drag %u: the files are in place", session->id);
  // the items were moved out; this empties the staging folder
  if (!session->staging.removeStaged()) {
    s->pendingRemoval.push_back(session);
    s->removalTries = 0;
    dispatchMainAfter(2.0, ^{
      retryRemoval(s);
    });
  }
  eraseDest(s, session->id);
}

void onCancelledMain(StatePtr s, DestPtr session)
{
  switch (session->phase.load()) {
  case DestPhase::Waiting:
    if (s->launcherSession == session) {
      [s->controller hideLauncher];
      s->launcherSession.reset();
      ++s->launcherGeneration;
    }
    session->phase = DestPhase::Done;
    eraseDest(s, session->id);
    postLoopEnded(s, session->id, false);
    break;

  case DestPhase::Looping: {
    // there is no call that cancels an AppKit drag: end it over a window that refuses it,
    // so it drops nowhere (draggingSession:endedAtPoint: then finishes the cancel)
    const CGPoint at = currentCursor();
    const auto x = static_cast<int32_t>(at.x);
    const auto y = static_cast<int32_t>(at.y);
    [controllerOf(s) showRefuser:fd::squareAround(x, y, kLauncherSide, fd::displayAt(x, y, activeDisplays()))];
    s->postToCore([s, x, y] {
      if (s->hooks.releaseAt) {
        s->hooks.releaseAt(x, y);
      }
    });
    dispatchMainAfter(3.0, ^{
      if (s->loopSession == session) {
        LOG_ERR("file drag %u: the drag did not end after it was cancelled", session->id);
      }
    });
    break;
  }

  case DestPhase::Dropped:
    // a promise still waiting fails on its next wait
    session->phase = DestPhase::Done;
    eraseDest(s, session->id);
    break;

  case DestPhase::Done:
    break;
  }
}

void cancelSession(StatePtr s, DestPtr session, fd::CancelReason reason, bool tellServer)
{
  if (session->cancelled.exchange(true)) {
    return;
  }
  session->reason = reason;
  session->staging.cancel();
  LOG_INFO("file drag %u cancelled: %s", session->id, fd::cancelReasonName(reason));
  if (!session->told.exchange(true) && tellServer) {
    s->sendPeer(fd::Message::cancel(session->id, reason));
  }
  dispatchMain(^{
    onCancelledMain(s, session);
  });
}

void showLauncherMain(StatePtr s, DestPtr session)
{
  if (session->cancelled) {
    return;
  }
  DFFileDragController *c = controllerOf(s);
  if (s->launcherSession && s->launcherSession->phase == DestPhase::Waiting) {
    // an older drag never got its button press
    cancelSession(s, s->launcherSession, fd::CancelReason::Busy, true);
  }
  const CGPoint at = currentCursor();
  const auto x = static_cast<int32_t>(at.x);
  const auto y = static_cast<int32_t>(at.y);
  [c showLauncher:fd::squareAround(x, y, kLauncherSide, fd::displayAt(x, y, activeDisplays()))];
  s->launcherSession = session;
  const uint64_t generation = ++s->launcherGeneration;
  LOG_DEBUG("file drag %u: start window up at %d,%d", session->id, x, y);

  dispatchMainAfter(kLauncherPressDelaySeconds, ^{
    if (generation != s->launcherGeneration || session->cancelled) {
      return;
    }
    const uint32_t id = session->id;
    s->postToCore([s, id, x, y] {
      if (s->hooks.launcherReady) {
        s->hooks.launcherReady(id, x, y);
      }
    });
  });

  dispatchMainAfter(kLauncherTimeoutSeconds, ^{
    if (generation != s->launcherGeneration || s->launcherSession != session || session->phase != DestPhase::Waiting) {
      return;
    }
    LOG_WARN(
        "file drag %u: the drag never started under the cursor (the press did not reach the start window; "
        "check that Deskflow may control this computer in Privacy & Security > Accessibility)",
        session->id
    );
    [s->controller hideLauncher];
    s->launcherSession.reset();
    ++s->launcherGeneration;
    cancelSession(s, session, fd::CancelReason::Timeout, true);
  });
}

void startDrag(StatePtr s, DestPtr session, NSView *view, NSEvent *event)
{
  DFFileDragController *c = controllerOf(s);
  session->phase = DestPhase::Looping;
  s->loopSession = session;

  fd::Icon icon = session->icon;
  if (icon.isEmpty()) {
    std::scoped_lock lock(s->destMutex);
    if (s->pendingIconId == session->id) {
      icon = s->pendingIcon;
    }
  }
  const auto &entries = session->staging.entries();
  NSImage *image = imageFromIcon(icon);
  // a picture of the whole drag is shown once, as it is; an icon stands for the first item
  const bool preview = image != nil && fd::iconPointWidth(icon) > 0;

  const NSPoint at = [view convertPoint:event.locationInWindow fromView:nil];
  NSMutableArray<NSDraggingItem *> *items = [NSMutableArray array];
  int n = 0;
  for (const uint32_t top : session->tops) {
    const auto &entry = entries[top];
    NSFilePromiseProvider *provider = [[NSFilePromiseProvider alloc] initWithFileType:typeIdentifierFor(entry)
                                                                             delegate:c];
    provider.userInfo = @{@"drag" : @(session->id), @"index" : @(top)};
    NSDraggingItem *item = [[NSDraggingItem alloc] initWithPasteboardWriter:provider];
    NSImage *contents = (n == 0 && image != nil) ? image : (preview ? nil : placeholderImage(entry.isDirectory));
    const NSSize size = preview ? image.size : NSMakeSize(kDragImageSide, kDragImageSide);
    const CGFloat offset = preview ? 0 : std::min(n, 3) * 4.0;
    [item setDraggingFrame:NSMakeRect(
                               at.x - size.width / 2 + offset, at.y - size.height / 2 - offset, size.width, size.height
                           )
                  contents:contents];
    [items addObject:item];
    ++n;
  }
  session->promisesLeft = static_cast<int>(items.count);

  NSDraggingSession *drag = items.count > 0 ? [view beginDraggingSessionWithItems:items event:event source:c] : nil;
  if (drag == nil) {
    LOG_ERR("file drag %u: AppKit did not start the drag", session->id);
    s->loopSession.reset();
    [c hideLauncher];
    session->phase = DestPhase::Done;
    cancelSession(s, session, fd::CancelReason::Unsupported, true);
    eraseDest(s, session->id);
    postLoopEnded(s, session->id, false);
    return;
  }
  drag.animatesToStartingPositionsOnCancelOrFail = NO;
  drag.draggingFormation = preview ? NSDraggingFormationNone : NSDraggingFormationPile;
  if (preview) {
    drag.draggingLeaderIndex = 0;
    LOG_INFO(
        "file drag %u: the drag looks as on the other computer (%ux%u px shown %.0f x %.0f points around the cursor)",
        session->id, icon.width, icon.height, image.size.width, image.size.height
    );
  }
  LOG_INFO(
      "file drag %u: the drag runs (%lu items, %.0f ms after they were announced)", session->id,
      static_cast<unsigned long>(items.count), (CFAbsoluteTimeGetCurrent() - session->begunAt) * 1000.0
  );
}

void loopEnded(StatePtr s, NSDragOperation operation)
{
  DestPtr session = std::move(s->loopSession);
  s->loopSession.reset();
  if (!session) {
    return;
  }
  const bool dropped = operation != NSDragOperationNone && !session->cancelled;
  if (dropped) {
    LOG_INFO("file drag %u dropped", session->id);
    session->phase = DestPhase::Dropped;
    if (!session->droppedSent.exchange(true)) {
      s->sendPeer(fd::Message::dropped(session->id));
    }
    if (session->promisesLeft <= 0) {
      finishSession(s, session);
    } else {
      dispatchMainAfter(kNoPromiseTimeoutSeconds, ^{
        if (!session->promiseAsked && session->phase == DestPhase::Dropped && !session->cancelled) {
          LOG_WARN("file drag %u: the drop target never asked for the files", session->id);
          cancelSession(s, session, fd::CancelReason::DropFailed, true);
        }
      });
    }
  } else {
    auto reason = fd::CancelReason::DropFailed;
    if (session->cancelled) {
      reason = session->reason;
    } else {
      LOG_INFO("file drag %u: let go where nothing takes the files", session->id);
    }
    cancelSession(s, session, reason, true);
    session->phase = DestPhase::Done;
    eraseDest(s, session->id);
  }
  postLoopEnded(s, session->id, dropped);
}

//! Promise queue: Finder asked for top-level item \p index at \p url (it was dropped there)
void writePromise(StatePtr s, uint32_t dragId, uint32_t index, NSURL *url, void (^completion)(NSError *))
{
  DestPtr session = findDest(s, dragId);
  if (!session || session->cancelled) {
    completion(promiseError(@"The drag was cancelled before the files arrived."));
    return;
  }
  if (index >= session->staging.entries().size()) {
    completion(promiseError(@"Unknown dragged item."));
    return;
  }
  session->promiseAsked = true;
  const auto &entry = session->staging.entries()[index];
  LOG_INFO("file drag %u: the drop target asks for \"%s\"", dragId, entry.localPath.c_str());
  if (!session->droppedSent.exchange(true)) {
    // a source that keeps the files back until the drop (kCapLazy) sends them from now on
    s->sendPeer(fd::Message::dropped(session->id));
  }

  using WaitResult = fd::FileDragStaging::WaitResult;
  // the receiver names the place; never replace what is there (a free "name (2)" instead)
  NSURL *folderUrl = url.URLByDeletingLastPathComponent;
  const char *folder = folderUrl.fileSystemRepresentation;
  NSString *wanted = url.lastPathComponent;
  const std::string name = wanted.length > 0 ? toUtf8(wanted) : entry.localPath;

  // the data may still be on its way (all of it, from a source that waits for the drop): wait
  // for every byte of this item, and let Finder show how far a large one is
  FinderProgress finderProgress;
  auto result = session->staging.waitForItem(index, std::chrono::milliseconds(0));
  if (result == WaitResult::Timeout) {
    if (!s->stopping && folder != nullptr && shouldShowFinderProgress(session->staging)) {
      finderProgress.begin(s, session, index, fs::path(folder), name, entry.isDirectory);
    }
    const auto waitStep = finderProgress.progress != nil ? kProgressUpdateInterval : std::chrono::milliseconds(500);
    auto lastBytes = session->staging.receivedBytes();
    auto lastProgress = std::chrono::steady_clock::now();
    while ((result = session->staging.waitForItem(index, waitStep)) == WaitResult::Timeout) {
      if (s->stopping) {
        result = WaitResult::Failed;
        break;
      }
      finderProgress.update(session->staging.itemReceivedBytes(index));
      const auto bytes = session->staging.receivedBytes();
      const auto now = std::chrono::steady_clock::now();
      if (bytes != lastBytes) {
        lastBytes = bytes;
        lastProgress = now;
      } else if (now - lastProgress > kStallTimeout) {
        LOG_WARN("file drag %u: no data arrived for %lld s", dragId, static_cast<long long>(kStallTimeout.count()));
        cancelSession(s, session, fd::CancelReason::Timeout, true);
        result = WaitResult::Failed;
        break;
      }
    }
  }
  if (result != WaitResult::Eof || session->cancelled) {
    const auto reason = session->cancelled ? session->reason.load() : fd::CancelReason::DropFailed;
    finderProgress.end(session->cancelled ? fd::cancelReasonName(reason) : "failed", true);
    NSError *err = (session->cancelled && reason == fd::CancelReason::User)
                       ? [NSError errorWithDomain:NSCocoaErrorDomain code:NSUserCancelledError userInfo:nil]
                       : promiseError(@"The files did not arrive.");
    completion(err);
    cancelSession(s, session, reason, true);
    return;
  }

  fs::path placed;
  std::string error;
  bool ok = false;
  if (finderProgress.hasPlaceholder) {
    finderProgress.finishUnits();
    ok = fd::FileDragStaging::replacePlaceholder(
        session->staging.pathOf(index), finderProgress.placeholder, entry.isDirectory, placed, &error
    );
    if (ok) {
      finderProgress.hasPlaceholder = false;
      finderProgress.end("done", false);
    } else {
      finderProgress.end("failed", true);
    }
  } else if (folder != nullptr) {
    ok = fd::FileDragStaging::moveNoReplace(
        session->staging.pathOf(index), fs::path(folder), name, entry.isDirectory, placed, &error
    );
  }
  if (!ok) {
    LOG_WARN(
        "file drag %u: cannot put \"%s\" into %s: %s", dragId, entry.localPath.c_str(),
        folder != nullptr ? folder : "?", error.c_str()
    );
    completion(promiseError(@"The dragged item could not be put there."));
    cancelSession(s, session, fd::CancelReason::DropFailed, true);
    return;
  }
  LOG_INFO("file drag %u: placed %s", dragId, placed.c_str());
  completion(nil);

  dispatchMain(^{
    if (--session->promisesLeft <= 0 && session->phase == DestPhase::Dropped && !session->cancelled) {
      finishSession(s, session);
    }
  });
}

void beginSession(StatePtr s, const fd::Message &message)
{
  auto session = std::make_shared<DestSession>(fd::FileDragStaging::defaultRoot());
  session->id = message.dragId;
  session->begunAt = CFAbsoluteTimeGetCurrent();
  {
    std::scoped_lock lock(s->destMutex);
    if (s->pendingIconId == message.dragId) {
      session->icon = s->pendingIcon;
    }
  }

  auto reason = fd::CancelReason::Unknown;
  std::string error;
  if (!session->staging.begin(message.dragId, message.items, reason, &error)) {
    LOG_WARN("file drag %u refused: %s (%s)", message.dragId, fd::cancelReasonName(reason), error.c_str());
    s->sendPeer(fd::Message::cancel(message.dragId, reason));
    postLoopEnded(s, message.dragId, false);
    return;
  }
  session->tops = session->staging.topLevelIndices();
  {
    std::scoped_lock lock(s->destMutex);
    s->dests[message.dragId] = session;
  }
  LOG_INFO(
      "file drag %u: staging %zu items in %s", message.dragId, session->staging.entries().size(),
      session->staging.stagingDir().c_str()
  );
  dispatchMain(^{
    showLauncherMain(s, session);
  });
}

void cancelFromPeer(StatePtr s, const fd::Message &message)
{
  if (message.reason == fd::CancelReason::ConnectionLost) {
    // the server is gone: every drag from it stops, including ones still copying
    std::vector<DestPtr> all;
    {
      std::scoped_lock lock(s->destMutex);
      for (const auto &[id, session] : s->dests) {
        all.push_back(session);
      }
    }
    for (const auto &session : all) {
      cancelSession(s, session, fd::CancelReason::ConnectionLost, false);
    }
    return;
  }
  if (auto session = findDest(s, message.dragId)) {
    // flags 1: cancelled here (escape, right click, cursor left): tell the server
    cancelSession(s, session, message.reason, message.flags == 1);
  } else if (message.flags == 1) {
    s->sendPeer(fd::Message::cancel(message.dragId, message.reason));
  }
}

//! Writer thread: staging writes happen in the order the messages came, off the core thread
void writerLoop(State *raw)
{
  pthread_setname_np("deskflow file drag writer");
  const StatePtr s = raw->shared_from_this();
  fd::FileDragStaging::sweepStale(fd::FileDragStaging::defaultRoot());
  while (true) {
    fd::Message message;
    {
      std::unique_lock lock(s->writerMutex);
      s->writerCond.wait(lock, [&s] { return s->writerStop || !s->writerQueue.empty(); });
      if (s->writerStop) {
        return;
      }
      message = std::move(s->writerQueue.front());
      s->writerQueue.pop_front();
    }

    @autoreleasepool {
      switch (message.type) {
        using enum fd::Message::Type;
      case Begin:
        beginSession(s, message);
        break;

      case FileStart: {
        auto session = findDest(s, message.dragId);
        if (session && !session->cancelled && !session->staging.fileStart(message.index, message.size)) {
          LOG_WARN("file drag %u: cannot start a staged file", message.dragId);
          cancelSession(s, session, fd::CancelReason::WriteFailed, true);
        }
        break;
      }

      case Chunk: {
        auto session = findDest(s, message.dragId);
        if (!session || session->cancelled) {
          break;
        }
        if (session->staging.write(message.index, message.data)) {
          s->sendPeer(fd::Message::ack(message.dragId, message.seq));
        } else {
          LOG_WARN("file drag %u: cannot write a staged file", message.dragId);
          cancelSession(s, session, fd::CancelReason::WriteFailed, true);
        }
        break;
      }

      case FileEnd: {
        auto session = findDest(s, message.dragId);
        if (session && !session->cancelled && !session->staging.fileEnd(message.index)) {
          LOG_WARN("file drag %u: a staged file is incomplete", message.dragId);
          cancelSession(s, session, fd::CancelReason::WriteFailed, true);
        }
        break;
      }

      case Cancel:
        cancelFromPeer(s, message);
        break;

      default:
        break;
      }
    }
  }
}

} // namespace

//
// OSXFileDrag
//

OSXFileDrag::OSXFileDrag(IEventQueue *events, void *eventTarget, bool primary, Hooks hooks)
    : m_state(std::make_shared<State>())
{
  m_state->events = events;
  m_state->eventTarget = eventTarget;
  m_state->primary = primary;
  m_state->hooks = std::move(hooks);

  if (NSApp == nil) {
    LOG_WARN("file drag: no Cocoa application in this process, dragging files between computers is off");
    return;
  }
  m_state->available = true;

  events->addHandler(EventTypes::FileDrag, m_state.get(), [](const Event &event) {
    if (const auto *task = dynamic_cast<const CoreTask *>(event.getDataObject())) {
      task->run();
    }
  });

  StatePtr s = m_state;
  dispatchMain(^{
    // the core runs as a background-only app; its drag windows need an accessory app (still
    // no Dock icon or menu bar) to show and take the pointer
    if (NSApp.activationPolicy == NSApplicationActivationPolicyProhibited) {
      if ([NSApp setActivationPolicy:NSApplicationActivationPolicyAccessory]) {
        LOG_DEBUG("file drag: running as an accessory app so its drag windows work");
      } else {
        LOG_WARN("file drag: cannot run as an accessory app; drag windows may not work");
      }
    }
    controllerOf(s);
  });

  State *raw = m_state.get();
  m_state->writer = std::thread([raw] { writerLoop(raw); });
  LOG_INFO("file drag: ready (sends and receives files on this Mac as the %s)", primary ? "server" : "client");
}

OSXFileDrag::~OSXFileDrag()
{
  StatePtr s = m_state;
  s->parkedRelease = false;
  if (!s->available) {
    return;
  }
  s->events->removeHandler(EventTypes::FileDrag, s.get());

  {
    std::scoped_lock lock(s->writerMutex);
    s->writerStop = true;
    s->writerQueue.clear();
  }
  s->writerCond.notify_all();
  if (s->writer.joinable()) {
    s->writer.join();
  }
  s->stopping = true;

  std::map<uint32_t, std::unique_ptr<SourceSession>> sessions;
  {
    std::scoped_lock lock(s->sessionsMutex);
    sessions.swap(s->sessions);
  }
  sessions.clear(); // joins the sender threads

  std::map<uint32_t, DestPtr> dests;
  {
    std::scoped_lock lock(s->destMutex);
    dests.swap(s->dests);
  }
  for (auto &[id, session] : dests) {
    session->told = true;
    session->cancelled = true;
    session->staging.cancel(); // wakes waiting promises; deletes the staged files
  }

  dispatchMain(^{
    [s->controller teardown];
    s->controller = nil;
    s->launcherSession.reset();
    s->loopSession.reset();
    s->offer.reset();
    for (auto &session : s->pendingRemoval) {
      session->staging.removeStaged();
    }
    s->pendingRemoval.clear();
  });
}

uint32_t OSXFileDrag::capabilities() const
{
  if (!m_state->available) {
    return fd::kCapNone;
  }
  // both roles, and the client to server messages (kMsgDFileDragWatch, Offer, HandOff)
  return fd::kCapSource | fd::kCapDestination | fd::kCapReverse | fd::kCapLazy | fd::kCapPreview;
}

void OSXFileDrag::watchEdges(uint32_t sideMask, int32_t x, int32_t y, int32_t w, int32_t h)
{
  if (!m_state->available) {
    return;
  }
  StatePtr s = m_state;
  const fd::EdgeRect desktop{x, y, w, h};
  dispatchMain(^{
    watchEdgesMain(s, sideMask, desktop);
  });
}

void OSXFileDrag::handOff(uint32_t dragId, int32_t parkX, int32_t parkY)
{
  if (!m_state->available) {
    return;
  }
  // the cursor leaves right after this. On the server the user's release that ends the
  // drag happens over there and must still reach this computer once; on a client the
  // button was pressed by the server, and OSXComputer lets go of it when the drag is over
  m_state->parkedRelease = m_state->primary;
  m_state->away = true;
  m_state->currentDragId = dragId;
  StatePtr s = m_state;
  dispatchMain(^{
    handOffMain(s, dragId, parkX, parkY);
  });
}

void OSXFileDrag::takeBack()
{
  if (!m_state->available) {
    return;
  }
  m_state->parkedRelease = false;
  m_state->away = false;
  m_state->parkJumped = false;
  if (const uint32_t id = m_state->currentDragId.exchange(0); id != 0) {
    LOG_INFO("file drag %u: cursor came back, stop sending", id);
    stopSourceSession(m_state, id);
  }
  StatePtr s = m_state;
  dispatchMain(^{
    unpark(s, "taken back");
  });
}

void OSXFileDrag::entered()
{
  if (!m_state->available) {
    return;
  }
  m_state->parkedRelease = false;
  m_state->away = false;
  m_state->parkJumped = false;
  StatePtr s = m_state;
  dispatchMain(^{
    unpark(s, "cursor is back");
  });
}

bool OSXFileDrag::takeParkedRelease()
{
  return m_state->parkedRelease.exchange(false);
}

bool OSXFileDrag::parkedDragIsCaught() const
{
  return m_state->parkedOver;
}

void OSXFileDrag::cancelLocalDrag()
{
  // Escape ends a drag without a drop. Posted after the core's event tap: it is this
  // computer's, not a key for the computer the cursor is on
  for (const bool down : {true, false}) {
    if (CGEventRef key = CGEventCreateKeyboardEvent(nullptr, kEscapeKeyCode, down); key != nullptr) {
      CGEventSetIntegerValueField(key, kCGEventSourceUserData, kEventTag);
      CGEventPost(kCGSessionEventTap, key);
      CFRelease(key);
    }
  }
}

void OSXFileDrag::removeParkJump(int32_t &dx, int32_t &dy)
{
  if (!m_state->parkJumped) {
    return;
  }
  std::scoped_lock lock(m_state->parkJumpMutex);
  if (!m_state->parkJumped) {
    return;
  }
  // the motion with the jump in it goes about as far as the jump, the same way (back towards
  // this computer, which the user is leaving). Any other motion is the user's and stays
  const int32_t jumpX = m_state->parkJumpX;
  const int32_t jumpY = m_state->parkJumpY;
  const bool alongX = std::abs(jumpX) >= std::abs(jumpY);
  const int32_t jump = alongX ? jumpX : jumpY;
  const int32_t moved = alongX ? dx : dy;
  if ((moved < 0) != (jump < 0) || std::abs(moved) < std::abs(jump) / 2) {
    return;
  }
  m_state->parkJumped = false;
  LOG_INFO(
      "file drag: motion %+d,%+d has the parked drag's move %+d,%+d in it, which is not the user's", dx, dy, jumpX,
      jumpY
  );
  dx -= jumpX;
  dy -= jumpY;
}

void OSXFileDrag::receive(const fd::Message &message)
{
  StatePtr s = m_state;
  if (!s->available) {
    return;
  }

  // drags sent from here (both computers send now: drag ids come from the server's one
  // counter, so a drag sent from here and one coming here never share an id)
  switch (message.type) {
    using enum fd::Message::Type;
  case Ack: {
    std::scoped_lock lock(s->sessionsMutex);
    if (auto it = s->sessions.find(message.dragId); it != s->sessions.end()) {
      it->second->onAck(message.seq);
    }
    return;
  }
  case Caps:
    // not from the wire: the flags of the computer the next drag goes to (see handOff())
    s->peerLazy = fd::lazyTransferAvailable(capabilities(), message.flags);
    s->peerPreview = (message.flags & fd::kCapPreview) != 0;
    return;
  case Dropped: {
    // a destination that waits for the files (kCapLazy) asks for them with this
    std::scoped_lock lock(s->sessionsMutex);
    if (auto it = s->sessions.find(message.dragId); it != s->sessions.end() && it->second->start()) {
      LOG_INFO(
          "file drag %u: dropped on the other computer, sending %zu items now", message.dragId, it->second->itemCount()
      );
    }
    return;
  }
  case Finished:
    stopSourceSession(s, message.dragId);
    return;
  case Cancel:
    if (message.dragId == 0) {
      // the connection is gone: every drag sent from here stops too
      std::map<uint32_t, std::unique_ptr<SourceSession>> sessions;
      {
        std::scoped_lock lock(s->sessionsMutex);
        sessions.swap(s->sessions);
      }
      s->currentDragId = 0;
      sessions.clear(); // joins the sender threads
    } else {
      bool sending = false;
      {
        std::scoped_lock lock(s->sessionsMutex);
        sending = s->sessions.contains(message.dragId);
      }
      if (sending || s->currentDragId == message.dragId) {
        LOG_INFO("file drag %u cancelled: %s", message.dragId, fd::cancelReasonName(message.reason));
        stopSourceSession(s, message.dragId);
        return;
      }
    }
    break; // also for drags coming here
  default:
    break;
  }

  switch (message.type) {
    using enum fd::Message::Type;
  case Icon: {
    // arrives just before its Begin
    std::scoped_lock lock(s->destMutex);
    s->pendingIcon = message.icon;
    s->pendingIconId = message.dragId;
    return;
  }

  case Cancel:
    if (message.reason == fd::CancelReason::ConnectionLost) {
      // nothing still queued for the lost server matters now
      std::scoped_lock lock(s->writerMutex);
      s->writerQueue.clear();
    }
    break;

  case Begin:
  case FileStart:
  case Chunk:
  case FileEnd:
    break;

  default:
    return;
  }

  {
    std::scoped_lock lock(s->writerMutex);
    s->writerQueue.push_back(message);
  }
  s->writerCond.notify_one();
}
