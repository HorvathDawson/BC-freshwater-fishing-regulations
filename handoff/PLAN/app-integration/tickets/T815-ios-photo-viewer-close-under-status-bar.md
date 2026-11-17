# T815 — iOS: the spots photo viewer's close ✕ sits under the status bar

Status: OPEN (app, iOS; app chat)

Source: owner 2026-11-16 ~21:22, testing iPhone TestFlight build 36 (app chat; pictures in the chat). Verbatim: "the picture viewer for spots has the x under the notification bar".

## Look at
The photo viewer (`photo-zoom` scenario on Android) does not inset its close control by the top safe area on iOS (the
notch / Dynamic Island). Fix with the safe-area inset the rest of the app uses; check the web and Android unchanged.

## 2026-11-16 21:45 — cause and fix (branch `app-fix/ios-1116`, app chat lane)
The viewer is a full-screen `Modal` (its own window) whose content had no inset of its own: a 12 px padding from the
window's top edge put the ✕ under the clock / Dynamic Island. The app's other full-window modals (menu pages, T50/T313)
pad by `WindowInsets` (`WindowInset.native.tsx`: on iOS the root's safe-area insets, measured once, not mid-slide; on
Android the native SafeAreaView; on the web a plain View). `spot/PhotoViewer.tsx` now wraps its header and photo row in
`WindowInsets` inside the dark root, so the ink still fills under the bars. Test: `spot/PhotoViewer.insets.test.tsx`
(the iOS half with an iPhone 17 Pro's insets: the ✕ and the photo row inside a 62 / 34 px padded box; fails on the old
viewer). Web: unchanged (the web half adds no padding; the zoom tests pass as they were). Android: the old
`out/t454/01-fit.png` (2026-11) shows the same ✕ drawn over the status bar there, so Android moves below the bar too.
