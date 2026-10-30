# T298 — First download: keeps going in the background (notification), asks before using mobile data, fish sayings while it runs

Status: OPEN — next APK (owner 2026-10-29).

Source: owner: "for the initial download of the offline regs can we make it so it can work in background if user closes
app? also it should ask user for permission to download off wifi if they are not connected. finally we want to add cute
fish sayings that cycle as the download is happening. for downloading in the background it could become a notification
bar download?"

## Work
1. **Background download (Android):** the first install (release files, basemap, tiles, map assets, synopsis PDF) keeps
   downloading when the app is closed or the screen is off, with a progress notification in the notification bar
   ("Downloading BC fishing rules · 45%"); tapping it opens the app. Use the platform's own mechanism (Android
   DownloadManager or a foreground service / WorkManager via an Expo module or config plugin) — resumable, survives app
   close; on the next open the app picks up finished files, verifies sha256 (T262/T272 path) and finishes setup. iOS: a
   background URLSession download where expo-file-system supports it; note what's possible. Notification permission asked
   only when needed (Android 13+), with a plain reason.
2. **Mobile data:** before a large download (first install, a big update), if the device is on cellular (or metered), ask:
   "Download about N MB using mobile data?" — **Download now** / **Wait for Wi-Fi** (resumes automatically on Wi-Fi). Small
   updates (< ~5 MB) don't ask. Remember the choice for this download only.
3. **Fish sayings:** short, cute fishing lines that cycle every few seconds under the progress bars during the download
   (in the spirit of the search placeholders; never promise fish; e.g. "Untangling the leader…", "Checking the tackle box…",
   "Waiting for the bite… of data"). Words in core tables; cycle respects reduced-motion (no animation, just swap).

## Acceptance
Emulator: start the first install, close the app → notification progresses → reopen → setup finishes; airplane/cellular
emulation shows the prompt and "Wait for Wi-Fi" resumes on Wi-Fi; sayings cycle; screenshots.

Depends: T262, T272. Size: M–L. Owner: no.
