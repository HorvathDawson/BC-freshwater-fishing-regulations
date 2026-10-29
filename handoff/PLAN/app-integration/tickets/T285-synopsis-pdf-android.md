# T285 — Opening the synopsis PDF fails in the Android app ("couldn't load synopsis")

Status: DONE 2026-10-29 (owner 2026-10-29, on the staging APK). Shots: `handoff/PLAN/app-integration/T285-shots/`.

## Cause (reproduced on the testbed emulator, release r-fa1915a56adb)
- The installed release's synopsis URL (`<release>/r-xxx/synopsis-<id>.pdf`) answers **404**: staging keeps only the latest
  release's files, and the PDF (named by content) rides on under the new release. A device on the older release has nothing
  to download -> "Couldn't load the synopsis." Same on any release that was since replaced.
- Not T256 (that moved pdf-lib on web only). Also found: About's "Open the official Synopsis" handed the https PDF to
  Chrome (first-run screen, no viewer); and `fetch` held the 25 MB body on the Java heap (T262's OOM) on small-heap phones.

## Fix
- `synopsisPdf.native.ts`: download through expo-file-system `DownloadTask` straight to the document directory (kept for
  offline; size-checked, 3 tries, waits out offline); on 404 the source's `alternate()` (App: the live manifest's synopsis
  with the same sha256) supplies the live URL; a failed highlight opens the plain copy; no PDF app -> `NoPdfViewer`.
- About link on phones goes through `openBook(1)` (our copy, held file, system viewer); web keeps its link unchanged.
- Failed card: "No app on this device opens PDFs..." for no viewer; **Close** (also Android back, tap outside) closes it
  without opening gov.bc.ca (owner add-on); Retry and "Open on gov.bc.ca" stay.
- Tests: `synopsisPdf.native.test.ts` (native download, content:// VIEW + read grant, held = offline, 404 alternate, no
  viewer, highlight failure), `NetBanner.test.tsx` (Close + no-viewer words).
- Emulator (testbed APK): About and regs "Rule book p.9" open the PDF in the system viewer at the cited page, online and in
  airplane mode (held copy); no viewer -> card + Close.

## Work
Reproduce on the testbed emulator (staging data). Find why (native path using a web-only blob/pdf-lib route, file URI
permissions, missing viewer intent, download/cache failure, the T256 pdf-lib split). Make "Open the official Synopsis" and
the regs page's rule-book links open the PDF at the right page on Android (system viewer via a content:// URI or an in-app
viewer), offline too when the release's synopsis is on the device. Clear message if no viewer exists.

## Acceptance
Emulator: link opens the PDF at the cited page, online and in airplane mode; web unchanged; test pins the native path.

Depends: none. Size: S. Owner: no.
