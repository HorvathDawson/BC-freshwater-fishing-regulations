# T912 — A regulations data update stops downloading when the app is closed

Status: IN PROGRESS (lane app/t912-bg-download: fixed + emulator walk; left (Claude): integrate (0.8.3 batch)).

Owner 2026-11-19 ~10:50 (main chat), verbatim: "also iff i close app it doesnt continue to downlaod the regulation data
updates." (He is on Android, Play 0.8.2 build 37; promoting it to closed testing for himself the same minute.)

## Work
T298 meant the system's downloader to carry the big files in the background (`background (T298): the system's
downloader carries the big files`, tests in packages/data). Reproduce on the emulator with the Play-like build (patched
MapLibre): start an update, swipe the app away / press Home, and watch whether the download job keeps going (adb,
the downloader's logs) and whether the next open picks up the finished files or starts over. Find where the job is
tied to the app process (JS-side fetch instead of the system downloader, a foreground-only task, a job cancelled on
destroy) and fix it so a closed app keeps downloading and the next open applies it. Tests + an emulator walk.

## Outcome (lane app/t912-bg-download, 2026-11-19)
**Cause.** T298 handed the system's downloader (Android DownloadManager) the FIRST install's big files only. An update
(`Booted.stage` → `install`) fetched its changed data files in the app first, one by one, and queued each big file
(tiles, mask, basemap, rule book) only when the install reached it. Closed in that window (the whole update, when it changes
only data files), nothing was queued: the process died and every download with it; the next open started the update again.
Reproduced on the emulator with the code before the fix (T903 lane's APK, release/update code identical to 264d78c5e):
`bg-update` — app killed the moment the update was downloading: 0 system jobs queued, none started in 60 s after the kill;
the reopen downloaded it again in the app.

**Fix.**
- `boot.ts` `stage`: before the install starts, every file the update downloads is queued with the system's downloader
  (`updateItems`: the changed data files first, then the big files); a failed queue is said + reported and the install
  goes on in the app.
- `bgDownload.ts` `fetch` (new): a queued data file is taken from its job — claimed beside its install path, unpacked if
  gzip, sha-verified — and the install places it. No job (a first install's data file, unchanged), a job that failed,
  vanished, stalled `stuckMs`, brought other bytes (also reported) or was CUT part way (paused, not for a network: the
  downloader must ask for unencoded bytes, which the data host sends decoded with no length/ETag, so it cannot resume —
  seen on the emulator at 93 %) → downloaded in the app, the log says why (W46).
- The update service passes its mobile-data rule to the stage (`{ metered }`: a big update without "Always" is queued
  Wi-Fi only), so a download that goes on after the app is closed does not switch to mobile data against the setting
  (before, the lazily queued big files always allowed mobile data).
- Notification line for a data file: "Regulations data · N MB" (`DOWNLOAD_NOTE_WORDS.files.data`).
- Unchanged: the first install (T298, bench-proven) keeps its data files in the app.

**Tests.** `bgDownload.test.ts` (fetch: claimed + verified gzip or plain, not queued → in app, failed / other bytes /
cut / stuck → in app with the words, network wait waited on), `service.test.ts` (the metered rule, mutation pin),
`tools/release-boot.test.ts` (an update over a fake system downloader: all 4 files queued Wi-Fi only before the app fetched
any — mutation pin fails without the queue — the app "closed", the system finishes, the next open claims all 4 with
nothing fetched in the app, the launch after applies). `test-updates.test.mjs` (data-book). `pnpm -C app check:fast` green.

**Emulator** (test bed `bg-update`, new; `test-updates.mjs data-book`: the live `climate` with its SQLite change counter
moved + the rule book with a trailing PDF comment, both under test/; `big`'s basemap-id move downloads nothing — the phone
keeps the file at that path). Fixed APK: both system jobs queued before the kill, "2 files downloading" in the shade with
the app dead, both started and finished with the app not running (line held to 250 kB/s). Final run (APK of 9defa2447,
clean, patched MapLibre) 7/7 PASS: killed 10:20:24 with 2 jobs queued → both finished by 10:21:18, app not running →
reopen claimed `climate-…sqlite.gz` and the rule book from the system (nothing downloaded again) and staged → applied at
the next pause (`release r-379e72fc9524-book4538` in use) → reset to the live release. An earlier fixed run found the data
file's job cut at 93 % and parked "waiting to retry" for good (no length from the host → no resume); fixed in 9defa2447
(the app fetches such a file at once, said). Run dirs: lane scratchpad `t912/before` (repro), `t912/after2` (pass).

**iOS (not done; iOS work waits).** The same JS runs there over the T722 background URLSession module, so an update's
files are now queued with it too: they go on with the app in the background or suspended; iOS cancels them when the app
is swiped away in the app switcher and the next open resumes them (resume data). Not built or walked on a device.

**Not done / open.** Jobs of an update that is abandoned before it is claimed (a newer release supersedes it) stay in the
downloader's folder until the next update of the same file (pre-existing for big files, T298). A cut data file is
fetched again from zero by the app (small).
