# T732 — The phone's backup carries saved spots first, photos second, never the downloaded release

Status: DONE (f7b41c208; landed 2026-11-17 in land/app-1117 (copies 1280 px, WebP 75)).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T732 | The phone's backup carries saved spots first, photos second, never the downloaded release (Android Auto Backup skips everything past 25 MB today) | DONE | f7b41c208 | audit G14, 2026-11-14; owner words relayed by the Android chat ("save spots with prioirty", "images can be compressed if still visible"); owner 2026-11-14 evening: "Only need to … (detail: ticket file) |

Source: audit finding G14 (`handoff/PLAN/ios/parity-audit.md`, pass 2, 2026-11-14): on Android, saved spots and photos
are never in the Google backup, because the installed release (~480 MB) puts the app over Auto Backup's 25 MB limit and
Android then skips the whole backup. Owner 2026-11-14, as relayed by the Android chat (the iOS chat did not hear it
first hand; to be confirmed with the owner on return), verbatim: "we should have it save spots with prioirty" and
"images can be compressed if still visible". The Android chat's reading: spots (place, name, notes, parking point, visit
log) always first; photos second and may be stored / backed up compressed while they still look fine; the release,
tiles and other re-downloadable data excluded from backup, as on iOS (T722, F6).

## Work
1. Prove the gap on the Android emulator: what the backup includes today and its size (`bmgr` / a backup dry run).
2. Android backup rules through the Expo config (full-backup-content and data-extraction-rules): exclude the release,
   tiles and caches; include the spots data.
3. Photos: Auto Backup skips EVERYTHING when the set passes 25 MB, so photos cannot simply be "included". Design so
   spots are never the part dropped and nothing is lost silently: say in the app or here what is and is not backed up.
4. Photo compression: propose size and quality, one before/after pair; existing photos must not be degraded visibly.
5. iOS: the same priority holds there by construction once T722 excludes the release (iCloud backup has no 25 MB cap);
   say so with evidence.
6. Check the privacy page and `handoff/PLAN/play/data-safety.md` still hold; the Android chat owns both files today:
   report sentences that need changing, do not edit them.

## Acceptance
A backup dry run on the emulator that carries the spots data and stays under 25 MB with the release installed; a restore
onto a wiped install brings the spots back; what happens to photos past the limit is stated and shown; the photo pair
looked at by the owner. Merge only after the owner's look, the Android bench, and a message to the Android chat.

Depends: T722 (iOS backup exclusion, sibling). Size: M. Owner: yes (look at the photo pair and the "what is backed up"
wording; confirm the relayed words). Default: build as a proposal on the branch; timing before closed testing only if it
lands clean and proven, otherwise first thing after (the Android chat tells the owner).

## Owner answers

2026-11-14 evening, relayed by the main iOS chat, verbatim:
1. "Only need to backup photos and spots the rest of the data isn't important"
2. "Settings could also be saved"

What was built to them (branch `android/t732-backup`, still a proposal, not merged): the rule sets stay as proven in
§8 (spots + `.bak` + the seven settings files), and the cloud set ADDS the photos' backup copies
(`spots/backup-photos/`); the device transfer keeps `spots/photos` at full size. Quota order: spots first, then the
settings' bytes, then photo copies newest first. **The main chat's reading, not the owner's words:** sentence 1 is the
yes to the new native dependency `expo-image-manipulator` (photos cannot ride the 25 MB cloud backup any other way, §3);
it is in `app/deps.md`. See §9.

## Results

2026-11-14, backup lane (branch `android/t732-backup`, from local `redesign/stream-sections` 8d7d01d9a: origin was not
reachable, no DNS). Code and design only: no emulator, no build. The emulator proof ran later the same day: §8.

### 1. Facts: where the data lives on Android, and the manifest today
expo-file-system `Paths.document` = `Context.getFilesDir()` (`expo-modules-core` `AppDirectoriesService.kt:18-19`,
`persistentFilesDirectory`); `Paths.cache` = `getCacheDir()`.

| Data | Where (Android) | Size | Source |
|---|---|---|---|
| Spots (all visits, notes, frozen panels) | `files/spots/spots.spots` (+ `.bak`, `.writing` while saving) | small: KB per visit; a few hundred visits = low MB (estimate) | `spots/store.native.ts:20,25`, `spots/core.ts:58` |
| Photos (as kept) | `files/spots/photos/<spot>-<n>.jpg` | **full camera pixels**, JPEG re-encoded at `quality: 0.8` (no resize): ~3–5 MB each for 12 MP (4.6 MB measured below) | `photoStore.native.ts:15`, `apps/mobile/src/pickPhoto.native.ts:21` |
| Settings the reader sets | `files/spots/{layers.json, notify.json, low-data.txt, crash-reports.txt, run-species.json, first-run.txt, walkthrough.txt}` | bytes to KB | `spots/store.native.ts` (`fileStore`) |
| Caches / state beside them | `files/spots/{like-visit, dfo-feed, gauge-history, inseason-feed, inseason-seen, inseason-inbox, updates, update-log, report-outbox, crash-exit-seen, crash-pending, app-update, recent-searches}.json/.txt`, `install-id.txt`, `location-ask.txt`; `files/feeds/runtiming.json`; `files/settings/{app-env,update-channel}.txt` | KB to a few MB | same file; `runTimingStore.native.ts:6`; `apps/mobile/src/envStore.native.ts:9` |
| The release | `files/release/<id>/…` + `release/tiles/…` + `release/basemap/…` + `release/current` | r-f749475d0962 manifest: data files 57.3 MB, atlas 197.2 MB, basemap 225.4 MB, mask 0.06 MB = **~480 MB**, plus `release/previous` during an update | `install.native.ts:24-25`, `install.ts:215,281-285`, `data/release/r-f749475d0962/manifest.json` |
| Rule book | `files/synopsis-<sha16>.pdf` | 11.7 MB | `install.ts:432`, `synopsisPdf.native.ts:18` |
| System downloads in flight | `getExternalFilesDir("bg-download")/…` + shared prefs `metered:<id>` | up to a whole big file | `modules/bg-download/…/BgDownloadModule.kt:62,71` |
| Map tile cache | MapLibre's ambient cache, `files/mbgl-offline.db` (proven on the emulator, §8: 312 KB there, measured by the BEFORE preflight) | up to its cap | – |
| Others | `databases/` (WorkManager, expo-background-task), `shared_prefs/` (expo-notifications, bg-download), `cache/` (picker copies, `synopsis-open.pdf`) | small | – |

**Auto Backup's default** (Android docs, "Back up user data with Auto Backup"): with no rules it takes `getFilesDir()`,
`getDatabasePath()` files, shared preferences, `getExternalFilesDir()` and `getDir()`; it leaves out `getCacheDir()`,
`getCodeCacheDir()` and `getNoBackupFilesDir()`. **The manifest today** (prebuild, `apps/mobile/android/app/src/main/
AndroidManifest.xml:17` in the owner's checkout, generated): `android:allowBackup="true"` (Expo's default,
`@expo/config-plugins` `AllowBackup.js:37`: `config.android?.allowBackup ?? true`; `app.json` sets nothing), and no
`fullBackupContent` or `dataExtractionRules`; no library manifest under `app/node_modules` names either. minSdk 24,
targetSdk 36 (`expo-modules-autolinking` defaults), so both rule files matter: `fullBackupContent` on Android 6–11,
`dataExtractionRules` on 12+.

**At the quota** (Android docs, same page): "If the amount of data is over 25 MB, the system calls `onQuotaExceeded()`
and doesn't back up data to the cloud." The full backup measures first (a preflight) and gives up whole: the spots go
with the release. Device-to-device transfer has no such limit. **Proven on the emulator (§8)**: today's state (quota hit)
and the fixed state (success, 17,920 bytes) are steps 2 and 4 there.

### 2. Built: the rules (`apps/mobile/plugins/with-backup-rules.js`, in `app.json`'s plugins)
A local config plugin: writes `res/xml/backup_rules.xml` and `res/xml/data_extraction_rules.xml` and points
`android:fullBackupContent` / `android:dataExtractionRules` at them (`app.json` has no key for either). INCLUDE lists, so
everything not named stays out by construction (Android: one `<include>` and nothing else is taken by default).
- **cloud-backup (and Android ≤11):** `spots/spots.spots`, `spots/spots.spots.bak`, and the settings: `layers.json`,
  `notify.json` (alert rules), `low-data.txt`, `crash-reports.txt` (a consent), `run-species.json`, `first-run.txt`,
  `walkthrough.txt`; and (§9, after the owner's answers) `spots/backup-photos` — the photos' compressed copies, never
  the originals.
- **device-transfer (Android 12+):** the spots and settings, plus `spots/photos` (every photo at its kept size: no
  quota); not the copies (the originals are there).
- **Out on purpose:** the release, the rule book, tile/feed/forecast caches; `updates.json` (its validators would tell
  the update service a release is current on a phone that has none: the reader re-sets "download on mobile data");
  bg-download jobs (prefs + external folder name another phone's DownloadManager ids); `install-id.txt` (a new phone is a
  new install); `location-ask.txt` (the new phone asks again); recent searches; unsent problem reports and crash
  records; `settings/` (developer environment and update channel: a new install takes its build's defaults).
- **Android ≤11 carries no photos even in a transfer**, decided: the `requireFlags="deviceToDeviceTransfer"` gate is
  Android 9+, and an ungated photo include on Android 7–8 could push the cloud set over quota and lose the spots.
- `disableIfNoEncryptionCapabilities` is left unset (backup still runs on a phone with no screen lock; Android
  end-to-end encrypts it when a lock exists). Owner question B.
- A manifest that already names other rules, or `allowBackup="false"`, fails the prebuild (one owner of the rules).
- `npx expo config --type introspect` shows both attributes on `<application>`. Test: `app/tools/backup-rules.test.ts`
  pins both XML files whole, the manifest attributes, both refusals, and the plugin's place in `app.json`.

### 3. Photos and the limit: options and the recommendation
- **(a) Photos out of cloud backup, and the app says so.** Zero machinery (this is the state the rules above give
  today). Cost: photos are only in a new-phone transfer, never in a cloud restore.
- **(b) A backup set (RECOMMENDED):** the app keeps compressed copies (1600 px long edge, JPEG 70, ~0.37 MB) of the
  newest photos in `files/spots/backup-photos/`, chosen to fit what the spots and settings leave of the quota less
  5 MB headroom; the rules add that one folder to cloud-backup. The Spots screen shows the line. Spots are counted
  first, so they are never the part dropped; what is left out is named in the line. **Built:** the choice and the line,
  `packages/core/src/backupSet.ts` (`planBackupSet`, `backupLine`, `BACKUP_QUOTA_BYTES`, `BACKUP_HEADROOM_BYTES`;
  newest visit first, ties by reference, first fit) and `backupSet.test.ts` (whole sentences). **Built since (§9);
  the design as first written:**
  (needs a new native dependency the owner should approve, `expo-image-manipulator` on the SDK 58 line, plus a
  `deps.md` line): `packages/data/src/spots/backupSet.native.ts` + `backupSet.web.ts` (web: nothing, no backup) +
  `backupSet.d.ts` — on save/delete of a spot and once per start: list every photo ref with its visit time, make any
  missing copy into `cache/backup-stage/` (`manipulateAsync` resize 1600, compress 0.7), size the spots/settings files,
  `planBackupSet`, then move the kept copies into `spots/backup-photos/` and delete the ones no longer kept (a copy
  never replaces an original); `apps/mobile/plugins/with-backup-rules.js` adds `spots/backup-photos` to cloud-backup only
  (the transfer already has the originals); the line on the Spots list (`packages/ui-native/src/spot/…`, words from
  `backupLine`), Android only (a `.native`/`.web` pair, rule 34a; iOS shows nothing or its own line). A restore brings
  copies, not originals: the restore path must map `backup-photos/<ref>` back to `photos/<ref>` when the original is
  missing (one function in `photoStore.native.ts`'s `uri`), else restored spots show no photo.
- **(c) A custom BackupAgent** (`onFullBackup`, `FullBackupDataOutput.getQuota()`, API 26+): could say "photos until
  25 MB" exactly, but it is Kotlin outside JS tests, a new module, `android:backupAgent` + `fullBackupOnly`, and it
  replaces the platform's own file walk. Not recommended.

### 4. Photo compression
Today: the picker's file is copied whole; `quality: 0.8` re-encodes at full camera size (a 12 MP phone gives
4032×3024); nothing resizes on a phone (`photoStore.native.ts:6-8`); the web already downsizes to `PHOTO_MAX_EDGE` 2048.
Measured with `sips` on a 12 MP stand-in (a 4:3 frame of the macOS "Tahoe Day" lake wallpaper at 4032×3024 with light
grain, JPEG 80 to match the picker; real phone photos vary ±50 %):

| Version | Pixels | JPEG q | Size | Per 20 MB |
|---|---|---|---|---|
| as kept today | 4032×3024 | 80 | 4.61 MB | 4 |
| proposed stored (new photos) | 2048×1536 | 80 | 0.78 MB | 25 |
| proposed backup copy | 1600×1200 | 70 | 0.37 MB | 54 |
| smaller backup copy (option) | 1280×960 | 70 | 0.25 MB | 81 |

Proposal: NEW photos stored at 2048 px / q80 (same `expo-image-manipulator` step in `photoStore.native.ts` `keep`);
backup copies at 1600 px / q70. **Existing photos are never re-encoded**: they stay as kept; only the backup set copies
them. Pair for the owner (scratch, not committed; regenerate with `sips -Z 1600 -s format jpeg -s formatOptions 70`):
before `before-12mp-q80.jpg` (4,608,757 B), after `after-backup-1600-q70.jpg` (373,736 B), in the backup lane's scratch
folder `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/035b8307-2c5e-4a11-bceb-d03b90dacf24/scratchpad/t732/`.

### 5. iOS
iCloud device backup takes the app container's `Documents/` and `Library/` (not `Library/Caches`, not `tmp/`), with no
per-app cap: the limit is the reader's iCloud storage, and the reader can switch the app off in Settings → iCloud →
Backups (Apple's iOS Data Storage Guidelines / `isExcludedFromBackup` docs; from memory). `Paths.document` is
`Documents/`. Branch `ios/t722-install` a190b3c4a marks `release/` and the rule book `isExcludedFromBackup`
(`boot.ts` `outOfBackup`, `BgDownloadModule.swift:83,287-290`), at every install and boot. After it, iCloud carries
`spots/` whole (spots file, all settings and caches there, every photo at full size), `settings/` and `feeds/`. No
all-or-nothing app quota, so the spots are carried by construction and **no backup set is needed on iOS**. The stored-
photo compression (2048/q80, new photos) is still worth it there: ~6× less of the reader's iCloud per photo.

### 6. Privacy page and data-safety (READ ONLY here; owned by the Android chat)
Sentences that stop being true once spots ride in the phone's own backup (and are already untrue on iOS today, where
Documents are in iCloud backup):
- `html.ts:138` "Your photos and the spots you save stay on your device, and the app never sends us your GPS position."
  → "Your photos and the spots you save stay on your device and in your phone's own backup if you have it on (we never
  see either), and the app never sends us your GPS position."
- `html.ts:145` "The pictures stay on your device and are not uploaded." → "The pictures stay on your device and are
  never sent to us. Your phone's own backup can keep a copy (on Android, smaller copies of the newest pictures)."
- `html.ts:146-147` "Spots, notes and photos are stored only on your device, and there is no account. If you delete the
  app or clear its data, they are gone; we cannot recover them." → "Spots, notes and photos are stored on your device,
  and in your phone's own backup (your Google account or iCloud) if you have it on; there is no account with us and we
  never see them. If you delete the app or clear its data, they are gone from the device, and we cannot recover them;
  restoring your phone's backup can bring them back."
- `html.ts:204` "Spots, photos, notes and settings live only on your device: delete them in the app, or uninstall the
  app, and they are gone." → "Spots, photos, notes and settings live on your device, and in your phone's own backup if
  you have it on: delete them in the app, or uninstall the app, and they are gone from the device; your phone's backup
  keeps its last copy until the backup service removes it."
- `data-safety.md` row 7 "nowhere: kept on the phone with the spot" → "kept on the phone with the spot; the phone's own
  backup (the user's Google account, Android Backup) holds the spots and, with the backup set, smaller photo copies; we
  never receive them". Play's Data safety answers (Photos: not collected) stay, IF Play still treats Android Backup as
  not developer collection (my reading of the Play help, **to confirm against the current text**).

### 7. Emulator proof (for the booked slot; one build with the plugin, one without)
1. `aapt2 dump xmltree app.apk --file AndroidManifest.xml | grep -i -e allowBackup -e fullBackupContent -e dataExtractionRules`
   and `aapt2 dump xmltree app.apk --file res/xml/data_extraction_rules.xml` → both attributes; the include list above.
2. BEFORE (today's APK, release installed, 3 spots, 2 photos): `adb shell bmgr enabled true`;
   `adb shell bmgr transport com.android.localtransport/.LocalTransport`;
   `adb shell settings put secure backup_local_transport_parameters "is_encrypted=true"`;
   `adb logcat -c; adb shell bmgr backupnow --monitor-verbose ca.canifishthis.app; adb logcat -d | grep -i -e quota -e PFTBT`
   → the package reported over quota (quota hit at preflight), nothing written.
3. `adb shell run-as ca.canifishthis.app du -sk files/release files/spots files/spots/photos files/synopsis-*.pdf` → the
   sizes behind 2.
4. AFTER (plugin APK, same data): the same `bmgr backupnow` → success; `adb shell dumpsys backup | grep -A3 canifishthis`
   shows the backup time; size of the set: `adb root; adb shell du -sk /data/cache/backup/*/_full/ca.canifishthis.app`
   (Google-APIs image, not Play) → under 25 MB, and `tar tvf` of that file lists only the included paths.
5. Photos past the limit: add 40 photos, back up again → still success, still no `spots/photos/` in the archive.
6. Restore: `adb shell bmgr list sets` (token); `adb uninstall ca.canifishthis.app; adb install app.apk` (restore at
   install), or `adb shell bmgr restore <token> ca.canifishthis.app` → the spots and settings back, the app asks for the
   release download, photos missing (said by the line once (b) lands).
7. Transfer: `adb shell settings put secure backup_local_transport_parameters "is_device_transfer=true"`, back up, restore
   → the archive and the restore include `spots/photos/`, never `release/`.
8. Reset: `adb shell settings delete secure backup_local_transport_parameters`; transport back to the default.

### 8. Emulator proof: RESULTS (2026-11-14, `n1_pixel`, Android 15 / API 35 google_apis, lane 1)
Branch rebased onto origin `redesign/stream-sections` c739bdd75 (build 32 = 0.7.10), head 87ec94d38. Test-bed APK built
from that head by `session.sh --build` (stamp: commit 87ec94d38 clean, patched MapLibre build id 7bf3dc29…, versionCode
32). BEFORE APK: the T717 test-bed build (0.7.9, versionCode 31, no rules; `--allow-stale`). Release r-f749475d0962
installed on the device throughout (`files/release` 473,848 KB). Transport: `com.android.localtransport/.LocalTransport`,
`backup_local_transport_parameters` `is_encrypted=true` (cloud) or `is_encrypted=true,is_device_transfer=true`
(transfer). The local transport keeps its set at `/data/user/0/com.android.localtransport/files/1/_full/<pkg>` on this
image (not `/data/cache/backup/…`, as step 4 above guessed). Scripts and logs: the backup lane's scratch
`t732proof/` (`before.sh`, `after.sh`, `diag.sh`, `restore.sh`, `session{A,B,C,D}.log`, the pulled archives).

| # | Step | Command / key output | Result |
|---|---|---|---|
| 1 | Rules in the APK | `aapt2 dump xmltree` of the installed `base.apk`: `allowBackup=true`, `fullBackupContent=@0x7f140000` (xml/backup_rules), `dataExtractionRules=@0x7f140002` (xml/data_extraction_rules); both files dumped (R8 renames them `res/Qq.xml`, `res/4j.xml`): cloud-backup 9 includes, device-transfer the same 9 + `spots/photos`, full-backup-content the 9 | **PASS** |
| 2+3 | BEFORE (no rules, release installed) | `bmgr backupnow --monitor-verbose ca.canifishthis.app` → `ERROR_PREFLIGHT … (v31)`, `Package ca.canifishthis.app with result: Size quota exceeded`; logcat `PFTBT: Error -1005 backing up ca.canifishthis.app`, `Transport quota exceeded for package`; no archive written. Behind it (`du -sk`): `files/release` 473,848 KB, `synopsis-…pdf` 11,452 KB, `cache` 23,024 KB, `files/feeds` 5,728 KB, `files/spots` 1,560 KB, app total 516,548 KB. The preflight measured every file, `mbgl-offline.db` (312 KB, in `files/`: the ticket's guess confirmed) and `shared_prefs/bg-download.xml` included | **PASS** (the gap is real: nothing is backed up) |
| 4 | AFTER (T732 APK, same release, 3 spots made through the UI, 2 with a photo, all with notes) | `Package ca.canifishthis.app with result: Success`; archive **17,920 bytes** (vs the 25 MB quota); `tar tvf`: `_manifest`, `f/spots/spots.spots` (4,688), `spots.spots.bak` (4,663), `layers.json`, `notify.json`, `first-run.txt`, `walkthrough.txt`. No release, no synopsis, no `mbgl-offline.db`, no feeds/caches, no `updates.json`, no `install-id.txt`, no shared prefs, no photos. (`low-data.txt`, `crash-reports.txt`, `run-species.json` absent on this device, so not in the set) | **PASS** |
| 5 | Photos past the limit | 8 × 5 MB files added to `spots/photos` (39,160 KB), cloud backup → `Success`, archive still **17,920 bytes**, no `spots/photos` | **PASS** |
| 5b | The same 40 MB, device-transfer rules | `Size quota exceeded` (`PFTBT Error -1005`): the transfer rules DO take the photos (that is why it is measured over 25 MB), but the LOCAL transport applies its 25 MB quota to every full backup. A real device-to-device transfer has no quota (Android docs); the emulator cannot show that | info (emulator limit) |
| 7 | Device transfer (2 real photos) | `Success`, archive **56,320 bytes**: the 6 spot/settings files + `f/spots/photos/` with both photos (17,926 B each); never `release/`. `pm clear`, `bmgr restore 1 ca.canifishthis.app` → `restoreFinished: 0`; restored files: spots, `.bak`, 4 settings, both photos; md5 of every restored file = before; the 3 titles and 3 notes in the spots file; release, synopsis, `updates.json`, `install-id.txt`, `update-log.json`, `gauge-history.json`, `mbgl-offline.db` absent | **PASS** |
| 6 | Cloud restore onto a wiped app | cloud backup 17,920 B → `pm clear` → `bmgr restore 1` → `restoreFinished: 0`; restored: spots, `.bak`, `layers.json`, `notify.json`, `first-run.txt`, `walkthrough.txt` (md5 = before), no photos, none of the excluded files. Then the app opened (no first-run notice: `first-run.txt` came back): it downloaded the release again by itself (`files/release` 473,848 KB again, map up in 282 s), made a NEW `install-id.txt` and fresh `updates.json` (none restored, so no stale validators), the Spots tab lists T732 spot 1-3 with their dates and places, each row NO PHOTO (cloud carries no photos, as designed), notes in the spots file; crash buffer: none | **PASS** |
| 8 | Reset | `settings delete secure backup_local_transport_parameters`; transport back to `com.google.android.gms/.backup.BackupTransportService` (the image's default before the proof) | done |

Found on the way (none of them the rules):
- **The emulator's local transport had a broken `@pm@` set** (its `@ancestral_record@` and `@meta@` keys 0 bytes since
  its first backup here): every `bmgr restore`, and restore-at-install, failed `restoreFinished: -1000` with
  `EOFException` in `PackageManagerBackupAgent.getAncestralRecordVersionValue`, before the app was reached (sessions B,
  C). Removing that set's journal and key files (`/data/backup/com.android.localtransport.LocalTransport/@pm@`,
  `…/files/1/_delta/@pm@`) let the next backup write `@pm@` whole (14 and 4 bytes); both restores then passed.
  Emulator state only; a phone's Google transport keeps its own.
- With nothing included present (a wiped app), the local transport answers `Transport rejected package` (-1002, size
  0): expected, not a failure.
- Restored files carry an mtime of 1970-01-21 (the platform's restore; seen on every restored file). The app reads the
  files' contents, not their dates, as far as the spot store goes; noted, not chased.
- Test bed: the `spots` scenario's `spot-notes` flow fails at its first `takeScreenshot` when `run.mjs --out` is an
  absolute directory outside Maestro's output folder ("resolves outside this run's takeScreenshot output folder");
  the proof's own spot maker passed a relative `SHOT`. The scenario also deletes its spot at the end, so it cannot
  seed data that must stay.

### 9. Built after the owner's answers: the photo backup set (code and tests; emulator proof waits for the go)
- **Rules** (`with-backup-rules.js`): `spots/backup-photos` added to cloud-backup and to `fullBackupContent`
  (Android ≤11 reads the cloud list; the set fits by design). Device-transfer unchanged. `backup-rules.test.ts` pins
  both files whole again.
- **The set** (`packages/data/src/spots/backupSync.ts`, the rules over a file seam; `backupSet.native.ts` the phone's
  files; `backupSet.web.ts` null; `backupSet.d.ts`): every photo a spot or visit holds, at its visit's time; a copy made
  once per photo (`expo-image-manipulator`: rendered, resized to 1600 px on the long edge when larger, saved JPEG 0.7;
  core `BACKUP_PHOTO_EDGE` / `BACKUP_PHOTO_QUALITY`) into `cache/backup-stage/`; core `planBackupSet` with the bytes of
  the spots file, `.bak` and the seven settings files counted first; kept copies moved into `spots/backup-photos/`;
  left-out and stale copies (a photo no spot holds) deleted. Originals are never re-encoded. One sync at a time.
- **When** (`apps/mobile/src/App.tsx`): at the start and after every save or delete (the spots list changes), Android
  only (`Platform.select`, one value: iCloud carries the photos as kept, §5, so iOS makes no copies and shows no line).
- **Restore**: on that same start, a photo a spot holds whose original is missing and whose copy is in the set is
  COPIED into `spots/photos/` (the copy stays); an original that exists is never written over; the photo store is
  reopened so every "no photo" tile asks again. A restored photo is the copy (1600 px), under its original name.
- **Nothing silent**: a copy that cannot be made is reported (`reportFailure`) and counted as on this phone only (the
  line says so); a copy that cannot be put back is reported and its copy kept for the next start.
- **The line** (core `backupLine`, whole-sentence tests): under the Spots list (`SpotsScreen`, `testID="spots-backup"`),
  e.g. "Your phone's backup holds your 4 spots and 3 of 4 photos, made smaller, the newest first. The other 1 photo is
  on this phone only."
- **Tests**: `packages/data/src/spots/backupSync.test.ts` (fit and order, copy made once, stale copy deleted, restore
  never over an original, a failed copy reported and said, a failed restore keeps its copy), `SpotsScreen.test.tsx`
  (the line shown, and not with no spots or no line), `backup-rules.test.ts`.
- **Dependency**: `expo-image-manipulator` 58.0.15 (the SDK 58 `bundledNativeModules` pin), `apps/mobile` + a peer of
  `@app/data` + the root `overrides`; `deps.md` line; `layers.json` lets `packages/data/src/spots` import it.

### 10. Emulator proof of the photo set (2026-11-14 evening, test-bed APK from 76b828cb3, one heavy job, 33 min)
| # | Step | Key output | Result |
|---|---|---|---|
| 1 | Rules in the APK | cloud-backup and full-backup-content: the 9 files + `spots/backup-photos`; device-transfer: the 9 + `spots/photos` | **PASS** |
| 2 | Three spots, each with a 12 MP photo, through the app | 3 copies made in `spots/backup-photos/` (66,021 / 64,477 / 67,426 B; originals 570–681 KB after the picker's own re-encode); the line: "Your phone's backup holds your 6 spots and 3 of 5 photos, made smaller, the newest first. The other 2 photos are on this phone only." The 2 left out are the two older spots' photos, which were **0-byte files** on the device (copied in as root by the earlier proof, then the emulator was killed before they reached disk): no copy could be made, each was reported and said, as designed | **PASS** |
| 2b | Cloud backup | `Success`, set **227,328 bytes**: spots, `.bak`, the settings, `backup-photos/` with the 3 copies | **PASS** |
| 3 | Wipe (`pm clear`) + `bmgr restore 1` | `restoreFinished: 0`; restored: spots, `.bak`, settings, the 3 copies; no `photos/`. The app opened, downloaded the release again (map up in 310 s), and its start-up sync put the 3 copies back in `spots/photos/` (66,021 / 64,477 / 67,426 B, originals never overwritten: there were none); the Spots tab shows all three photos (screenshot `t732proof/out-proof2/restored-spots.png`), line "Your phone's backup holds your 6 spots and all 3 photos, made smaller."; crash buffer: none | **PASS** |
| 4 | ~50 MB of photos (26 noisy 1600×1200 JPEGs, 50.3 MB as files, added to one spot through the picker) | the picker's own re-encode stored them at 24,788 KB for all 29 photos; 29 copies, **20,088 KB**; the line: "Your phone's backup holds your 7 spots and all 29 photos, made smaller."; cloud backup `Success`, set **20,440,576 bytes**, 29 copies in the archive | **PASS**, but **the over-quota path was not reached**: all 29 fit (budget ~20.9 MB). "Some left out, newest first" is shown by `backupSync.test.ts` and core `backupSet.test.ts`, not on the device |

Found: a copy of a photo that is already 1600 px or smaller is barely smaller than its original (the 26 test photos: ~780 KB copy for ~850 KB stored). Cheap follow-up if wanted: when the original is no larger than the long edge and smaller than its copy, keep the original's bytes as the copy.

### Not done / unverified
Not shown on an emulator: a real device-to-device transfer with no quota (needs two phones and Google's D2D transport;
the local transport caps it at 25 MB, step 5b); the Google cloud transport itself (no Play services account on this
image: the local transport stands in for it, same rules engine, same 25 MB quota); Android 11-and-lower's
`fullBackupContent` path (this image is API 35, which reads `dataExtractionRules` only; the file is pinned by the test
and in the APK). Not built: the backup-set worker, the line in the app, the stored-photo resize (all wait on the
`expo-image-manipulator` decision). The privacy and data-safety texts are untouched (other chat's files).

## Owner's answer, 2026-11-16 11:57 (question dialog in the main chat)
Asked the three picks together (screen lock: built NO; copy size 1600 px, about 54 fit, or 1280 px, about 81 fit; new
photos stored at 2048 px, proposed YES), he typed: "1280 is probably enough also could we compress as jpeg or
something which makes the size a lot smaller? ". Readings (the main chat's and mine): copies at 1280 px; no
screen-lock rule and 2048 px for new photos stand as proposed (he did not object).
On his question: the copies are already JPEG (quality 70; 0.37 MB at 1600 px, measured; about 0.25 MB at 1280 px
from the "81 fit" figure). To do before changing the quality: measure JPEG 60 and WebP at 1280 px on the sample photo
and show him one before / after pair; my estimate is 0.17 to 0.20 MB (about 100 to 115 photos), not measured.

## Measured for his question, 2026-11-16 12:57 (the app chat; the comparison picture went to him in chat at 12:59)
The same stand-in photo, copies at 1280×960 (`sips` for JPEG, `cwebp` for WebP; a phone's encoder will differ a little):
JPEG 70 = 245,946 B (about 81 fit in 20 MB); JPEG 60 = 179,946 B (about 110); JPEG 50 = 134,821 B (about 148);
WebP 75 = 81,444 B (about 245); WebP 60 = 65,438 B; WebP 50 = 56,386 B. At full size the app chat sees no difference
between JPEG 70, JPEG 60 and WebP 75 on this photo. Files: scratchpad `t732-pair/` of session 29ad1860 (regenerate
with the two tools). ASKED, NOT ANSWERED: B (JPEG 70), C (JPEG 60) or D (WebP 75); recommended D if a restore on
the emulator shows WebP copies (the copies are Android-only; `expo-image-manipulator` has `SaveFormat.WEBP`; the
copy's file name ends `.jpg` today, so the name or the restore mapping in `photoStore.native.ts` needs a look),
otherwise C. To-Do row `t732-backup-words-confirm` carries the question. Nothing is changed on the branch yet: the
built code is on `t732-proof` @ 56b6ea539 (`app/packages/data/src/spots/backupSet.native.ts`, 1600 px, JPEG 0.7);
`android/t732-backup` is now at fa562f0d6 (the rules commit only), so merge from `t732-proof`.

## Owner, 2026-11-16 ~16:55 (app chat dialog, verbatim label)
Photo copies in a backup, 1280 px (picture `photo-copies-compare.jpg`, sent 12:59, again 16:55): "D: WebP 75" (the
recommendation was B, JPEG 70). OWED before T732 lands: the photo copy step encodes WebP at quality 75.

## Built to "D: WebP 75", 2026-11-16 20:11 (branch `t732-proof`, merged with origin `redesign/stream-sections` b60d6436e)
- **Branches**: `android/t732-backup` fa562f0d6 is the rules commit alone (the same patch as `t732-proof`'s da37d03e1, on
  the older base); `t732-proof` holds the whole of T732 (rules, photo set, both proofs, the owner's answers, this change).
  Land `t732-proof`; `android/t732-backup` adds nothing.
- **Encoding** (core `backupSet.ts`): `BACKUP_PHOTO_EDGE` 1600 -> **1280** (the 11:57 answer, not yet in the code),
  `BACKUP_PHOTO_FORMAT` **"webp"** (new), `BACKUP_PHOTO_QUALITY` 0.7 -> **0.75**. `backupSet.native.ts` saves with
  `SaveFormat.WEBP` (Android `Bitmap.CompressFormat.WEBP`: lossy below 100 on every API level, minSdk 24 included; iOS
  `SDWebImageWebPCoder`, a dependency of the manipulator's pod, though the app runs the set on Android only).
- **Names**: a copy is `<original>.webp` (`backupSync.ts` `backupCopyName`: `s1-1-ab.jpg` -> `s1-1-ab.jpg.webp`), so
  it can never be another photo's. A restore copies it back under the spot's own reference (`photos/s1-1-ab.jpg`, WebP
  bytes): the spot's data is not rewritten, and the image views decode by content, not by name. No mime type is stored
  or sent anywhere on this path; the backup rules (`with-backup-rules.js`) name the folder, not file types, so they are
  unchanged.
- **Before WebP**: a JPEG copy named as its original could exist only on the emulator (the 2026-11-14 test-bed APK from
  76b828cb3; T732 was never on the dev branch, so no staging or Play build made one). It still restores (the WebP copy
  wins when both exist), then a WebP copy is made from the restored photo and the old copy is deleted.
- **Tests**: new `packages/data/src/spots/backupSet.native.test.ts` (fake file system + fake manipulator): the encode
  call pinned as the native module gets it (`resize {width: 1280}` / `{height: 1280}`, `save {format: "webp",
  compress: 0.75}`, none for a photo already smaller), a round trip (copies made, the phone wiped to what the cloud set
  carries, the next start puts each photo back from its WebP copy under its own reference, nothing re-made), and the
  pre-WebP restore. `backupSync.test.ts`: the new names, the pre-WebP restore and replacement, WebP winning over JPEG.
  `CANIFISHTHIS_R8=1 pnpm check`: see the commit.
- **Size** (`cwebp -q 75`, libwebp as Android's encoder; a phone differs a little): the 12 MP stand-in (4,608,757 B)
  -> 1280×960 **82,740 B** (about 250 in 20 MB; JPEG 70 at 1600 px was 373,736 B); the test bed's own photo
  (`app/tools/testbed/media/testbed-photo.jpg`, 800×300, 28,295 B, not resized) -> 8,104 B.
- **Bench owed (the app chat; no backup scenario exists in `app/tools/testbed/scenarios`)**: `spots` and `photo-zoom`
  (a photo picked, shown and zoomed: the store's seam changed), then the manual walk of §10 on a test-bed APK from this
  branch: three spots with a photo each through the app -> `run-as ca.canifishthis.app ls -l files/spots/backup-photos`
  shows `*.jpg.webp` files, `file`/the first bytes say `RIFF....WEBP`, sizes ~60–100 KB for a 12 MP photo; cloud
  `bmgr backupnow` -> `Success`, `tar tvf` lists the `.webp` copies; `pm clear` + `bmgr restore 1` -> the app's next
  start puts them back in `spots/photos/` under their `.jpg` names and the Spots tab and the photo viewer draw them
  (screenshot). Start from a fresh local-transport set (the emulator still holds the 2026-11-14 JPEG set; restoring it
  instead is the pre-WebP path, worth one run too).

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
audit G14, 2026-11-14; owner words relayed by the Android chat ("save spots with prioirty", "images can be compressed if still visible"); owner 2026-11-14 evening: "Only need to backup photos and spots the rest of the data isn't important", "Settings could also be saved"
