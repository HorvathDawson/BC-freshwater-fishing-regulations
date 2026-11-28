# T1263 — Play Console warning: native libraries not aligned for 16 KB memory pages

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1263 | Play Console warning: native libraries not aligned for 16 KB memory pages | OPEN | — | check the AAB to learn which .so files are unaligned, then upgrade or rebuild them; confirm Play's deadline |

Source: the owner, 2026-11-27, read in Google Play Console (Android, closed testing), verbatim: "Recompile your app with 16 KB native library alignment
Your app uses native libraries that are not aligned to support devices with 16 KB memory page sizes. These devices may not be able to install or start your app, or the app may start and then crash."

## What a read-only look found (2026-11-27; guesses are labelled)
- Native code the app ships (from `app/apps/mobile/package.json` and `modules/`): MapLibre native (`@maplibre/maplibre-react-native` 11.5.0, Vulkan
  variant per `app.json`; a pinned or local build is possible through `plugins/with-pinned-maplibre.js` / `with-local-maplibre.js`), Hermes and
  React Native 0.88 core, Expo modules (`expo-sqlite` SQLite, `expo-image-manipulator`, `expo-notifications`, `expo-location` and others), react-native-gesture-handler,
  and our own modules `modules/bg-download`, `modules/exit-info`, `modules/memory-trim`.
- Which of these `.so` files are NOT 16 KB aligned is UNKNOWN until a built AAB or APK is checked. GUESS: React Native and Expo at these versions
  ship aligned libraries; a prebuilt third-party `.so` (MapLibre, or a pinned fork) is the likelier source.
- R8 is on (`plugins/with-r8.js`); it does not touch `.so` alignment.

## Work
1. Check a release build: `zipalign -c -P 16 -v 4 <apk>` (run on the APK from the AAB), and `unzip -v` / `llvm-readelf -l` on each `lib/arm64-v8a/*.so`
   (LOAD segment alignment must be 0x4000 or more). Or open Play Console > App Bundle Explorer, which lists the unaligned libraries.
   Record the list here. No build is needed if a recent AAB is on hand.
2. For each unaligned library: upgrade its package to a version built for 16 KB; for a pinned MapLibre, rebuild with NDK r28 or newer (or r27 with the
   16 KB flag); for our own modules, check their CMake flags. Packaging: `useLegacyPackaging` must not be on.
3. Test on an Android 15 emulator image with 16 KB pages: install, start, open the map, a water page, a download.
4. Confirm Play's deadline and whether this warning blocks production access or only updates (read Play's page; do not guess). Record the date here.

## Acceptance
The unaligned list recorded; a build whose `zipalign -c -P 16` passes; the app starts and the map draws on a 16 KB emulator; the Play warning gone
after the next upload; Play's deadline written into this ticket.

Depends: none. Size: M (L if MapLibre must be rebuilt). Priority: HIGH (install or start failure on 16 KB devices). Owner: no (he confirms the Play deadline if he can see it in Console). Default: upgrade packages first, rebuild only what stays unaligned.

## Owner

- [ ] 2026-11-27 — If Play Console shows a date for the 16 KB requirement, or says whether it blocks production access, write it here. Default: we read Play's public page.
