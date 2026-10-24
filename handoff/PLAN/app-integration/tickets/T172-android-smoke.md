# T172 — Android smoke: show the app builds, installs and runs on the emulator (before cleanup)

Status: DONE — 9cc8724f (2026-10-23; smoke only: the app builds, installs, opens and runs on
the emulator; no code changed; every problem found is a line in T50 "Found by T172")

Source: owner 2026-10-23: "maybe do a simple android emulation before cleanup but do full emulator work and comparison
after cleanup. that way we show it at least works before". The full parity walk and system-bar work stay T50 (after
cleanup).

## Setup (measured 2026-10-23)
Android SDK at ~/Library/Android/sdk (build-tools, cmake, cmdline-tools, emulator, ndk, platform-tools, platforms,
system-images); AVD `n1_pixel` (from the N1 spike, handoff/PLAN/app-data/N1-SPIKE.md); 48 GB RAM. adb/emulator are not
on PATH (use $HOME/Library/Android/sdk/{platform-tools,emulator}); no JAVA_HOME set — use Android Studio's bundled JBR, confirmed present: OpenJDK 21.0.10 at
`/Applications/Android Studio.app/Contents/jbr/Contents/Home` (export JAVA_HOME to it for Gradle).

## Work (smoke only)
1. Boot `n1_pixel` headless (`-no-window` if it runs; else windowed) via scripts/heavy.sh, ALONE (no reach run, app check
   or other emulator at the same time).
2. A development build of app/apps/mobile (`expo prebuild` + `expo run:android`, or the documented script) installed on
   the emulator; data from the local servers (the emulator reaches the Mac at 10.0.2.2:39217; never stop :39217/:39218).
3. Smoke path, a screenshot of each (adb screencap): the app opens; Map tab draws tiles; tap a water → its regulations
   page (Fraser part); Conditions map with gauge colours; Spots tab; More tab. Note crashes, red screens, blank maps and
   any system bar covering UI — do NOT fix parity here (that is T50); only fix a blocker that stops the app opening, or
   list it.
4. Write the result into this ticket (what works, what does not, build time, the exact commands), shots under
   handoff/PLAN/app-integration/T172-shots/; anything found becomes a line in T50.
Size: S (build time dominates). Shut the emulator down at the end (its own PID only).

## Result (2026-10-23, release r-46595c88e76e from :39217, AVD n1_pixel = Pixel 1080×2400 @420 dpi, Android 15 / API 35)

**It works.** Clean dev build, install and first run with no crash (logcat crash buffer empty, 0 FATAL). Shots in
handoff/PLAN/app-integration/T172-shots/.

| Step | Result | Shot |
|---|---|---|
| App opens | YES after clearing the N1 spike's leftover data (see "Did not work" 1); first run downloads the release for offline use (581.6 MB, ≈2 min from :39217) then opens the Map tab | 02, 03 |
| Map tab draws tiles | YES — basemap, labels, regulation colouring (closed / own regulations / base), zone labels, legend, scale | 03 |
| Fraser → Regulations | YES (tapped the river on the map): "Fraser River · 2 stretches", Open today, season bar, rule cards, fish images, rule-book links. Search also works ("Fraser" lists places) once the field gets the tap (see T50 line 1) | 05, 04 |
| Conditions map, gauge colours | YES — flow-vs-past-years colouring ("36 reaches coloured"), Now/+1d/+3d/+5d/+10d chips, legend | 07 |
| Layers sheet | YES | 08 |
| Spots tab | YES (empty state, "Pick on the map") | 09 |
| More tab | YES (data freshness list, Display themes) | 10 |
| Spot capture (long press → pick water → when → summary → save) | YES end to end, live weather fetched | 11–14 |
| T145 tap off a spot clears the selection | YES | 14 → 15 |
| T145 tap ON a spot pin | the map's tap does NOT also fire (guard holds) — but the pin's own press never fires either: nothing happens | 16 |
| T175 water copy, zoom out on a small creek | NOT VERIFIED — adb `input` is single-pointer; a one-finger double-tap-drag only panned. Creek water view drawn (17, 19) | 17–19 |
| Red screens | only the two environment ones below; none from app code | 00, 01 |

### Did not work / environment (not fixed — smoke only)
1. **Stale held release → hard error on open.** The AVD still held the N1 spike's install (`files/release/current` →
   r-6960fe8201e3). The app opened that set and stopped: "The map data could not be loaded. paint: version 1 is not 4"
   (shot 01). `adb shell pm clear ca.canifishthis.app` fixed it (emulator state, not app code) — but a phone with an
   older held set would hit the same wall after an app update; listed in T50.
2. **The AVD boots in airplane mode** (left from N1): the emulator could not reach 10.0.2.2. Fixed per session with
   `adb shell cmd connectivity airplane-mode disable`.
3. **The debug build loads JS from localhost:8081**, not from Expo's dev-client URL (no expo-dev-client in the app), so
   with Metro on another port the first launch is the red "Unable to load script" screen (shot 00). Fixed with
   `adb reverse tcp:8081 tcp:8091`.

### Times
- `expo run:android` (prebuild from clean + Gradle `assembleDebug` 1 m 35 s, 251 tasks + install + launch): **102 s**.
  Metro first bundle 2.1 s (1,478 modules). Emulator cold boot 11 s (`-gpu host`, `-no-window`).
- First-run release install on the emulator: ≈ 2 min for 581.6 MB from the Mac.
- Whole heavy hold (boot → build → smoke → shutdown): 20:36:36–20:51:59, ≈ 15 min.

### Commands (exact)
All inside ONE heavy.sh hold: `scripts/heavy.sh android 30 -- bash <scratch>/t172/session.sh`, where session.sh is:
```bash
export JAVA_HOME="/Applications/Android Studio.app/Contents/jbr/Contents/Home"
export ANDROID_HOME=$HOME/Library/Android/sdk; export PATH="$JAVA_HOME/bin:$ANDROID_HOME/platform-tools:$ANDROID_HOME/emulator:$PATH"
emulator -avd n1_pixel -no-window -no-audio -no-boot-anim -no-snapshot-save -gpu host &       # own PID kept
adb wait-for-device; until [ "$(adb shell getprop sys.boot_completed | tr -d '\r')" = 1 ]; do sleep 3; done
cd app/apps/mobile
EXPO_PUBLIC_DATA_BASE=http://10.0.2.2:39217 CI=1 ../../node_modules/.bin/expo run:android --port 8091 &   # own process group
# … wait for a "done" file while the smoke is driven from outside, then:
kill -TERM -<expo pgid>; (cd android && ./gradlew --stop); adb shell reboot -p; kill <emulator pid if still up>; adb kill-server
```
Driven from outside during the hold:
```bash
adb shell cmd connectivity airplane-mode disable      # AVD state left from N1
adb reverse tcp:8081 tcp:8091                          # debug build fetches JS from localhost:8081
adb shell pm clear ca.canifishthis.app                 # drop the N1 spike's held release r-6960fe8201e3
adb shell am start -W -n ca.canifishthis.app/.MainActivity
adb shell input tap X Y / input swipe X Y X Y 1200 (long press) / input text Fraser / input keyevent 4
adb exec-out screencap -p > handoff/PLAN/app-integration/T172-shots/NN-name.png
adb shell uiautomator dump /sdcard/ui.xml               # bounds of tap targets
```
`EXPO_PUBLIC_DATA_BASE` is the existing config (app/apps/mobile/src/config.ts); nothing hardcoded. The generated
app/apps/mobile/android/ is gitignored (Continuous Native Generation) and was not committed. The Gradle daemon was
stopped, the emulator shut down, Metro (:8091) killed; :39217/:39218 untouched.
