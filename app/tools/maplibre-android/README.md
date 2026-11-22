# MapLibre Native Android: the pinned upstream build (T978)

Every Android build (the staging page, Play internal, production, the test bed) ships a MapLibre Native Android SDK built
here from ONE pinned upstream commit, with **no patch of ours**. Owner 2026-11-22: "Pinned upstream main (Recommended)":
build from one pinned upstream main commit; switch back to a release tag once upstream releases one containing 0001.

| file | what |
|---|---|
| `pin.env` | THE pin: `MLN_COMMIT` (upstream sha) and `MLN_VERSION` (the local artifact's version, naming the sha). Why main and not a tag, and when to move: its header |
| `build.sh` | clone the pinned commit + submodules, refuse any local change, build the Vulkan release AAR (arm64-v8a, x86_64), publish to `cache/maplibre/<MLN_VERSION>/` |
| `jobpool.init.gradle` | build-only: caps the native build at `MLN_JOBS` (4) compile jobs and one link |
| `check-loaded.sh` | inside a test-bed session: the installed APK carries the pinned AAR's lib (build id), the app maps it, one screenshot |
| `archive-t420/` | the T420 lean/full patches, their upstream drafts and render-test runner. **No build reads them** (kept for history) |

## Why (short)

Since upstream PR #3406 (2025-06) MapLibre re-applied every stored feature state to every rendered tile on every frame;
on this app that was ~6 fps on the owner's Pixel 9 (T420, T512). Our fix went upstream as PR #4919 (merged 2026-11-20,
9cf8446d). The second half of the old lean patch (0002, "re-upload a bucket only when a changed feature is in it") measured
within noise on top of current main (T978, 2026-11-22: render-thread CPU −2 % / −5 %, ranges overlapping), so it is not
carried. Upstream main itself measured 20–32 % cheaper on the render thread than the old 13.6.1 + lean AAR.

## Build the AAR (HEAVY)

One-time SDK parts (NDK and CMake as `platform/android/buildSrc/src/main/kotlin/Versions.kt` at the pin asks):

```bash
JAVA_HOME="/Applications/Android Studio.app/Contents/jbr/Contents/Home" \
  ~/Library/Android/sdk/cmdline-tools/latest/bin/sdkmanager --install "ndk;28.2.13676358" "cmake;3.31.6"
```

```bash
scripts/heavy.sh mln-aar 30 -- bash app/tools/maplibre-android/build.sh <work dir outside the repo>
# → cache/maplibre/<MLN_VERSION>/org/maplibre/gl/android-sdk-vulkan/<MLN_VERSION>/android-sdk-vulkan-<MLN_VERSION>.aar
#   + cache/maplibre/<MLN_VERSION>/SOURCE.txt (the commit it was built from)
```

Times (2026-11-22, 4 compile jobs, Mac under load): 703 s full; ~4 min after a small source change; clone + submodules
~1–2 min. A worktree without the repo links the main checkout's (`deploy/lib/build-links.sh` prints the exact command).

## How a build uses it

- `deploy/staging/build-apk.sh` (every mode, `--production` too) and `app/tools/testbed/session.sh` default
  `MLN_LOCAL_MAVEN` to `cache/maplibre/<MLN_VERSION>`. `app/apps/mobile/app.config.js` hands that version to the
  binding's config plugin; `plugins/with-local-maplibre.js` adds the repo to Gradle.
- build-apk.sh refuses, before the heavy lock, a repo whose one AAR is not `android-sdk-vulkan-<MLN_VERSION>.aar` (an old
  lean AAR, an experiment), and after the build an APK/AAB whose arm64 `libmaplibre.so` build id is not that AAR's. The
  email / version file line reads `MapLibre build id <id> (pinned upstream <MLN_VERSION>)`.
- `--stock-maplibre="<reason>"` (not with `--production`) builds Maven Central's 13.6.1 instead: for a bisect only.
- The test bed may build an experiment AAR with `MLN_LOCAL_MAVEN=<repo> TESTBED_APK_TAG=<tag>`; its stamp then says
  `LOCAL AAR … not the pin`.
- iOS is not affected: the iPhone uses MapLibre's iOS SDK from its pod, and no MapLibre patch was ever in the iOS build
  (T893). The feature-state fix reaches iOS only with an upstream iOS release.

## Moving the pin

Edit `pin.env` (both lines), run `build.sh`, build a test-bed APK and run `check-loaded.sh` plus the colour check
(Map → Conditions → Map), then note it in the ticket. The binding patch (`app/patches/@maplibre__maplibre-react-native@11.5.0.patch`)
reaches MapLibre's package-private camera calls (`MLRNCameraCache`): a pin move must still compile it (it did on
3a6b955).
