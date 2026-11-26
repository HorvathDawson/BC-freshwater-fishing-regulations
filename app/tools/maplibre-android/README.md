# MapLibre Native Android: the pinned upstream build

Every Android build (the staging page, Play internal, production, the test bed) ships a MapLibre Native Android
SDK built here from one pinned upstream commit, with no patch of ours. Our feature-state fix is upstream (PR #4919),
so the pin is upstream main until a release tag contains it. The retired patches are in `archive-t420/`.

| file | what |
|---|---|
| `pin.env` | the pin: `MLN_COMMIT` (upstream sha) and `MLN_VERSION` (the local artifact's version). Why main and when to move: its header |
| `build.sh` | clone the pinned commit and submodules, refuse any local change, build the Vulkan release AAR (arm64-v8a, x86_64), publish to `cache/maplibre/<MLN_VERSION>/` |
| `jobpool.init.gradle` | build-only: caps the native build at `MLN_JOBS` (4) compile jobs and one link |
| `check-loaded.sh` | inside a test-bed session: the installed APK carries the pinned AAR's lib (build id), the app maps it, one screenshot |
| `archive-t420/` | the retired T420 patches. No build reads them |

## Build the AAR (heavy)

One-time SDK parts (the NDK and CMake that `platform/android/buildSrc/src/main/kotlin/Versions.kt` asks at the pin):

```bash
JAVA_HOME="/Applications/Android Studio.app/Contents/jbr/Contents/Home" \
  ~/Library/Android/sdk/cmdline-tools/latest/bin/sdkmanager --install "ndk;28.2.13676358" "cmake;3.31.6"
scripts/heavy.sh mln-aar 30 -- bash app/tools/maplibre-android/build.sh <work dir outside the repo>
```

The AAR lands at `cache/maplibre/<MLN_VERSION>/org/maplibre/gl/android-sdk-vulkan/<MLN_VERSION>/`, with `SOURCE.txt`
naming the commit. A full build takes about 12 minutes. A worktree without the repo links the main checkout's
(`deploy/lib/build-links.sh` prints the command).

## How a build uses it

- `deploy/staging/build-apk.sh` (every mode) and `app/tools/testbed/session.sh` default `MLN_LOCAL_MAVEN` to that
  repo. `app/apps/mobile/app.config.js` hands the version to the binding's config plugin;
  `plugins/with-local-maplibre.js` adds the repo to Gradle.
- `build-apk.sh` refuses a repo whose one AAR is not `android-sdk-vulkan-<MLN_VERSION>.aar` or whose `SOURCE.txt`
  does not name `MLN_COMMIT`, and an APK or AAB whose arm64 `libmaplibre.so` build id is not that AAR's.
  `--stock-maplibre` is refused in every mode except the test bed's (`session.sh`, with a reason: Maven
  Central's 13.6.1, for an emulator bisect, never shipped).
- `plugins/with-pinned-maplibre.js`: a production Android config without the pinned repo, or with another
  version, is refused; any other config only warns.
- The test bed may build an experiment AAR with `MLN_LOCAL_MAVEN=<repo> TESTBED_APK_TAG=<tag>`; its stamp says
  `LOCAL AAR ... not the pin`.
- iOS is not affected: it uses MapLibre's iOS pod.

## Moving the pin

Edit both lines of `pin.env`, run `build.sh`, build a test-bed APK, run `check-loaded.sh` and the colour check
(Map, Conditions, Map), then note it in the ticket. The binding patch
(`app/patches/@maplibre__maplibre-react-native@11.5.0.patch`) reaches package-private camera calls
(`MLRNCameraCache`): a pin move must still compile it.
