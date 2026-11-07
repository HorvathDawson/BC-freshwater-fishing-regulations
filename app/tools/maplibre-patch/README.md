# MapLibre Native patch: feature state as deltas (T420, prototype)

A test-bed-only build of MapLibre Native Android (Vulkan) with one change: feature state is no longer re-applied to
every rendered tile on every frame. Owner approved the prototype 2026-11-06; shipping it is a separate decision.
**Production builds stay on the stock `org.maplibre.gl:android-sdk-vulkan:13.6.1` from Maven Central**: the patched SDK
is used only when `MLN_LOCAL_MAVEN` is set, and `app.config.js` refuses a production build with it set.

| file | what |
|---|---|
| `feature-state-deltas.patch` | the patch against `android-v13.6.1` (12 source files, +75 −24; plus 3 render tests) |
| `build.sh` | clone + patch + build the AAR (arm64-v8a, x86_64) + publish it to a local Maven repo |
| `jobpool.init.gradle` | build-only: caps the native build at `MLN_JOBS` (4) compile jobs and one link |
| `tests.sh` | the upstream feature-state render tests + related unit tests, on this Mac (Metal) |
| `check-loaded.sh` | inside a test-bed session: the installed APK carries the local AAR's lib, the app maps it, one screenshot |
| `upstream-issue.md` | draft issue for maplibre/maplibre-native (not filed) |

## The defect and the fix

Since upstream PR #3406 (2025-06, b81de155) `SourceFeatureState::coalesceChanges`, called from
`RenderTileSource::prepare` every frame, merges the whole stored state into the frame's changes
(`changes.insert(currentStates…)`) and hands it to every rendered tile. Each tile updates each bucket on that source
layer: a lookup per stored id per data-driven binder, a re-evaluation for each id it holds, and its vertex buffer
marked modified even when nothing matched (on Vulkan that is a new buffer). The cost is ids ever set × tiles × layers,
per frame, whether or not anything changed. Full evidence with file:line: `upstream-issue.md`.

What #3406 fixed: before it, tiles got only the deltas of the frame in which a state changed, so a tile laid out later
(loaded after the state was set, re-parsed, or not rendered that frame) never showed the state.

The patch keeps that and drops the per-frame cost:
- `Tile` records the feature-state version its buckets reflect; `GeometryTile::onLayout` clears it (new buckets).
- `coalesceChanges` bumps the version only in a frame with changes. Per tile: up to date → nothing; one version behind
  in a frame with changes → this frame's deltas; otherwise (new buckets, or it missed a version) → the full state once.
  No copy of `currentStates` per frame.
- Deletions merge into the same frame's changes (before, a deletion overwrote that layer's same-frame updates; the
  per-frame full re-apply hid it).
- `updateVertexVectors` returns whether a vertex was rewritten; buffers are marked modified and buckets re-uploaded only
  then.
- Render tests added (each reuses an existing expected image): `feature-state/update-after-render`,
  `feature-state/remove-key-after-render`, `feature-state/vector-source-new-tiles`.

Not changed: the store still never shrinks (a removal leaves an empty entry), and states still survive a style swap.

## Build (exact commands, 2026-11-06)

One-time SDK parts (the tag's `platform/android/buildSrc/src/main/kotlin/Versions.kt`: NDK 28.2.13676358, CMake 3.24+):

```bash
JAVA_HOME="/Applications/Android Studio.app/Contents/jbr/Contents/Home" \
  ~/Library/Android/sdk/cmdline-tools/latest/bin/sdkmanager --install "ndk;28.2.13676358" "cmake;3.31.6"
```

The AAR (HEAVY; the work dir is outside the repo, never commit what it holds):

```bash
scripts/heavy.sh mln-aar 30 -- bash app/tools/maplibre-patch/build.sh <work dir>
```

`build.sh` clones `android-v13.6.1` with its submodules (all but vcpkg and the doxygen theme, plus the nested ones the
Android build needs: maplibre-tile-spec's earcut/fsst/json, freetype's dlg) into `<work dir>/src`, applies the patch,
writes `13.6.1-fsdelta` into `platform/android/VERSION` (the local artifact's version), then runs

```bash
./gradlew --init-script app/tools/maplibre-patch/jobpool.init.gradle --max-workers=1 \
  -Pmaplibre.abis="arm64-v8a x86_64" -PpublicationRepositoryUrl=file://<work dir>/m2 \
  :MapLibreAndroid:assembleVulkanRelease :MapLibreAndroid:publishVulkanreleasePublicationToConfiguredRepository
```

(upstream's release is `make apackage` = `assembleVulkanRelease` for all four ABIs). Output:
`<work dir>/m2/org/maplibre/gl/android-sdk-vulkan/13.6.1-fsdelta/` (AAR + POM + Gradle module, unsigned).
First build (2026-11-06, 4 compile jobs, both ABIs, clone already in place): 487 s. Clone + submodules: ~85 s.

Tests (HEAVY): `scripts/heavy.sh mln-tests 30 -- bash app/tools/maplibre-patch/tests.sh <work dir>`.
Results 2026-11-06 (Release, Metal; build 158 s): the `feature-state/` filter ran 24 render/query tests, 18 passed
and 6 ignored by upstream's own ignore lists (promote-id-*, fill-pattern/update-feature-state), 0 failed, the 3 new
ones included; 57 unit tests (`Query.*`, `*FeatureState*`, `GeoJSON*`, `Source.*`, `VectorTile*`, `GeometryTile*`,
`CustomGeometryTile*`) passed. Not run: the new render tests against UNPATCHED code (so it is not shown that they
would have caught the old behaviour), the full render suite, Android device tests.

## Wiring (opt-in, test bed only)

- `MLN_LOCAL_MAVEN=<work dir>/m2` at prebuild time. `app/apps/mobile/app.config.js` then (a) hands the one version found
  there to `@maplibre/maplibre-react-native`'s own config plugin as `android.nativeVersion` (that plugin rewrites every
  `org.maplibre.reactnative.*` gradle property, so it must be told through its props), and (b) adds
  `plugins/with-local-maplibre.js`, which adds the local repo to every Gradle project's repositories. The repo must
  hold exactly one version of `android-sdk-vulkan` with its AAR, or the config fails. Unset: neither happens.
- A production build (`EXPO_PUBLIC_APP_ENV=production` or the EAS production profile) with it set is refused.
- `app/tools/testbed/session.sh` refuses `MLN_LOCAL_MAVEN` without `TESTBED_APK_TAG` (the stock test-bed APK is
  kept) and with `--opengl` (the patched SDK is the Vulkan variant).

The test-bed APK with the patch, and the check that it is the lib the app runs:

```bash
MLN_LOCAL_MAVEN=<work dir>/m2 TESTBED_APK_TAG=fsdelta scripts/heavy.sh testbed 30 -- \
  bash app/tools/testbed/session.sh --build -- bash app/tools/maplibre-patch/check-loaded.sh <work dir>/m2 <out dir>
# → app/tools/testbed/out/app-testbed-fsdelta.apk
```

An A/B against the stock APK: the same scenario once per APK, e.g. `TESTBED_APK_TAG=fsdelta` (patched, reuses the
built APK when `--build` is left out) vs no tag (stock). The A/B must be run with the same emulator settings for both.

## On a MapLibre version bump

1. Check upstream first: if `coalesceChanges` no longer merges `currentStates` every frame (or the patch, or an
   equivalent, landed), drop this folder and the wiring.
2. Otherwise set `TAG` in `build.sh` to the version the new `@maplibre/maplibre-react-native` asks for
   (`node_modules/@maplibre/maplibre-react-native/android/gradle.properties`, `nativeVersion`) and `VERSION` to
   `<that>-fsdelta`; read `NDK`/`CMAKE` off the tag's `Versions.kt`.
3. `git apply --check` the patch on the new tag; on a conflict, redo the four points above by hand and regenerate
   the patch with `git add -N metrics/integration/render-tests/feature-state && git diff --binary -- src metrics`.
4. Rebuild, run `tests.sh`, rebuild the test-bed APK, run `check-loaded.sh`, then the A/B.
