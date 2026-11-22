# T1009 — Android: lift Play's "DEX code optimization" score from Low; measure all three categories every build

Status: OPEN, after T1003, in the same release batch (0.9.0).

Source: owner 2026-11-22, pasting the Play Console card verbatim: "DEX code optimization — Low — Optimize your scores to
improve your memory usage and overall performance for your users. Learn more", with "we want to make this better?
new ticket".

## How this differs from T1003
T1003 answers Play's notice about **one** category ("Obfuscation (1%)"): it turns R8 on for every release build,
production included, and uploads the mapping file. That alone takes the score off the floor. This ticket is the rest:
Play scores the bundle in **three** categories, **optimization, shrinking and obfuscation** (developer.android.com,
"DEX code optimization", Android vitals), and the card says "Low" overall. The goal here is a score that is good, not
just over the alert line, and a build that tells us the three numbers before Play does.

## Why it is Low today
Every production AAB since 0.8.x ships with R8 off (`android.enableMinifyInReleaseBuilds=false`;
`plugins/with-no-minify.js`, T250 / T749). With R8 off nothing is optimized, shrunk or renamed. T749 measured R8 on
build 33: dex 11.0 MB → 3.6 MB stored in the APK, boot "Code" memory 70.6 → 38–52 MB. That last figure is what the card
means by "memory usage".

## Work
1. **Measure locally, per build.** Run Google's R8 Configuration Analyzer (or the `unzip` + mapping measurement the
   Android vitals page gives) on the release AAB and print the three percentages. Add them to the build output of
   `build-apk.sh --play` and to the owner's release email, next to "Code shrinker (R8): on".
2. **Find what keeps the score down.** From R8's merged configuration (`-printconfiguration`) and `-whyareyoukeeping`,
   list the broad library keep rules (React Native, expo-modules-core, MapLibre, pdfium `io.legere.pdfiumandroid.** { *; }`,
   Gson) and how much each one keeps. For each: narrow it where the library allows (our own `r8-keep-rules.pro` is
   already narrow, each rule with its reason), or record why it stays. Never drop a rule the T749 benches proved
   needed (Room `<init>()` is the T250 crash).
3. **Optimization category.** Confirm `proguard-android-optimize.txt` stays the default file (it is today), no
   `-dontoptimize` / `-dontshrink` / `-dontobfuscate` from us or a library's consumer rules, and R8 full mode (AGP 8
   default). One build check fails when the merged configuration holds any of the three.
4. **Floor in CI-side tooling.** After the first measured build, set a floor per category (a little under the
   measured value) in the build check, so a library update that adds a `{ *; }` rule shows up as a failed build, not
   as a Play notice.
5. **Proof:** the same emulator benches as T1003 step 4 on the build with any narrowed rule (`run.mjs install crash-bed
   bg-install regs-open tap-stalls memory`). A narrowed keep rule crashes only on the path that needs it, so a rule
   is narrowed only when a bench walks that path.
6. **Record Play's figures** after the Play internal upload: the three percentages and the card's word (Low / Medium /
   High) in this ticket, before and after.

## Acceptance
- The build prints the three percentages; a check fails on `-dontoptimize`/`-dontshrink`/`-dontobfuscate` and under
  the floors.
- Play's card no longer says Low; its three figures are in this ticket.
- Every bench passes on the build with narrowed rules.

Out of scope: resource shrinking (stays off, `r8-keep-rules.pro` says why), Hermes JS bytecode (Play scores DEX only).

Risk: medium (narrowed keep rules; each one bench-proved). Size: M. Depends on: T1003. Owner: none until Play's figures.
