# T1009 — Android: lift Play's "DEX code optimization" score from Low; measure all three categories every build

Status: IN PROGRESS — lane L1, branch perf/T1003-r8-release (6f0be8f4d, c34b990ba): the build prints R8's three
scores and fails on a -dont* pass or under the floors (70/70/70; measured 72.0 / 72.3 / 72.5); no library rule narrowed
("Measured" below); left (Claude): the independent review and merge, Play's figures and the card's word after the
0.9.0 Play internal build.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1009 | Android: Play "DEX code optimization: Low": measure optimization / shrinking / obfuscation every build, narrow broad library keep rules, floors in a build check (after T1003) | IN PROGRESS — L1 perf/T1003-r8-release: scores + floors in the build; left: review/merge, Play's figures | 6f0be8f4d, c34b990ba | owner 2026-11-22 (Play Console card) |

Source: owner 2026-11-22, pasting the Play Console card verbatim: "DEX code optimization — Low — Optimize your scores to
improve your memory usage and overall performance for your users. Learn more", with "we want to make this better?
new ticket".

## What
T1003 turns R8 on for every release build and uploads the mapping (Play's "Obfuscation (1%)"). This ticket: Play scores
**optimization, shrinking and obfuscation**; get a good score, and a build that prints the three numbers before Play
does. Low because every production AAB since 0.8.x shipped with R8 off (`plugins/with-no-minify.js`, T250 / T749).

## Done (lane L1, branch perf/T1003-r8-release, 6f0be8f4d, c34b990ba)
- `app/tools/r8-report.mjs check`, wired into `build-apk.sh`: reads `BUNDLE-METADATA/com.android.tools/r8.json` from the
  AAB (the file Play reads since AGP 8.10; AGP 9.2.1, R8 9.2.14); score = 100 minus `stats.noOptimizationPercentage` …;
  fails on `-dontoptimize` / `-dontshrink` / `-dontobfuscate` in the merged `configuration.txt`, on no `proguard.map` /
  no `r8.json`, and under `FLOORS` 70 / 70 / 70 (Play's alert line is 25); prints the "Code shrinker (R8): on;
  optimization …" line for the build output and the Play email.
- Measured, this head R8 ON (arm64, staging env): AAB 39.46 MB (−2.10), APK 58.06 MB (−7.44), DEX 7.92 MB (1 dex, −74%
  vs 30.41 MB); **72.0% / 72.3% / 72.5%**; 6,909 of 8,661 classes renamed; boot "Code" memory 46.1 MB (T749: 70.6 MB R8
  off). 0.8.5 production AAB (versionCode 43, R8 off): Play obfuscation 1%.
- What keeps the other ~28% (`r8-report.mjs rules`): only LIBRARY consumer rules, largest `expo.modules.notifications.**`
  (279 classes / 2,028 members), `io.github.expo.modules.v2.**` (199 / 1,700), `com.facebook.react.bridge.**` (157 /
  1,262), `io.legere.pdfiumandroid.**` (110 / 1,320), `com.horcrux.svg.**` (54 / 1,524), `org.maplibre.geojson.**` (43 /
  421). None offers a narrower switch: narrowing means AGP `optimization { keepRules { ignoreFrom(...) } }` and
  re-stating by hand, each walked by a bench; not done at 72% (owner question in the report). Our own
  `r8-keep-rules.pro` is already narrow (7 rules). Never drop a rule the T749 benches proved needed (Room `<init>()` is
  the T250 crash).
- Bench note: parallel emulator lanes need their own AVD and console port (`TESTBED_AVD=n1_pixel_b TESTBED_PORT=5584`;
  `app/tools/testbed/README.md`).

## Work (left)
1. Independent re-review + merge of PR #44.
2. After the 0.9.0 Play internal build: record Play's three percentages and the card's word (Low / Medium / High) here,
   before and after.
3. The owed R8 device checks (T1003).

## Acceptance
- The build prints the three percentages; a check fails on `-dontoptimize`/`-dontshrink`/`-dontobfuscate` and under
  the floors.
- Play's card no longer says Low; its three figures are in this ticket.
- Every bench passes on the build with narrowed rules.

Out of scope: resource shrinking (stays off, `r8-keep-rules.pro` says why), Hermes JS bytecode (Play scores DEX only).

Risk: medium (narrowed keep rules; each one bench-proved). Size: M. Depends on: T1003. Owner: none until Play's figures.

State 2026-11-22 (stopped for credits): done: R8 on for every release build, mapping to Play, r8-report scores + floors, benches, docs (step 6), review fixes; left: independent re-review + merge of PR #44, the 0.9.0 Play internal build and Play's DEX figures, the owed R8 device checks (T1003); branch perf/T1003-r8-release, head d64401b74 + this line (last fast-fail run whole on 6fd805936; the final one stopped for credits).
