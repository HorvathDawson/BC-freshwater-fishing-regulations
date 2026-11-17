# T746 — While new data is applied the app shows a loading mark and a line of text, not a still screen

Status: IN PROGRESS (merged onto `redesign/stream-sections` 2026-11-16 via `merge/held-group1`; left (Claude): the Android benches and a look at the curtain, §4).

Source: owner 2026-11-14 (evening), relayed by the Android chat, verbatim: "also when changes apply to app we want to make
it so that it has a loading icon with text like splash so it doesnt looking like it is frozen. (applying changes as in
new data regs)".

## What is asked
While a new data release is being applied (after its download: checking, swapping the release in, reopening the
databases, the map reloading its style and tiles), the app shows a splash-like state: a loading mark and one line of
text that says what is happening, in the app's plain words. On Android, iPhone and the web.

## Work
1. Find every moment the screen can sit still during an apply, on each platform: at a cold start with a staged
   release, on return to the app, while the app is in use (if an apply can happen then), and on the web when the
   site's release changes under an open tab. For each: what the reader sees today, and how long it lasts (measured on
   the emulator, the simulator and the web).
2. Build one state for all of them (one load path per screen): the loading mark, the line of text, and nothing
   tappable underneath while the swap is in flight. Platform differences only through `.native` / `.web` file pairs.
3. A failed apply says so on the screen and is reported (`reportFailure`); the app keeps the release it had. No silent
   fallback.
4. The words live in `packages/core` with whole-sentence tests. Proposals for the wording go to the owner with a
   picture of each platform.
5. Benches: Android `regs-open`, `tap-stalls`, `update-freeze`, `big-update`; the iOS twins that exist.

## Acceptance
- No still screen longer than about 300 ms during an apply without the loading state up, on the three platforms; the
  measured times before and after are in this ticket.
- A failed apply is shown and reported; the old release still opens.
- The owner has picked the wording from pictures.

Depends: T722 (install branch). Size: M. Owner: wording pick.

## Results

### 1. The still moments, read from the code (branch `ios/t746-apply-splash` off `ios/t722-install`, 2026-11-14)
| path | what the reader saw before | JS thread | work running |
|---|---|---|---|
| in-session apply: the safe moment (map still 3 s), "Apply now", back from the background — phone and web alike (`App.tsx` `apply()` → one generation bump → `useReleaseBoot` boots again) | the OLD Shell, still tappable, no word at all | waiting (network, sliced decodes), then one React commit (T287 Android: ~124 ms blocked; 0 stalls after T312) | native: manifest re-read (5 s timeout), held manifest, paint (sliced), core SQLite open, `warmLike` (sliced), commit; the map restyles only if its values / tiles changed. Web: manifest, paint, commit |
| an apply that FAILS | the old Shell, nothing said: `applyError` was set but read by nobody; a `warmLike` throw was an unhandled rejection (no report, the apply silently never ended) | — | — |
| cold start with a staged set | the boot screen (the fish turns on the native driver / CSS, not JS) saying "Opening the map…" | sliced | `openSet`: paint, core |
| first install, the last step | the boot screen's second bar "Checking the downloaded files · N %" (not still: T272; T718's 44 s was the JS hash, now native on iOS, T722 F14) | — | hash, moves, open |
| web, a new release under an open tab | the same as the in-session apply (the web's update service stages the changed files it has opened, then applies at the safe moment) | same | same |

### 2. Built (commit 889a194ea + this ticket's follow-ups)
- **One state, `ApplyCurtain`** (`ui-native/src/ApplyCurtain.tsx`, drawn by the Shell over everything, inside its back
  layers): from the moment an in-session apply starts it takes every tap (and the system back); it is DRAWN only after
  300 ms and then stays at least 400 ms — the app's existing `SPINNER_TIMING` (T257), so it never flashes for a quick
  apply and never flickers. Drawn, it is the boot screen's look: card colour, the 110 px loading fish, one line.
  Threshold: 300 ms because it is the app's one waiting rule (every spinner shares it) and matches the acceptance's
  "about 300 ms"; an apply under it passes with only the invisible tap stop.
- **A cold start that applies** a set downloaded earlier says the same line on its boot screen (`BootStage` "apply",
  `data/release/boot.ts`: the opened record is read before the set opens; same rule as `Booted.applied`).
- **A failed apply** is said on the curtain ("The new regulations could not be put in place. The app is still using the
  data it had." + OK) and reported (`reportFailure` in the boot's catch); the set in use stays. The `warmLike` throw is
  now that failure too.
- Every way in is the same generation bump, read by `@app/ui` `useApplyCurtain` (one load path).
- No `Platform.OS`; no new dependency; tokens only. The JS thread: the fish turns on the native driver (phone) and in
  CSS (web), so a blocked JS commit does not freeze it; nothing had to be moved off the thread.

### 3. Words
**Owner 2026-11-15, verbatim:** "can we do more fish puns here that are directed at this ?" — so the line is now one of
five fish puns that each still say what is happening (`APPLY_WORDS.lines`, core `install.ts`, whole-sentence tests):
1. "Reeling in the new regulations…"
2. "Netting the latest regulations…"
3. "Landing the new regulations…"
4. "Tying on the new regulations…"
5. "Casting the new regulations into place…"

**How one is chosen:** by the release being applied, never at random — core `applyLine(id)` hashes its `data_release`
id (FNV-1a, 32-bit) to an index. The curtain gets the id from the update service's staged target when `apply()` runs;
a cold start's boot screen gets the same id from the boot (`onStage("apply", id)`). So one apply says one line from
start to end, the boot line and the curtain agree for the same release, and tests are deterministic. No id: line 1.
The screen reader hears the line that is drawn (the fish's label is the same string). The ellipsis is the one
character "…", as the boot screen's own lines ("Opening the map…").

**Fit (one line on the narrowest phone):** measured with the app's face (Archivo 400, the TTF's advance widths) at the
curtain's 15 px: 198–263 px; the longest ("Casting the new regulations into place…") 263 px against 272 px of room at
a 320 px-wide window less the curtain's 24 px sides (the boot screen draws it at 13 px, smaller). None wraps at 320;
larger system text sizes are not covered by this measure. Checked on the simulator (build 2a0ab83f9, iPhone 17, 2026-11-15):
at the default text size every line seen sits on one line; at the largest standard Dynamic Type size
(`content_size extra-extra-extra-large`) the longest, "Casting the new regulations into place…", WRAPS to two lines
(picture `ios-applying-largest-text-casting.jpg`). Not fixed here: it goes to T726 (Dynamic Type).

Pictures of the pun lines (scratchpad `t746/`, build 2a0ab83f9): `ios-applying-light-reeling.jpg` ("Reeling in…",
release …-t746b12), `ios-applying-dark-landing.jpg` ("Landing…", …-t746b15), `ios-cold-start-netting-dark.jpg` (a cold
start's boot screen, "Netting the latest regulations…" under the splash pun, …-t746b16), `web-applying-light-reeling.jpg`,
`web-applying-dark-landing.jpg`.

Failure line, unchanged and plain: "The new regulations could not be put in place. The app is still using the data it had."

History (the first proposal, 2026-11-14, before the owner's answer; not in the app): "Putting the new regulations in
place…" (was the default), "Applying the new regulations data…", "Updating the regulations and map…".

### 4. Measured
Bench: a local stand-in for the data origin (`127.0.0.1:39461`, forwards to data-staging, read-only) answers the
test-channel manifest with a bumped `data_release` (same files) — a "newer release" with no staging write. Sync button
→ staged → the Shell's safe moment applies it. Times are the app's own `[swap]` lines.

**Web** (export of 889a194ea, headless Chrome 402×874, own process, 2026-11-15 00:06, load average 15.6 / 13.3 / 15.9 —
other lanes' jobs running, so these are indicative): apply → committed **346 ms** and **164 ms** (two runs); no long
task over 50 ms between apply and the curtain going. Curtain: blocking at 5–6 ms; run 1 drawn at 307 ms, gone at 708 ms
(the 400 ms hold); run 2 never drawn (164 ms < 300 ms: no flash). Before this change the same stretch was the old map
standing with no word (the work is the same; the curtain adds no work). The map after the commit: this bench's release
has the same files, so the map reloads nothing; a release whose tiles or paint changed is NOT measured.

**iPhone simulator** (build 9c46116f1 `--testbed`, data base = the stand-in, device `t746-17` iPhone 17 / iOS 26.5, heavy.sh
lane 3, 2026-11-15 01:16–01:28; load average 26–52, other lanes' jobs running, so rough figures). The app's own lines:
| way in | apply → committed | still stretch before this change | with the curtain |
|---|---|---|---|
| safe moment (map still 3 s), light | **702 ms** | 702 ms of the old map with no word | blocking at +17 ms, drawn at +300 ms, down at the commit |
| back from the background (staged while search was open) | **880 ms** | 880 ms | same; then 3 JS stalls right after the commit (327, 175, 52 ms) with the curtain already down |
| cold start with a staged set | launch → set opened 1.3 s (JS up at +0.6 s, open 0.7 s) | the boot screen said "Opening the map…" | the boot screen says "Putting the new regulations in place…" (picture) |
| safe moment, manifest held 4 s on purpose (pictures only) | 4723 / 4687 ms | — | the curtain stood the whole time |
- **Commit → the map drawn:** in this bench the release has the same files: after the commit MapLibre drew no frame and
  loaded no tile (`[pan]` frames 0, tiles 0, only React renders), so there is no reload stretch to cover and the curtain
  ends at the commit. A release whose tiles / paint changed (a real atlas update) restyles the map: NOT measured.
- **Finding (follow-up):** on the way back from the background the JS thread stalls ~550 ms just after the commit (327 +
  175 + 52 ms; none after the safe-moment applies). It is not map drawing (no frames); likely the foreground return's
  own work (feed checks, re-renders) landing with the commit. Bench: `ios-bench.mjs` case B (scratchpad `t746/`).
- Staging each "newer release" copied the map assets (776 files) in ~35 s and rechecked the set (~50 s from Sync to
  staged): background work, the map in use meanwhile.

**Android**: not measured (the main lane schedules the device step). To run there: `update-freeze` (the T287 bench:
its "applied at the next pause" step is the back-from-background path; read `[swap] curtain up/down` beside `[swap]
apply` / `committed`), `big-update`, `regs-open`, `tap-stalls`; and a look at the curtain on screen (the test channel,
Sync, the map left still 3 s).

### 5. Pictures (scratchpad `t746/`, JPEG)
`web-applying-light.jpg`, `web-applying-dark.jpg`, `ios-applying-light.jpg`, `ios-applying-dark.jpg`,
`ios-cold-start-applying-dark.jpg` (the boot screen of a cold start that applies). Android: with the device step.

### 6. Not verified
- A failed apply on a device (unit + render tests only: the message, the report, the old set kept).
- Taps landing on the curtain on a device (react-native-web and RN both stop a touch at the top view; not driven by a
  test bed tap during an apply).
- A release with changed tiles / paint (the map's restyle after the commit), on any platform.
- Android, any of it.

### Tests
`ApplyCurtain.render.test.tsx` (timing, no flash, the whole line, back held / closes the failure),
`ApplyCurtain.flow.test.tsx` (every way in through the real hooks; the failure said + reported, old set kept),
`ui/boot.render.test.tsx` (applying flag; warm-up throw = failure), `tools/release-boot.test.ts` (stage "apply" at a cold
start that applies, and after the T403 install-over), `core/install.test.ts` (whole sentences). `pnpm -C app check:fast`
exit 0; the four guard files pass unfiltered; whole vitest suite 634 files / 7658 tests passed.

## Owner answer, 2026-11-15 evening (OWNER-TODO.md "owner answers, round 6", verbatim: "Yes to both (Recommended by it)")
All the proposed fish-pun lines for the "applying new data" screen stay; none struck (To-Do row
`t746-applying-words`). Nothing further is asked of the owner on this ticket.

## Merged, 2026-11-16 (`merge/held-group1`, onto 465eea07d)
The branch was 776 commits behind. Conflicts: `ui/boot.ts`, `apps/mobile/App.tsx`, `core/install.test.ts` (imports), the
ledger and this file. In `useReleaseBoot` both models kept: the head's T760 quiet try (`hidden` / `surface` / `trying`)
and this ticket's `release` on the loading state, `applying` on the ready state and the single `.catch` (a `warmLike`
throw is the apply's failure); the failure path clears `hidden` and sets `trying: false`. A failure is now reported
even when its boot was already superseded (it was dropped unreported before). The cold start's "apply" stage
(`data/release/boot.ts`, the opened record read before the set opens) merged cleanly beside T756's background install.
Nothing of the branch was superseded by the head.


## Held group 1 merge (T746, T738, crash-send, T741), benches 2026-11-16 20:01 (app chat)
`merge/held-group1`: the four branches merged in the owner's order onto the head (conflicts on boot.ts / App.tsx /
install.test.ts resolved onto merge 3's install model; T738's never-down bar now leaves T760's "not moving" line on the
actual bytes, `Progress.had`, with a test; crash-send's worker half was already on the head). `CANIFISHTHIS_R8=1 pnpm
check` rc 0. Android benches on the `g1` APK, all PASS: install, memory, tap-stalls, regs-open (twice), perf,
bg-install, crash-bed, update-freeze and big-update (staging test channel, reset after), crash. The `crash` scenario had
never started its stub (a bench gap, also failing on the head APK; fixed in 2d615838f, cherry-picked here); `crash` then
`crash-bed` in one session counts the first's leftover cached-process exit record, so they run in separate sessions.
