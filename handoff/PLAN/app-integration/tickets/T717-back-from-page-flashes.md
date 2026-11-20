# T717 — Back from a water's page flashes while the map loads

Status: DONE 2026-11-20 (6c4547b13, 870ed86fa) — build-32 item 1 ticked (owner ruling 2026-11-20: every check or look is taken as done); emulator back-flash 0 flash frames.

Source: owner 2026-11-14, Android phone on 0.7.9 or 0.7.8, verbatim: "Also when we go back from a regulation tab it
flashes very hard while the map is loading."

## Goal
- Back from a water's page shows the map as it was left: no blank, grey, white or wrong-colour frame, in light and dark,
  standard and satellite ground, from the Map tab and the Conditions map, after a short or a long stay, system back
  and the page's ‹.
- T498's whole-pane open and T716's open at the lift kept; no second map built; one load path; no silent fallbacks.

## How it was measured
Test bed `back-flash` (`node app/tools/testbed/run.mjs back-flash`, new): the map framed on Norrish Creek, the water
tapped on the map, the page left up 2.5 s (30 s for `long`), Back; the whole thing recorded (`screenrecord`, a frame per
change) and cut by `pane-frames.py back`: every frame after Back must be the page or the map as left (mean difference
≤ 8 of 255 from the last frame before the tap), else it is a flash and the case FAILS. Release test-bed APKs, patched
T420 MapLibre, staging r-f749475d0962 on the test channel, emulator `n1_pixel`.

## Cause (evidence)
1. **The map was unmounted under the page.** Once a water's pane was shown, the Shell dropped the browse map
   (`browseLive` was null unless browsing or a landing), so Back built a NEW MapLibre view: its surface painted the
   style's background, then grey, then the tiles came in. In dark mode the new surface first painted the LIGHT
   background (rgb 168,166,160 for ~66 ms) between two near-black frames: the "flashes very hard".
2. **On the Conditions map** the remounted map stayed blank past the recording (≥ 4 s), and the "Gauge reports level
   only" card sat at the bottom edge: the foot's height had been measured as 0 while the page emptied the foot.
3. (Found after fix 1) a map KEPT under the page was handed the colouring of the map in front — while a water is open
   that is the water's own map (`nav.mapTab` = "map") — so a kept Conditions map was restyled to Standard colours
   under the page and back after Back (876 ms of wrong colours).

## What changed (`app/packages/ui-native/src/Shell.tsx`, `shell/paneReveal.ts`)
- `useBrowseKept`: a water opened while the browse screen was showing keeps the ONE browse map mounted, hidden behind
  the page (opacity 0, untouchable, `aria-hidden`), at its browse size (`keptStyle`: the tab bar's row kept, so Back
  does not resize its surface). During T498's hold it is the frozen last-browse element, as before.
- The kept map is handed the colouring of the last browse render (`browseLook`), so it stays as left; Back hands it the
  live colouring (a day changed on the page then repaints it, as it should).
- The foot's height is measured on the browse screen only.
- The tap's cue (T716) is put out once the pane is shown, so the map under the page is as it was before the tap.
- A searched water's arrival framing (`arrive`) now reaches the kept map under the page, so Back shows it framed.

## Before / after (emulator, Back → map as left; flash = frames that were neither the page nor that map)

| case | 0.7.8 (before T716) | 0.7.9 code (HEAD before fix) | after |
|---|---|---|---|
| base (light, Map tab, system back) | 484 ms flash | 285 ms (14 frames) | 0 frames |
| button (the page's ‹) | — | 285 ms | 0 |
| dark | 265 ms, incl. light frames | 183 ms, incl. ~66 ms light beige | 0 |
| satellite | — | 300 ms | 0 |
| conditions | blank ≥ 4 s, card misplaced | blank ≥ 4 s, card misplaced | 0 (4/4 runs; fix 1 alone: 876 ms of Standard colours) |
| long (30 s on the page) | — | 316 ms | 0 |

T716 did not make it worse (0.7.8 was as bad or worse). Memory with the page up (TOTAL PSS): 0.7.8 284 / 302 / 338 MB
(base / dark / conditions, map unmounted); final build 296 / 327 MB (base / dark), satellite 352, long 328, conditions
303-363 over 4 runs: about +10-25 MB for the map kept under the page. Runs (`app/tools/testbed/out/runs/`): before
`2026-11-14T17-10-44` (0.7.9 code), `2026-11-14T17-21-56` (0.7.8); fix 1 `2026-11-14T17-28-07`, `2026-11-14T17-38-45`
(+ `regs-open` 18/18 and double tap PASS, `pane-open` PASS with 20 Map-view holds and no native crash); final fix
(built in a clean worktree: HEAD + this change only) `t717-final-2026-11-14T18-36-43` (base, button, dark, satellite,
long: 0 flash frames) and `t717-final-2026-11-14T18-59-27` (conditions ×4: 0 flash frames). Clips and strips:
`~/bcfish-session-backup-2026-11-12/back-flash/` (before-*.png, after-*.png; mp4s under before-079/, before-078/,
after/).

## Checks
- `back-flash` FAILS on the build before the fix (6/6 cases) and passes after.
- vitest: `TapCue.shell.test.tsx` (T717: the map stays mounted, hidden and unlit under the page, Back shows the same
  map — one mount; on the Conditions map the kept map keeps its Conditions colouring under the page). Both
  mutation-checked: the old unmount gives "no map" / 2 mounts; handing the live colouring fails the second.
  `Search.shell.test.tsx` (fits recorded per map; two mounts, not three), `MapDay` / `MapRail` (the map kept, not gone), `WaterMode.test.tsx` and `apps/mobile/src/back.test.tsx` (press the one map a reader can reach; two reachable still fail — back.test was missed in the first commit and turned app CI red on b74512aee, fixed in the next). The kept map is out of the accessibility tree and takes no touch (TapCue pins pointer-events none).

## Left
- One unexplained app exit in the final run's first Conditions case: the page opened, then the app closed (the
  recording shows the window leaving; no log was kept then). Not seen in 4 more Conditions runs nor in fix 1's
  Conditions opens and 20 Map-view holds. `back-flash` now saves the death's log lines (`*-death.txt`) when it happens.
- Memory with the page up rose 303 → 338 → 363 → 351 MB over the 4 Conditions repeats (no restart between): worth a
  `memory` scenario pass if it keeps climbing.
- Web: the staging smoke's "tap a named water" FAILS on a local web build of HEAD WITHOUT this change too (same tap,
  same timeout), so it is not T717's; the unnamed-stream tap and the rest pass with T717.
- Pane-open, Fraser from the Conditions map: the hold timed out at 1.2 s and the page's small map drew after it was
  shown (5 screens). No recording of HEAD to compare with; not caused here as far as the run shows (T508 territory).
