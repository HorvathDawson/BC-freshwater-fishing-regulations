# T1242 — The loop-removal trial: why 11,875 braid loops survive the atlas prune, and what removing them changes

Status: IN PROGRESS — lane G, branch `feat/T1242-creekless-loops`: creek-less loops in 0.10.0 (decided 2026-11-27, ruling E31); loops with a creek: left for after 0.10.0 (T1260)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1242 | The loop-removal trial: why 11,875 braid loops survive the atlas prune, and what removing them changes | IN PROGRESS — lane G: creek-less loops in 0.10.0 (E31); loops with a creek: T1260 | — | the PR, then the 0.10.0 re-cut |

Source: the owner, 2026-11-26 (planning chat), verbatim. The option: "D: simplify the water itself" (= option C plus
removing about 5,200 small loops from the atlas; section ids renumber; the lane said it does not fit 0.10.0). Late
folds: "Hold the re-cut for all of them". The tension put to him (D plus "hold the re-cut for everything" makes 0.10.0
wait for an atlas-level change whose unknowns are unmeasured) and his answer: "Run the loop-removal trial first, then
I decide". The notes: `water-options` (option D), `braids-design` (option C there). Related: T1176 (item 2),
T1240, T1241, T1199.

The parts C and D share (T1199, T1240, T1241) are built now, since they are in D either way. Only the loop removal is
the trial.

T1199 (the reserve and park cut rule) WAITS on this trial: his answer of 2026-11-27 to its opens/closes list was "Hold for
the loop-removal trial"; he looks at both together after the report.

## Work
1. **Why do 11,875 loops survive the prune?** They meet every condition the atlas prune states (the river's own
   watershed code, no name of their own, no creek on them), so the prune should have dropped them. Measure why not.
   Fixing that may BE the removal, with no new rule.
2. **A side atlas** with loops under 100 m from their river removed (5,209 loops, 1,231 km), never a channel with its own
   name, its own rule or a closure.
3. Measure, and report with before and after pictures:
   - the bindings diff (an answers diff per rule, AGENTS rule 63): none may change an answer;
   - saved spots: do they survive the section-id renumber (not verified yet);
   - the 558 creek mouths that move onto the main river;
   - the id renumber size, and the full-cycle cost (atlas, reach, tiles, deliver).
4. The report goes to the owner with the lists of every removed line.

## Acceptance
A written report with the numbers above and pictures, no change to `main`'s data. His decision (0.10.0, 0.11.0 or
no) is recorded in the owner line.

Severity: medium (it changes ids if it ships). Model: Opus. Size: L (an atlas cycle). Built by the data and release chat.
Needs the heavy-job queue (`heavy.sh`), one job at a time.

## Decided 2026-11-27 (owner, verbatim; ruling E31)
- "maybe we should just drop loops without tributary input streams for now and simplify as much as we can that way? so
  loops at same wsc level or whatever." Width: "no limit for creekless but we need to decide the none creekless loop
  size we will simplify".
- "for loops we keep named, 22 with gauges. the other ones should be analyzed because the cuts might come from the order
  of simplifaction to cuts ... we can leave lake loops".
- "the simplification should happen at the build level before regulations apply I think. then we can add specific
  exemptions there too lkike we need the sapawnign channels on stave since they have spoecific rules referencing them".
- Built ON by default (`--drop-small-loops inf`; `--keep-small-loops` for comparisons) with `SMALL_LOOP_CREEKS = "keep"`. Keeps: `waters/loop_exemptions.json` (by name),
  the curated cuts, tidal limits, gauge matches and owner calls (`graph.prune.loop_keeps`), lake loops, partial blue
  lines. The pass runs after the leaf prune. T1247's same-piece loops are nearly all these creek-less loops (3,393 of
  3,396 under 100 m), so this pass removes them within its guards.
- Then (2026-11-27): "i think i changed my mind for now we should just save info here and only remvoe the loops with no stream connecting it (if it isnt something like the stave spawning channels or following our other rules for when they cant be removed...) but we will leave loops that have a stream connecting for now. it is too much to figure out right now". Loops with a creek: left for after 0.10.0 (T1260, the planning chat's ticket); the
  creek-link work is parked on branch `feat/T1242-creek-links` with its hand-over in the night notes (`loops/`).
- The Goat River side channel: "it stays by 150m rule ...".

## Owner

- [x] 2026-11-26 — Read the trial's report and decide: the loop removal in 0.10.0, in 0.11.0, or not. Answered 2026-11-27: creek-less loops in 0.10.0, any size (E31).
- [x] 2026-11-27 — Loops WITH a creek: how the creek ends at the river. Answered 2026-11-27: left for now (T1260).
- [x] 2026-11-27 — The Goat River side channel on Lower Kootenay 1B reserve land (5.8 km of 7.3 km): remove it or keep. Answered 2026-11-27: kept ("it stays by 150m rule ...").
