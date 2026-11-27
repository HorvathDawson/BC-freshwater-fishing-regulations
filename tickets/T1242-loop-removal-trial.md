# T1242 — The loop-removal trial: why 11,875 braid loops survive the atlas prune, and what removing them changes

Status: OPEN — a measured trial first; the owner decides 0.10.0 or 0.11.0 after its report; owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1242 | The loop-removal trial: why 11,875 braid loops survive the atlas prune, and what removing them changes | OPEN — trial, then the owner decides; owner: see ## Owner | — | measure why the loops survive; a side atlas with loops under 100 m removed; the report |

Source: the owner, 2026-11-26 (planning chat), verbatim. The option: "D: simplify the water itself" (= option C plus
removing about 5,200 small loops from the atlas; section ids renumber; the lane said it does not fit 0.10.0). Late
folds: "Hold the re-cut for all of them". The tension put to him (D plus "hold the re-cut for everything" makes 0.10.0
wait for an atlas-level change whose unknowns are unmeasured) and his answer: "Run the loop-removal trial first, then
I decide". The notes: `water-options` (option D), `braids-design` (option C there). Related: T1176 (item 2),
T1240, T1241, T1199.

The parts C and D share (T1199, T1240, T1241) are built now, since they are in D either way. Only the loop removal is
the trial.

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

## Owner

- [ ] 2026-11-26 — Read the trial's report and decide: the loop removal in 0.10.0, in 0.11.0, or not. His words: "Run the loop-removal trial first, then I decide". Default: 0.11.0.
