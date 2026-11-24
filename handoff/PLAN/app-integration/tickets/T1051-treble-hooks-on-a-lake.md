# T1051 — "Are treble hooks allowed here?": a lake's Gear tab says nothing about treble or barbed hooks

Status: OPEN — 0.9.1; owner: OWNER-TODO "T1051 treble hooks on a lake" (which words), then an app build.

Source: in-app report `437acc75d93a4a1a` (staging feeds worker, the owner, 2026-11-24 18:49 UTC), reason "rule", on
Dick Lake `wbk:328989582`, part 0, "Today, Nov 24", Regulations view, web, app 0.9.0 (build 43, 127bf43), release
r-0aec88ed3edb. No screenshot attached. The words, verbatim:

> are we allowed tremble hooks here?

Triage: lane L19, 2026-11-24. Picture (staging, 390 px): `handoff/PLAN/app-integration/T1051-shots/report-now.png`.

## What the screen shows, and the answer

Dick Lake (Region 2, 17 ha, no row of its own) shows "Hooks: One hook, lure or fly" and nothing about points or barbs.
The answer is **yes: treble hooks and barbed hooks are allowed on Dick Lake.** The answer is not wrong, only missing.
On a Region 2 stream the same tab says "Single barbless hook, no trebles ⓘ" (Dick Creek, `gnis:23637`).

The book: p.80 (definitions) "single hook: a hook having only one point. (In contrast, a treble hook is a hook having
three points on a common shaft.) Note: use of a treble hook is permitted unless "single hook" is specified."; p.8 "It is
unlawful to: Use barbed hooks or a hook with more than one point in any river, stream, creek or slough in B.C. (Note:
the use of barbed hooks in lakes is permitted, unless noted in the Regional Water-Specific Tables)."; p.21 (Region 2)
"Single barbless hook: must be used in all streams of Region 2, all year." Neither stream rule binds a lake
(`zp:barbless_single_hook_streams.r1/r3`, `z2:single_barbless_hook.r1`: `feature_types: [stream]`), and Dick Lake has no row.

## Cause

Not one of the gear-rework defects (the barbless inversion, `hook_count`'s three meanings and the 8 waters that lost
"single" were converted to `gear` slots; here `points_per_hook` / `barb` simply do not bind a lake, which is right). The
gear frame knows the answer: `pipeline/deliver/answers/gear.py:589-595` sets `hook = "trebles_and_barbs"` when nothing
restricts points or barbs. By an earlier owner call (ROUND13 G: "any hook" confused) the page then names no hook at all
(`NO_HOOK_RESTRICTION`, `gear.py:341-344`), so the "yes" is never said. The p.8 lake note is stored
(`zp:barbless_single_hook_streams.r2`, an `advisory` on lakes) but not drawn.

## Options (words are the owner's)

- **A (recommended):** where nothing restricts hooks, the Hooks row gets a second line "Treble and barbed hooks
  allowed ⓘ" (the ⓘ opens the p.80 "single hook" definition; the source line adds p.8). Core words it from the frame's
  `hook`; the frame decides.
- **B:** the same line on lakes only (streams always carry the provincial single-barbless rule, so in practice the same
  waters; B says it is a lake fact).
- **C:** keep ROUND13 G (say nothing) and add the p.80 note to the Hooks ⓘ only.

## Work (after the pick)

1. Core (`app/packages/core/src/regs/gear.ts` hooks row, `words.ts`) words `trebles_and_barbs`; confirm the frame ships
   `hook` for every part (if not, the frame change rides the 0.9.1 data release).
2. Whole-string tests (Dick Lake "Treble and barbed hooks allowed"; Dick Creek unchanged), mutation-pinned; shots light + dark.

Size: S. Needs: owner pick; app build (data release only if the frame lacks the field). Safe for 0.9.1.
Depends: none. Owner: yes (the words). Default: A, built only once picked.
