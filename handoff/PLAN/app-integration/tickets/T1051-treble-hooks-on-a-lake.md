# T1051 — "Are treble hooks allowed here?": a lake's Gear tab says nothing about treble or barbed hooks

Status: DONE — 5c68e2bda, 474f38bb0 (the frozen VM fixtures) (0.9.1; app only: the frame already ships `hook` and `fly` for every part, so it needs the 0.9.1 app build, no data release; it works on the 0.9.0 data)

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

## Owner answer (2026-11-24 ~12:33 MST, verbatim)

Question: the Gear tab says nothing about treble or barbed hooks. The book's answer is yes, both are allowed... Show it
now?

Answer: **"Say it everywhere (Recommended)"** (option text: a second Hooks line, "Treble and barbed hooks allowed" with
an info button to the book's wording, on every water where it is true). This REPLACES the earlier ROUND13 G call ("any
hook" confused; say no hook at all where nothing restricts hooks): the summary sentence still names no hook there, and
the Hooks row now says what is allowed.

## Done (lane L20, 2026-11-24)

- The frame ships `hook` (and `fly`) on every gear answer (`answers.generated.ts` `GearAnswer.hook`, `gear.py`
  `resolve`), so core words it; no pipeline change. `app/packages/core/src/regs/gear.ts` `hooksAllowed` + `gearLines`:
  a second Hooks line, computed per water and day, only what is true:
  - `trebles_and_barbs` (no point limit, no barb ban), no fly mode: "Treble and barbed hooks allowed", one ⓘ =
    `def_single_hook` (p.80 "use of a treble hook is permitted unless 'single hook' is specified"); the section's source
    line already carries p.8;
  - `any_barbless` (barbless required, points not limited), no fly mode: "Treble hooks allowed" (the row's "Barbless
    hooks" stays), ⓘ `def_single_hook`;
  - `single` (one point, barbs allowed), or a fly-only water whose answer allows barbs: "Barbed hooks allowed", ⓘ
    `single_barbless_hook` (its quote carries p.8 "the use of barbed hooks in lakes is permitted, unless noted");
  - `single_barbless` (every stream unless its row lifts it): nothing added. A fly is "a single-pointed hook" (p.80
    artificial fly), so no fly-only water says treble.
  Words in `words.ts` `GEAR.HOOKS_ALLOWED`, ids in `HOOKS_ALLOWED_TERMS`.
- Tests: `gear.test.ts` "(T1051) …" (Dick Lake and Dick Creek whole strings, the truth table, fly modes, a point count,
  the ⓘ only when the glossary has it). Mutation-proved: no line (2 fail), treble said on fly-only waters (1 fails).
  The fixture's default `hook` moved from `any` (no water has it) to `single_barbless` so the older cases add no line.
- Over the 0.9.0 release (`r-0aec88ed3edb`), named waters by what their Hooks row gains over the year: "Treble and
  barbed hooks allowed" 7,346 (7,300 lakes, 45 wetlands, 1 stream: Pend-d'Oreille River, RULINGS G9) + 8 lakes on some
  days only (Cowichan, Eagan, Main Lake …; single barbless on the others) + Osoyoos Lake (both / treble-only by day);
  "Treble hooks allowed" 1 (Quesnel Lake); "Barbed hooks allowed" 37 lakes (single-hook lakes such as Andy, Hihium,
  Whiteswan; fly-only lakes such as Valentine, Nilkitkwa); nothing added 12,127 streams + 245 lakes. Unnamed sections:
  695,733 gain the both line, 1,180,186 nothing.
- Pictures (web 390 px, Nov 24; before = the 0.9.0 app on the 0.9.0 release, after = this branch on a side release cut with T1050, so its Ice fishing lists no counts): `T1051-shots/dick-lake-gear-before-*.png`,
  `dick-lake-gear-after-*.png`, the stream `dick-creek-gear-after-*.png` (unchanged), `pend-doreille-gear-after-*.png`
  (a stream where both are true), `quesnel-lake-gear-after-*.png`, `andy-lake-gear-after-*.png` (light and dark).
- Not done here: Dick Lake's Bait line "Not allowed: Water insects and crayfish · Not on lakes." is T1075 (the Bait
  item, `bait.ts` / `BAIT` words, a different code path from the Hooks row).
- Owner to confirm (built as default): the two partial wordings ("Treble hooks allowed", "Barbed hooks allowed") and
  the ⓘ on "Barbed hooks allowed" (the Single barbless hook sheet, whose p.8 words say barbs are allowed in lakes).
