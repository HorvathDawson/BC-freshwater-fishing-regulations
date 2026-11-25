# T1051 — "Are treble hooks allowed here?": a lake's Gear tab says nothing about treble or barbed hooks

Status: PARTIAL — 5c68e2bda, 474f38bb0, 4b45d37e1 (review fix) (0.9.1; needs the 0.9.1 app build AND the 0.9.1 data release for the new glossary term: on 0.9.0 data the line shows with no ⓘ). left (Claude): the three owner questions below go to the owner through the coordinator (the planning chat tracks owner items), then the picked wording is built; the defaults are built meanwhile.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1051 | A lake's Gear tab says nothing about treble or barbed hooks (both allowed; Dick Lake) | PARTIAL — 0.10.0; app build + data release (glossary term); owner questions on the words | 5c68e2bda, 474f38bb0, 4b45d37e1 | staging report 437acc75d93a4a1a (owner 2026-11-24); lane L19 |

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

## Done (lane L20, 2026-11-24; review of #144 the same day)

- The frame ships `hook` (and `fly`) on every gear answer (`answers.generated.ts` `GearAnswer.hook`, `gear.py` `resolve`),
  so core words it. `app/packages/core/src/regs/gear.ts` `hooksAllowed` + `gearLines`: a second Hooks line, per water and
  day, only what is true:
  - `trebles_and_barbs` (no point limit, no barb ban), no fly mode: "Treble and barbed hooks allowed";
  - `any_barbless` (barbless required, points not limited), no fly mode: "Treble hooks allowed" (the row's "Barbless
    hooks" stays);
  - `single` (one point, barbs allowed), or a fly-only water whose answer allows barbs: "Barbed hooks allowed";
  - `single_barbless` (every stream unless its row lifts it): nothing added. A fly is "a single-pointed hook that is
    dressed only with fur, feathers, hair, textiles, tinsel and/or wire" (printed p.80, PDF p.86 of the repo copy), so no
    fly-only water says treble. (The review quoted "a single or double hook dressed …": that is the older wording in
    `gear.py`'s fly-mode comment, not this book's; the comment in `gear.ts` now quotes the printed text.)
- Review HIGH (4b45d37e1): the hook answer is the WHOLE water's. Where a hook rule (points or barb) is held only in a part
  no map draws (`in_part`), on one side or some hours (`side`, `timed`) or in a circumstance (`circumstantial`), an
  unqualified "allowed" is untrue somewhere: no line there, as before T1051. In the 0.9.0 release: François Lake (outlet
  area, map 1), Upper Arrow Lake (north-end drawdown area), Pitt Lake (north of the head-of-lake signs) on every day, and
  Mahood Lake on Jul 1–Dec 31 (its two in-part areas). Their wording is an owner question (pictures below).
- Review MED (4b45d37e1): the ⓘ on "Treble and barbed hooks allowed" and "Barbed hooks allowed" opens a NEW glossary term,
  `treble_and_barbed_hooks` "Treble and barbed hooks" (`pipeline/deliver/answers/glossary.py` `t_treble_and_barbed`): plain
  words, then in the book's words p.80's single-hook definition with its treble note and p.8's "the use of barbed hooks in
  lakes is permitted, unless noted …", pages 8 and 80. The p.80 `def_single_hook` alone backed only the treble half, and
  the "Single barbless hook" term's title contradicts the line. "Treble hooks allowed" (barbless required) keeps
  `def_single_hook`. The term ships in the answers file (0.9.1 data release); an app on 0.9.0 data draws the line without
  its ⓘ (the glossary lacks the id).
- Review LOW: an unknown hook answer draws no line and is reported (`UnknownHookAnswer`, `reportFailure`, W46).
- Tests: `gear.test.ts` "(T1051) …" and "(T1051 review) …" (Dick Lake and Dick Creek whole strings; the truth table; fly
  modes; a point count; François, Mahood, Upper Arrow and Pitt lakes with their in-part rule; side, timed, circumstance;
  the unknown answer reported); `pipeline/tests/test_glossary_treble_barbed.py` (the term quotes both halves). Mutation-
  proved: no line (2 fail), treble on fly-only waters (1), no part guard (2), in_part only (1), any part rule counts (1),
  the unknown answer silent (1). The frozen regs VM fixtures (klwa_1010, klwa_1012, x35p_1010) carry the line.
- Over the 0.9.0 release (`r-0aec88ed3edb`), named waters by what their Hooks row gains over the year (recounted after the
  review fix): "Treble and barbed hooks allowed" every day 7,342 (7,296 lakes, 45 wetlands, Pend-d'Oreille River, RULINGS
  G9), on some days 8 lakes (single barbless on the others: Cowichan, Eagan, Main …) + Osoyoos Lake (both / treble-only by
  day) + Mahood Lake (Jan 1–Jun 30); "Treble hooks allowed" 1 (Quesnel Lake); "Barbed hooks allowed" 37 lakes (single-hook
  lakes such as Andy, Hihium, Whiteswan; fly-only lakes such as Valentine, Nilkitkwa); no line because of a part rule 3
  lakes (François, Upper Arrow, Pitt); nothing added 12,127 streams + 245 lakes.
- Pictures (web 390 px, Nov 24, `T1051-shots/`, light and dark): before (0.9.0 app on the 0.9.0 release)
  `dick-lake-gear-before-*`; after (this branch on a side release cut with this branch's glossary) `dick-lake-gear-after-*`,
  the ⓘ sheet `dick-lake-hooks-sheet-*` and with its book's words open `dick-lake-hooks-sheet-book-words-*`,
  `francois-lake-gear-after-*` (no line); earlier after shots `dick-creek-gear-after-*` (a stream, unchanged),
  `pend-doreille-gear-after-*`, `quesnel-lake-gear-after-*`, `andy-lake-gear-after-*` (their ⓘ then opened the earlier
  terms). PROPOSALS only (not built): `proposal-A-francois-lake-except-in-spots-*` "Treble and barbed hooks allowed, except
  in some spots (see above)" and `proposal-B-francois-lake-most-of-the-lake-*` "Treble and barbed hooks allowed in most
  of the lake; a single barbless hook in some spots (see above)" (the page's "Fishing is different in some spots today"
  block lists the part rule above the Gear section).
- Not done here: Dick Lake's Bait line "Not allowed: Water insects and crayfish · Not on lakes." is T1075 (the Bait
  item, a different code path from the Hooks row).

## Owner questions (built default in brackets)

1. The two partial wordings: "Treble hooks allowed" (barbless required, Quesnel Lake) and "Barbed hooks allowed"
   (single hook required, 37 lakes). [built as written]
2. The ⓘ sheet "Treble and barbed hooks" (`dick-lake-hooks-sheet-*`): its title and plain words. [built as shown]
3. The lakes where a part holds a single barbless hook (François, Upper Arrow, Pitt; Mahood Jul–Dec): A "…, except in some
   spots (see above)", B "… in most of the lake; a single barbless hook in some spots (see above)", or C no line (as
   now). [C until he picks]
