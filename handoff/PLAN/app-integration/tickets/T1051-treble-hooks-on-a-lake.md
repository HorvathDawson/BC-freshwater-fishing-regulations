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

## What
Dick Lake (Region 2, 17 ha, no row of its own) showed "Hooks: One hook, lure or fly" and nothing about points or barbs.
The answer is **yes: treble hooks and barbed hooks are allowed on Dick Lake** (missing, not wrong). Book: p.80 "Note: use
of a treble hook is permitted unless "single hook" is specified."; p.8 "(Note: the use of barbed hooks in lakes is
permitted, unless noted in the Regional Water-Specific Tables)."; p.21 Region 2 single barbless binds streams only
(`zp:barbless_single_hook_streams.r1/r3`, `z2:single_barbless_hook.r1`: `feature_types: [stream]`). Cause: the frame sets
`hook = "trebles_and_barbs"` (`pipeline/deliver/answers/gear.py:589-595`) but ROUND13 G (`NO_HOOK_RESTRICTION`,
`gear.py:341-344`) named no hook.

Owner answer (2026-11-24 ~12:33 MST, verbatim): **"Say it everywhere (Recommended)"** (a second Hooks line, "Treble and
barbed hooks allowed" with an info button to the book's wording, on every water where it is true). This REPLACES
ROUND13 G for the Hooks row; the summary sentence still names no hook there.

Size: S. Needs: owner pick; app build (data release only if the frame lacks the field). Safe for 0.9.1.
Depends: none. Owner: yes (the words). Default: A, built only once picked.

## Done (lane L20, 2026-11-24; review of #144 the same day)
- `app/packages/core/src/regs/gear.ts` `hooksAllowed` + `gearLines` (the frame ships `hook` and `fly` on every gear
  answer): `trebles_and_barbs`, no fly mode → "Treble and barbed hooks allowed"; `any_barbless` → "Treble hooks allowed";
  `single` or a fly-only water allowing barbs → "Barbed hooks allowed"; `single_barbless` → nothing. No fly-only water
  says treble (a fly is "a single-pointed hook that is dressed only with fur, feathers, hair, textiles, tinsel and/or
  wire", printed p.80).
- Review HIGH (4b45d37e1): a hook rule held only `in_part` / `side` / `timed` / `circumstantial` → no line. In 0.9.0:
  François Lake, Upper Arrow Lake, Pitt Lake every day; Mahood Lake Jul 1–Dec 31 (owner question 3).
- Review MED (4b45d37e1): the ⓘ opens a NEW glossary term `treble_and_barbed_hooks` "Treble and barbed hooks"
  (`pipeline/deliver/answers/glossary.py` `t_treble_and_barbed`, pages 8 and 80); "Treble hooks allowed" keeps
  `def_single_hook`. Ships in the answers file (0.9.1 data release); on 0.9.0 data the line has no ⓘ.
- Review LOW: an unknown hook answer draws no line and is reported (`UnknownHookAnswer`, `reportFailure`, W46).
- Tests: `gear.test.ts` "(T1051) …" / "(T1051 review) …", `pipeline/tests/test_glossary_treble_barbed.py`,
  mutation-proved; frozen regs VM fixtures (klwa_1010, klwa_1012, x35p_1010) carry the line.
- Counts over `r-0aec88ed3edb`: "Treble and barbed hooks allowed" every day 7,342 (7,296 lakes, 45 wetlands,
  Pend-d'Oreille River), some days 8 lakes + Osoyoos + Mahood (Jan 1–Jun 30); "Treble hooks allowed" 1 (Quesnel Lake);
  "Barbed hooks allowed" 37 lakes; no line for a part rule 3 lakes; nothing added 12,127 streams + 245 lakes.
- Pictures (`T1051-shots/`, 390 px, light and dark): `dick-lake-gear-before-*`, `dick-lake-gear-after-*`,
  `dick-lake-hooks-sheet-*`, `dick-lake-hooks-sheet-book-words-*`, `francois-lake-gear-after-*`, `dick-creek-gear-after-*`,
  `pend-doreille-gear-after-*`, `quesnel-lake-gear-after-*`, `andy-lake-gear-after-*`. PROPOSALS only (not built):
  `proposal-A-francois-lake-except-in-spots-*` "Treble and barbed hooks allowed, except in some spots (see above)" and
  `proposal-B-francois-lake-most-of-the-lake-*` "Treble and barbed hooks allowed in most of the lake; a single barbless
  hook in some spots (see above)".
- Not here: Dick Lake's Bait line "Not allowed: Water insects and crayfish · Not on lakes." is T1075.

## Owner questions (built default in brackets)

1. The two partial wordings: "Treble hooks allowed" (barbless required, Quesnel Lake) and "Barbed hooks allowed"
   (single hook required, 37 lakes). [built as written]
2. The ⓘ sheet "Treble and barbed hooks" (`dick-lake-hooks-sheet-*`): its title and plain words. [built as shown]
3. The lakes where a part holds a single barbless hook (François, Upper Arrow, Pitt; Mahood Jul–Dec): A "…, except in some
   spots (see above)", B "… in most of the lake; a single barbless hook in some spots (see above)", or C no line (as
   now). [C until he picks]
