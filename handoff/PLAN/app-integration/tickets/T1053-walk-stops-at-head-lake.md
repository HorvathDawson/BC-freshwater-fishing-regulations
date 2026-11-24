# T1053 — Eagle Creek "including tributaries": the walk stops at its head lake (Dick Lake), and Sources never says "tributaries"

Status: PARTIAL — owner: OWNER-TODO "T1053 walk through a head lake" (read the before/after list, pick the Sources words). Lane L21, 2026-11-24: both parts built on `feat/T1053-walk-climbs-head-lakes` (21f13ae62 walk, 61829dbbf words); waits for the owner to read the before/after list (`T1053-shots/head-lakes-before-after.md`) and pick the Sources words before the 0.9.1 data release. Nothing released.

Source: in-app report `3eaf5ac25fc39d73` (staging feeds worker, the owner, 2026-11-24 18:45 UTC), reason "rule", on
Eagle Creek `gnis:9405`, part 0, "Today, Nov 24", Regulations view, web, app 0.9.0 (build 43, 127bf43), release
r-0aec88ed3edb (reach digest 7b6af6d1ab2452e0). No screenshot attached. The words, verbatim:

> eagle creek here rule includes tributaries the trehnical details in sources dont show that and no streams above dick lake get included? the tributary walk should walk that I think?

Triage: lane L19, 2026-11-24. Picture (staging, 390 px, the rule's sheet with Technical detail open):
`handoff/PLAN/app-integration/T1053-shots/report-now.png`. Evidence (read-only probes on the live atlas + reach run, and
their output): `T1053-shots/probe_walk.py.txt` / `.out`, `T1053-shots/probe_head_lakes.py.txt` / `.out`.

## The row and the binding

`r2:eagle_creek@2-8` (region-2.json; book p.23): "EAGLE CREEK ✱ No Fishing"; entry `includes_tributaries: true`,
symbol "Incl. Tribs"; one rule `eagle_creek.r1` (ALL_GAME_FISH take 0, `extents: [whole]`, no `includes_tributaries` of
its own: it inherits the entry's true, AGENTS rule 9). The reach run's `tributaries` diagnostic:
`{"added": 0, "direct": 1, "only": false, "total": 1}`: Eagle Creek's one section, nothing walked.

## (b) Why nothing above Dick Lake

Eagle Creek is one stream piece (`360548981:0`, wsc 900-087856, 3.0 km). Its top is a `lake_out` edge from Dick Lake
(`lake:328989582`, same wsc 900-087856: Eagle Creek leaves Dick Lake). Dick Creek (`gnis:23637`, wsc 900-087856-839583)
and 9 unnamed pieces (4.5 km of streams, 10 pieces) feed Dick Lake. At the reach's top `may_enter` answers
`RIVER_CONTINUING` for Dick Lake (`pipeline/atlas/graph/tributaries.py` `_continuing`, lines 830-833: a `lake_out` edge is
in `MAINSTEM_EDGE_KINDS`), so the walk never enters the lake and never reaches Dick Creek. As built and pinned:
`pipeline/tests/test_tributary_lakes.py::test_the_walk_never_climbs_past_the_reachs_top` ("lake:top sits ABOVE the reach's
top piece … it is the river continuing above the regulated water, and its feeder is not a tributary of the reach").
On staging Dick Creek is open under Region 2's rules (trout 2 a day, single barbless hook).

Is it the ruling? No written owner ruling covers it. RULINGS T1 (p.80) says the walk collects STREAMS only (Dick Lake
itself is never covered either way); the walk's own doc says lakes are "climbed through — a creek feeding a tributary lake
is still a tributary" (`expand`); and the top-of-reach stop was written for a STRETCH rule, where the river above the
stretch is not its tributary. For a WHOLE-water rule the same stop treats the water's head lake as "the river going on".
The book: p.80 "tributaries: all streams that contribute to a larger stream or to a lake"; the tables' key (p.4): "An
asterisk symbol means the regulation applies to tributary streams as well." Neither says whether a stream feeding the
lake a creek flows out of is that creek's tributary. That is an owner ruling (RULINGS: "If you need a reading that is not
here, ask the user"); this lane changes no binding.

How wide it is (`probe_head_lakes.out`, every walking rule whose extents are all `whole` on a stream item): 119 rule ×
water stops on 57 entries, 59 distinct head lakes, 36 of them with streams above. 12 of the 36 head lakes have a row of
their own (Cowichan, Pitt, Trout (Lardeau), Slocan, Babine Main Body, Lakelse, Cultus (Sweltzer), Burnaby (Brunette),
Beaver (Colquitz), Lower Elk, Weaver, Naglico), about 16,700 km of streams above; 24 have none (Dick, Cameron, Morice,
Meziadin, Tsacha, Sooke, Yakoun, Keogh, Jacobs, Trophy, Nass Lake, …), about 7,100 km. Sums count overlapping
basins more than once.

## (a) Why Sources does not say "including tributaries"

The rule sheet's "Technical detail" is the rule's own export fields (`app/packages/core/src/regs/sheets.ts:46-47`,
`JSON.stringify(r.export.fields)`), where `includes_tributaries` is absent because the rule inherits the entry's (rule 9).
The "applies" lines come from `pipeline/deliver/answers/display/decided.py` `applies_here` / `add_sources` (~lines 158-200,
380-415): the tributary sentence (`applies_trib`, "Inherited from …: its row covers its tributaries.") is attached only to
a rule some part reaches BY THE WALK (`walked`). Eagle Creek's walk took nothing, so no sentence anywhere, and on the own
water's sheet nothing says the row covers its tributaries. (On Nelson Creek, whose walk took 6 pieces, the sentence exists
but reads "Inherited from …", worded for a tributary.)

## Options

Part 1, the words (a wording pick; recommended default A):
- **A:** the own water's rule sheet says the scope in words when the rule (or its entry, rule 9) includes tributaries:
  "Covers Eagle Creek and its tributary streams (✱, p.4)."; and the Technical detail shows the EFFECTIVE value
  (`includes_tributaries: true (from the row)`). Python (`applies_here`) + the export; core draws it.
- **B:** only the Technical detail shows the effective value.

Part 2, the ruling (the owner's):
- **K (as built):** a whole-water rule's walk stops at the head lake its water flows out of; Dick Creek stays under the
  zone rules. Nothing changes; the ruling is written into RULINGS.
- **L (recommended):** a whole-water rule's walk climbs through its head lake when that lake has NO row of its own
  (Dick Lake: Dick Creek and its 9 pieces close); a head lake with its own row stops it (the book handles that lake's
  catchment on its own row, the way T9 treats a joining water with a row). The lake itself stays uncovered (T1). 24 head lakes.
- **M:** climbs through every head lake (36, incl. Cowichan, Pitt, Slocan, Babine): far wider, competing with those
  lakes' own rows (T4, T14).

## Work

1. Part 1 after the pick: `applies_here` / sheet words, whole-string tests, shots light + dark. App build + the 0.9.1 data
   release (answers). Size S.
2. Part 2 after the ruling: L or M in `tributaries.py` (`_continuing` at a whole-water reach's top, a named policy
   constant with a mutation test), `POLICY_VERSION` bump with its changelog line (rule 20), a side reach run and a per-rule
   binding diff (rule 63) for the owner before the 0.9.1 data release; RULINGS row. Size L (a reach policy change across
   57 entries). Safe for 0.9.1 only if the ruling comes early and the binding diff is read before the data release cut.

Size: S (part 1) + L (part 2). Needs: owner pick (words) and owner ruling (walk); pipeline + data release; app build for
part 1. Depends: none. Owner: yes. Default: part 1 A once picked; part 2 K (as built) until the owner rules.

## The owner's answer (2026-11-24 ~12:33 MST), verbatim

Question: "Eagle Creek: its rule covers tributaries, but the walk stops at Dick Lake (the lake Eagle Creek flows out of),
so Dick Creek and 9 small streams above the lake stay on the zone rules. Province-wide, 36 such head lakes have streams
above them; 12 of those lakes have a rules row of their own. Should a river's tributary rule climb through its head
lake?"

Answer: **"Climb through every head lake"** (the option: all 36, even where the lake has its own row; also behind a
before/after list). Not the recommended option (L: climb only where the lake has no row of its own). The Sources wording
was folded into the options, not asked alone: the recommended wording is built as a PROPOSAL (part 1 below).

## PROPOSED ruling (RULINGS T18; the owner's answer, and the cases it did not name, for him to confirm on the list)

The owner's words decide the main case: a rule bound to its WHOLE water whose row covers tributaries walks up through
the lake its water flows out of, and every stream feeding that lake (and what feeds those) is the rule's tributary. The
cases it did not name, as built (each a proposal):

1. **What the climbed rule binds: streams only, never the lake.** The head lake is walked and never covered (T1:
   tributaries are streams), exactly as a lake in the MIDDLE of a river is today (`LAKE_MID_REACH_IS_THE_RIVER`: walked
   as the river passing through, never collected, its inflows tributaries). So head lakes and through lakes behave
   alike. A lake with its own row keeps its own row's rules; a lake without one stays on the zone's (Dick Lake stays on
   Region 2's). The stream on the outlet's own blue line above the lake is a tributary too: nothing of the rule's own
   water lies above its head lake, so nothing there is "the river going on".
2. **A stream above with a row of its own** gets both its own rules and the inherited ones (T4); where two ✱ rows'
   walks reach the same stream (the river's, climbed, and the head lake's own ✱ row), the NEARER row speaks on each key
   (T14: the lake's row, its water drains into the river's), except that an inherited CLOSURE still closes (T4/T14).
   Nothing new: the cascade already resolves a walked tributary this way.
3. **Which rules climb:** a rule of the book whose every extent is `whole` (`reach.build.climbs_head_lakes`). A stretch
   rule ("X River downstream of Y Lake") never climbs, even where the stretch ends at the lake. A DFO place does not
   climb (DFO prints no rows to say which waters are its own; the T743 reason) and a licensing record does not (its walk
   DOWN from each section already reaches the water below the lake, Li5). Both are owner questions if he wants them to.
4. **Which lake is a head lake:** a lake that drains into the rule's water by its outflow edge while NOTHING of the
   rule's own water (its reach or the entry's other matched waters) flows into it, directly or through lakes above
   (`tributaries.head_lakes`). A lake the water flows into is the water passing through or going on, never its head.
   A head lake that is the row's own water ("X Lake and X Creek", a rule bound to the creek) stays its rules' extents
   (AGENTS 63), reported `own_water`.
5. **Where climbing stops above the lake:** every guard the walk already has, unchanged and typed: a carve-out
   (`carved_out`), a code divide (`code_divide`, Dewar Lake), a canal (`barrier`), the receiving river
   (`receiving_river`: a creek FWA codes as a tributary of the river its lake lies on never takes that river — the
   Chehalis/Harrison class), and the order guard (`bigger_river`: a creek that is a lake's SECONDARY outlet does not take
   the lake's whole catchment). Watershed divides: the walk climbs by flow and code, so it never crosses one except by a
   code run the Dewar ruling already gives the lake. Each head lake, climbed or not, is in the reach run's `head_lake`
   diagnostic with its reason (W46).
6. Rules whose row or own field says tributaries are NOT included never walk, so are untouched.

## Built (lane L21, 2026-11-24): part 2 as the owner answered, part 1 as the recommended PROPOSAL

- **Walk** (21f13ae62): `tributaries.A_WHOLE_WATERS_WALK_CLIMBS_ITS_HEAD_LAKE` (policy switch, mutation-pinned),
  `tributaries.head_lakes` / `head_lake_verdicts`, `reach_context(climb_from=…)`; `reach.build.climbs_head_lakes(rule)`
  passes the rule's reach as `climb_from`; licensing passes `head_lakes=False`; the `head_lake` diagnostic lists each
  head lake with `climbed` and its typed reason. `POLICY_VERSION` 50; RULINGS T18; AGENTS 15. Tests:
  `pipeline/tests/test_t1053_head_lakes.py` (18, proved by mutation: the switch off and the `ctx.heads` line off both go red).
- **Sources words** (61829dbbf, PROPOSAL): every rule that reaches tributaries (its own flag, else its row's, AGENTS 9;
  or `tributaries_only`) says it on its `applies` line: "Covers Eagle Creek and its tributary streams (✱, p.4)." — a
  stretch: "Covers the part of X it names and the tributary streams joining that part"; tributaries only: "Covers only
  the tributary streams, not X itself". "(✱, p.4)" only where the row prints the ✱. The row's flag now travels in the
  bundle (`entry.includes_tributaries`) and the export entry; rule fields unchanged. 499 rules carry the sentence.
  Tests: `pipeline/tests/test_t1053_sources_words.py` (whole strings; mutation-proved). Data release only, no app build.
  NOT built: the Technical detail's effective value (option A's second half) — it needs an app change (`sheets.ts`
  prints the rule's own fields); owner's call. Pictures: `T1053-shots/sources-words-{light,dark}.png` (the words from
  the two answers files in a sketch of the sheet, not the app).

## Binding diff (side reach run on the live atlas; reach-only, no atlas rebuild)

Same code with the switch off vs on, and the live release's run vs on: identical — **42 rules change, all gains
(+33,328 rule×section), 0 lost, 0 unresolved, 0 DFO places**; 22 head lakes, 15,894 stream pieces, about 19,500 km.
Every gained piece is above a climbed head lake (or a dead end of a water climbed there), except the one knock-on: a
piece above Babine Lake becomes book-known steelhead water (`zp:steelhead` r1b/r2b/r4b and the licence record
`steelhead_targeting_known` +1). 14 of the triage's 36 lakes do not change (reasons in the list). The owner's list:
`T1053-shots/head-lakes-before-after.md` (per lake: own row?, the river's rules, streams gained, km; plus the answers
before → after for Dick Creek, Eagle Creek's Sources line, Frosst Creek (Cultus Lake, own row), Robertson River
(Cowichan Lake), Schulbuckhand Creek (Lakelse Lake), Fulton River (Babine Lake)). Map: `T1053-shots/eagle-dick-lake-before-after.png`.
Evidence: `T1053-shots/head_lakes.json`, `binding_diff_live_after.txt`, `headlake_list.py.txt`, `answers_view.py.txt`.

**The owner must see before shipping:** Sweltzer River's "No Fishing" now closes every Cultus Lake stream ALL YEAR,
Frosst Creek (own row, seasonal) included; Cowichan's Jul 15-Aug 31 closure reaches every Cowichan Lake stream; Slocan's
closure and trout/char release reach all of Slocan Lake's streams; the Lardeau's exemptions OPEN Trout Lake's streams
(lift direction); the Meziadin's bait ban now covers Meziadin Lake's streams (RU-14's prose said its row stops there).

Not verified: the app on a device or web with this data (no data release, no app build); the full answers diff
(`answers_diff` needs two built releases; the targeted view above reads the export + answers files); data-backed
pytest files that read the canonical export (`test_answers_26`) — not linked in the worktree.
