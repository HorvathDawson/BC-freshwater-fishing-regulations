# T1053 — Eagle Creek "including tributaries": the walk stops at its head lake (Dick Lake), and Sources never says "tributaries"

Status: PARTIAL — the head-lake list is superseded by the walk rulings of 2026-11-26/27 (#365 merged; ## Owner); the Sources words are approved. Lane L21, 2026-11-24: built on `feat/T1053-walk-climbs-head-lakes` (walk 21f13ae62, words 61829dbbf, own-row yield ab9cfb59b + 9159e65fc), PR #166 merged. left (Claude): T1105, T1106 (below). Left (no build, proposals): the Technical detail's effective value (T1105); DFO places and licensing records do not climb (T1106).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1053 | Eagle Creek ✱: the walk stops at its head lake (Dick Lake), and Sources never says "tributaries" | PARTIAL — head-lake list superseded by the walk rulings (#365); words approved; PR #166 merged; left (Claude): T1105, T1106 | 21f13ae62, 61829dbbf, ab9cfb59b, 9159e65fc | staging report 3eaf5ac25fc39d73 (owner 2026-11-24); lane L19 |

Source: in-app report `3eaf5ac25fc39d73` (staging feeds worker, the owner, 2026-11-24 18:45 UTC), reason "rule", on
Eagle Creek `gnis:9405`, part 0, "Today, Nov 24", Regulations view, web, app 0.9.0 (build 43, 127bf43), release
r-0aec88ed3edb (reach digest 7b6af6d1ab2452e0). No screenshot attached. The words, verbatim:

> eagle creek here rule includes tributaries the trehnical details in sources dont show that and no streams above dick lake get included? the tributary walk should walk that I think?

Triage: lane L19, 2026-11-24. Picture (staging, 390 px, the rule's sheet with Technical detail open):
`docs/app-integration/T1053-shots/report-now.png`. Evidence (read-only probes on the live atlas + reach run, and
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

## The owner's second answers, after reading the list (2026-11-24, ~15:10 and ~15:14 MST), verbatim

Question: "T1053 is built as you chose ('climb through every head lake') and the list is sent. It is bigger than Eagle
Creek suggested: 22 lakes, about 19,500 km of streams. Examples: the Babine River's bait ban now covers 61 named streams
above Babine Lake; the Cowichan and Slocan summer closures cover every stream above those lakes; Sweltzer River's 'No
Fishing' closes every Cultus Lake stream all year, including Frosst Creek, which has its own row and is open part of the
year today. Nothing is lost anywhere and nothing else moves. Ship which version in 0.9.1?"
Answer (~15:10): **"All 22, but a stream's own row wins"** (the option: climb everywhere, but a stream above the lake
that has its own row (Frosst Creek) keeps its own rules instead of the river's closure; needs a rebuild of the list for
him to read again).

Question: the Sources wording "Covers Eagle Creek and its tributary streams (✱, p.4)." on 499 rules.
Answer (~15:10): **"Yes, these words (Recommended)"**.

Typed (~15:14): **"make sure upstream includes tributarties still overrides lower includes tributtaries that it
conflicts with. make sense?"**

What the cascade does today (lane L21, read from `pipeline/deliver/bundle/read/ladder.py`): nearest-wins already holds
on every competition key (`OWN_ROW_BEATS_INHERITED`, T4; `NEARER_ROW_BEATS_FARTHER`, T14: the Duncan's bull trout quota
beats Kootenay Lake's tributaries' release on the Duncan's creeks) and an own row's rule replaces an inherited one of the
same kind (`OWN_ROW_REPLACES_INHERITED`, C-5). CLOSURES are the exception in both T4 and T14: an inherited closure still
closes (Granby River, own row, is closed Jul 25-Sept 15 by Kettle River's tributaries' line today). Frosst Creek
(`frosst_creek.r1` "No Fishing Oct 15-Apr 15", no ✱) is that case. How "own row wins" is built for the climb is with the
coordinator / owner (options A walk-level, B climb-only cascade, C general revision of T4/T14).

## The owner's third answer (2026-11-24, relayed by the coordinator; received 15:21 MST by `date`), verbatim

Asked (by the coordinator): build "own row wins" as A (walk-level), B (climb-only cascade), C (revise T4/T14 for every
inherited closure), or keep the earlier ruling — with the facts that Granby River (own row) is closed Jul 25-Sept 15 by
Kettle River's tributaries' line today, and that Frosst Creek's row is "No Fishing Oct 15-Apr 15".

Answer: **"keep earlier rule of closure always winning unless it is exempt) this does not include no fishiong all year
rulings tho those will yield in that case. only the dated closures. add all of this logic to gfotchas file please"**

Then, typed: **"tributary rules dont apply to the lake either only streams that is important too"**

Built (PROPOSAL until he reads the list): in the cascade, not the walk — `pipeline/deliver/bundle/read/ladder.py`
`ALL_YEAR_INHERITED_CLOSURE_YIELDS` (step 3c): an inherited, all-year (`when` empty), full "No Fishing" yields to the
stream's own row or a nearer ✱ row; a dated inherited closure still closes (T4/T14 as before). "Which row counts" is
open: `retention` (built default: that row says something about keeping or closing for the fish) or `any` (any rule of
that row, a bait ban too). Test: `pipeline/tests/test_t1053_all_year_closure_yields.py` (mutation-proved). GOTCHAS:
"Which "including tributaries" rule a stream answers to". RULINGS: T4, T14 and T18 amendments, proposed.

## The owner on unnamed ponds on a creek's line (relayed; received 15:33 MST by `date`), verbatim

Asked (by the coordinator): should the ~3,650 unnamed polygons on a creek's own line (Q124's default) keep following the
creek — yes, part of the creek / no, zone rules / show me.

Answer: **"only the polygon ponds that we have listed as piece of stream. (for example oxbows or slough lakes etc etc )
make sense? using the process we already have"**

So no build change: a polygon takes a stream's rules (walked ones included) only when the existing process lists it
as a piece of that stream (it belongs to the stream's registry item); a lake that is a water of its own is never covered
by a tributary rule. Measured (`lakecheck.py`): 3,657 walked lake polygons live, 3,668 after T1053, all held by stream
items. Pinned: `test_t1053_head_lakes.py::test_a_walk_covers_a_pond_only_where_the_registry_lists_it_as_a_piece_of_the_stream`
(mutation: the registry ignored, or `STREAMS_ONLY` off, both red). **He has in effect answered Q124 (T153's open owner
question) with "using the process we already have": the planning chat can close it.**

## Rebuilt list (lane L21, 2026-11-24 ~22:45 MST): the climb + "own row wins" (all-year closures only)

`T1053-shots/head-lakes-before-after.md`, rebuilt for the owner to read again. Runs: the same reach runs (the walk did
not change); answers built on the climbed bundle with `ALL_YEAR_INHERITED_CLOSURE_YIELDS` off and on.
- **Own row kept above the head lakes: Frosst Creek only** — Sweltzer's all-year "No Fishing" gives way to its own
  "No Fishing Oct 15-Apr 15"; it answers exactly as the live release. Dick Creek (no row) still closes with Eagle Creek.
- **The rule alone, on the live binding** (no climb): `retention` and `any` give the SAME result — 123 stream parts on
  118 waters change, of which ONE changes status: **Dove Creek above Anderson Lake** reopens for catch and release
  (Tsolum River's all-year ✱ "No Fishing" gives way to Anderson Lake's ✱ "Trout and kokanee catch and release"); the
  other 122 only name a different all-year closure (Burnaby Lake's tributaries over the Brunette's, Still Creek over
  Burnaby Lake's tributaries, Cruickshank over Puntledge …). So the "which row counts" question changes nothing today.
- A first run read a zone line ranked as a water (`z6:skeena_nass_winter_closure`) as a stream's own row and reopened
  56 Bulkley/Sustut tributaries; fixed (an own row must be an `r<region>:` water row), rerun, gone.
- Lakes: 3,657 polygons live / 3,668 after, all listed by the process as pieces of streams; no lake water covered.

## Review of #166 (fix round, lane L21, 2026-11-24 ~23:55 MST)

Tests now pin what keeps closed streams closed (every reviewer mutation dies): the water-row guard (a zone line ranked
as a water — `z6:skeena_nass_winter_closure`, the sturgeon and steelhead lines, 9,748 live sections — is never a stream's
own row), only a NEARER ✱ row (`trib_nearer`), the own row must speak for the fish asked, partial closures never yield,
an undrawn rule never yields, and `build.LICENSING_CLIMBS_HEAD_LAKES` (False, pinned in the policy test). The owner list
now reads every answer key, named or not (Dove Creek's top piece + 19 unnamed pieces around Anderson Lake, 21.6 km,
map), names the fish that reopen there (CT, DV, EB, GB, KO, LT, RB, ST; the rest stay closed), and has "where the climb
closes, and where it opens or loosens" (Cameron River newly closed all year at the top; the Lardeau's exemptions and
Perry Creek's brook trout 20 loosen).

How the built rule reads the edge cases (no build change; none yields in today's data unless said):
- a closure printed "Jan 1-Dec 31" carries dates, so it counts as DATED and still closes;
- a one-species all-year closure ("No fishing for X") counts as FULL for that fish and yields like any other (5 inherited
  rows are such; none yields today);
- "X's tributaries" rows and watershed rows count as own rows when they are bound to the stream by their own place (none
  yields today).

## The owner on Dove Creek (2026-11-25, multiple choice, relayed by the coordinator), verbatim

Asked: does a nearer tributary rule that is NOT a closure lift an all-year closure from further downstream (Dove Creek
above Anderson Lake reopening for catch and release, steelhead included)?
Answer: **"Yes, if it is the right lake (Recommended)"**

## Is it the right lake? WRONG (lane L21, 2026-11-25 00:20 MST; nothing in curated data or marks changed)

1. The printed row, Region 1, printed p.14 (repo PDF page 16), MU **1-3**: “ANDERSON” LAKE — "Trout and kokanee catch
   and release … Unnamed lake in the Walbran Creek Watershed approximately 7 km west/southwest of Mt. Walbran" (the
   name in quotes; the corpus carries the clause's tributaries mark on `anderson_lake.r4`). The Tsolum River row:
   printed p.19 (PDF 21), MU 1-6, "No Fishing" ✱.
2. The bound polygon `wbk:329095566` is FWA's gazetted Anderson Lake, 13.7 ha, at 49.731 N, 125.199 W, watershed group
   COMX (MU 1-6), FWA code 920-559826-990728-321151 — on Dove Creek, draining to the Tsolum River near Comox.
3. Five FWA lakes are named Anderson Lake; the only one on Vancouver Island is the Comox one. The printed lake is
   unnamed in FWA. The curated note on the binding (`data/curated/waters/name_variants.json`, target `wbks:
   ["329095566"]`, from the 2026-11-05 naming pass 73c04dbc2) itself says "Lake in MU 1-3. FWA has MU 1-6, regulation
   has MU 1-3 (boundary issue) … BC Lakes Database: survey_id 1129, WBID 00105SANJ, watershed code 930-063200-41400":
   that WBID is FWA's 50k key `00105SANJ`, which FWA gives to **`wbk:329504289`**, an unnamed 10.9 ha lake at 48.678 N,
   124.604 W in the Walbran Creek watershed (group SANJ, 50k code 930063200414…, MU 1-3). The match was made by NAME
   (a regulation name variant pinned to the wrong polygon), not a mark.
4. **Verdict: WRONG lake.** PROPOSAL (for the owner; not applied): move the "ANDERSON LAKE" name variant's target from
   `wbk:329095566` to `wbk:329504289` (the BC Lakes DB's own WBID), then rebuild the entry's `matched`. Effect: the
   Comox Anderson Lake and Dove Creek lose the Walbran lake's fly-only / bait ban / barbless / trout-and-kokanee release
   rules they carry today in the live release (a live error), and with T1053 **nothing reopens**: Dove Creek's top piece
   and the 19 unnamed pieces stay under the Tsolum River's ✱ "No Fishing" (the yield there rests only on
   `anderson_lake.r4`; expected from the key diff, not rerun). The owner list says so.

## The owner on the fix (2026-11-25, multiple choice, relayed), verbatim — T1117

Asked: "Move the row to the unnamed Walbran lake (wbk:329504289)?" Answer: **"Yes, fix it in 0.9.1 (Recommended)"**.
Done in this PR as T1117 (its own ticket): see T1117 for the change and the diff.

T1117 done in this PR (19a8dc392): with the right lake, nothing reopens around Anderson Lake; Frosst Creek is the only
"own row kept"; see T1117 for the diff.

## Owner

- [x] 2026-11-24 — Read the head-lake list (`docs/app-integration/T1053-shots/head-lakes-before-after.md`: 22 head lakes, 42 rules gain water, none lose) and say if any should change; the 0.10.0 chat chose it on your word as a proposal you can change in 0.10.1. → superseded by the walk rulings: P1 "it will keep walking but somehting like lakes own tributaries will replace it IF IT TARGETS THE SAME ITEM. ..." and chains "climb through everything ..." (2026-11-26; full words in `docs/OWNER-RULINGS.md`); "Leave lakes as they are" (2026-11-27); #365 merged.
- [x] 2026-11-24 — Which version of the climb → "All 22, but a stream's own row wins"; Sources words → "Yes, these words (Recommended)"; plus typed: "tributary rules dont apply to the lake either only streams that is important too" (2026-11-24).
- [x] 2026-11-24 — A dated inherited closure still closes unless the row is exempt; an all-year "No Fishing" yields to a stream's own row; the logic goes into GOTCHAS → "keep earlier rule of closure always winning unless it is exempt) this does not include no fishiong all year rulings tho those will yield in that case. only the dated closures. add all of this logic to gfotchas file please" (2026-11-24).
