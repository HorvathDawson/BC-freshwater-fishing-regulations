# T1053 — Eagle Creek "including tributaries": the walk stops at its head lake (Dick Lake), and Sources never says "tributaries"

Status: OPEN — 0.9.1 for part 1 (Sources words); part 2 waits on an owner ruling: OWNER-TODO "T1053 walk through a head lake".

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
