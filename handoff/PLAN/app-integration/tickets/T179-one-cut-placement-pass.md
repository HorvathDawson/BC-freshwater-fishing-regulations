# T179 — One cut-placement pass: auto cuts merge into names, real cuts stay apart

Status: PARTIAL (code deef0cec; build + binding diff pending). Owner 2026-10-24: "would it be better to revise how this is done? So that splits can merge with lake or
gauge or other auto generated splits, but also handle real splits close together? This just feels fragile."

## Why (what is fragile today)
`pipeline/atlas/splits/sectionizer.py` cuts one split at a time and decides "same place?" as each arrives
(`_pickup`), so the result depends on ORDER and on a radius:
- One field, two meanings: `proximity_m` is the projection tolerance (default 500 m) AND the pickup radius; a value of
  exactly 500 is read as "inherited, not authored" (`_DEFAULT_PROJECTION_M` sentinel, sectionizer.py:111). Curated
  splits set 30/60/80/100/300 m by hand where two cuts share a small area.
- Authorship is read off the id spelling (`_auto_split`: `gauge__…`, `area:…`, `border:…`, `length:…`) — a list that
  has already been wrong once (the code says it "belongs on SplitPoint").
- A pickup RELABELS the boundary it reuses; names were lost that way (Sproat Lake / `gauge__08HB008`, Spuzzum / the
  R2/3 MU line) and are patched by carrying aliases; 25 boundaries once showed a station code because the gauge ran
  second. Known gaps left on purpose: an auto split may still relabel an INTERIOR lake edge.

## Design (proposal)
Two phases per blue line, order-independent:
1. **Collect** every candidate position first — curated point/line/confluence/MU cuts, lake edges, area, border,
   gauge, length — each with its measure, `authored: bool` (set where each family is minted, never parsed from the
   id) and its id/label.
2. **Place**: cluster positions along the line.
   - An AUTO position within the merge distance of any other position joins it: one boundary, the authored name
     wins, every other id is an alias (the gauge's id stays bindable). Auto never relabels a name.
   - Two AUTHORED positions are never merged by distance — they are both real, however close; a piece between them
     shorter than SLIVER_M is reported as a curation conflict (fix the source), not silently collapsed.
   - An authored cut meeting a lake edge / natural end within the merge distance takes that place (today's Stamp and
     Bella Coola cases).
   Then cut once. `proximity_m` keeps only its projection meaning; per-split pickup radii and the 500 sentinel go.

## Acceptance
- Unit tests: gauge 0.5 m from a lake edge (Sproat) keeps the lake's name + gauge alias; MU line on a confluence
  (Spuzzum) keeps both names; two curated cuts 20 m apart stay two cuts; the same inputs in any order give identical
  boundaries (shuffle test).
- Atlas rebuild + `binding_diff` reviewed (rule AND gauge bindings, memory: split fix changes the trib walk); every
  moved binding explained or fixed before promoting. The curated `proximity_m` pickup values are removed or shown
  unneeded.

Depends: after T61 (2.8 normaliser moves bindings too — not in the same rebuild). Size: L (heavy: atlas rebuild).
Owner: review the binding diff.

## Outcome — PARTIAL (2026-10-24; code deef0cec, build pending)

Built as designed, in `pipeline/atlas/splits/sectionizer.py` (`_plan_blk` collect → place → cut once;
`place_points` is the plan, idempotent under `split_graph_at`):
- Authorship = `SplitPoint.source` / new `SectionBoundary.source`, set where each family is minted;
  `_auto_split`/`_pickup` and the 500 m sentinel are gone (`minted_id` stays for readers holding only an
  id: review app, bundle).
- Authored: NEVER MOVES (owner 2026-10-24, follow-up commit). A minted boundary within 5 m (SLIVER_M) —
  a standing border/gauge cut, or an area/border/length/gauge point in the same call — joins it AT THE
  AUTHORED MEASURE: a standing cut is shifted (`_shift_cut`: both pieces keep id + attributes, geometry,
  fids and the edges in between re-homed), the authored id canonical, the minted id an alias. A lake
  edge / natural end within 5 m is not taken: the point is cut where it stands, the pair goes to
  `split_conflicts.json` (`kind` lake_edge / line_end) and a re-anchoring proposal to
  `split_lake_anchor_proposals.json` (lake: wbk, gnis_id, inlet/outlet, distance, `anchor` {type lake,
  wbk}; mouth: `confluence` on its own blk; source end: `anchor` null). The sliver gate then stops the
  build on the < 5 m piece — except a point on the LAKE side of the edge, which has no piece to cut and
  becomes the edge's alias as before (no sliver, build continues; still reported). Never merged with
  another authored cut by distance unless one spot (<1 m, AT_CUT_M); 1–5 m apart both are cut and the
  pair goes to `split_conflicts.json` (`kind` authored_pair; the sliver gate stops the build —
  2026-10-06 ruling).
- `proximity_m` is projection-only. The 8 curated pickup radii became `"at_edge": true` (reach 300 m to a
  lake edge, end or minted boundary — never another authored cut); their `proximity_m` values stay as
  projection tolerances (unchanged projection).
- Auto: gauge joins the nearest name within 400 m on EITHER side (old: one-sided, order-dependent, read
  off the gauge's 400 m projection value); area/border/length join only the same point; no auto takes a
  bare end. Naming authored > lake > border > area > gauge > length; every other id an alias.
- build.py: curated + gauge + curated-area points placed together before the braid sweep; the sweep skips
  minted points that joined a place; braid pass `exact=True`; length cuts placed too. POLICY_VERSION 34.

Unit tests (`test_sectionizer.py`): Sproat gauge 0.5 m keeps lake name + alias; Spuzzum MU on confluence
keeps both; two curated 20 m apart stay two; 2.5 m apart cut + reported; 40-shuffle order test
(mutation-checked: unsorted authored loop fails it); plan == cut; two-pass order within 5 m.

Predicted binding diff (from the live `splits.resolved.json`; lake edges need the graph):
- 7 gauges below a curated cut now join it (08MH024 CPR bridge Mission 20 m, 08MH076 Kanaka 21 m,
  08MH035 Pitt 43 m, 08HD007 Salmon/Kay Ck 80 m, 08EC001 Fort Babine 88 m, 08MH001 Vedder +200 m,
  08NK030 Elko dam 232 m) and 08GC006/007 (97 m) share one boundary: those slivers vanish, section ids on
  those blks shift, rules ending there gain/lose the sliver, those stations' section_gauge changes.
- Area cuts standing on a gauge boundary now name it (area canonical, gauge alias): end tokens only.
- The 8 `at_edge` waters: unchanged if each radius took a lake edge / end; any that took another
  curated cut now cut separately — check Waneta (Pend d'Oreille) and Corra Linn first.
- split_conflicts.json: authored pairs expected none (all < 0.03 m). NOT expected empty any more:
  of the 23 curated points the live atlas records as picked up, the 8 `at_edge` are unchanged and the 2
  lake anchors (Adams, Pennask) and the Thompson mouth sit exactly on their place; the other 12 either
  stood > 5 m off (already cut separately under deef0cec) or < 5 m off and now: shift a minted cut
  (Kootenay Idaho border onto the border cut) or stay + conflict + proposal at a lake edge / end — Stamp
  (Great Central Lake dam, 0.5 m: a lake_edge conflict; stream side → the gate STOPS the build, the fix
  is to accept the proposal or set at_edge), Morice/Bulkley (resolved at 0.0: a mouth line_end,
  confluence proposal), and any of Ladore, Revelstoke, Koocanusa, Peace Canyon, Cayoosh, Elk railway,
  Meziadin, Halfway within 5 m. Exact list needs the graph (atlas rebuild).
- Follow-up unit tests: minted border cut shifts to the authored 123 m (Idaho border); shift re-homes
  geometry / trib edge / continuation / out_of_bc per side; standing gauge moves to the authored
  500.22 m; area/border/length within 5 m join the authored measure; lake edge 1.5 m / 4 m (outlet /
  inlet) stay + conflict + lake proposal + slivers; source end 3 m stays (report-only), mouth gets a
  confluence proposal. Mutation-checked: letting the authored take a near lake/end fails 2, not
  shifting the minted cut fails 3, dropping the auto 5 m join fails 1.

Follow-up 2 (owner 2026-10-24, code only):
- Proposals reach 25 m (`PROPOSAL_M`): a curated cut within 25 m of a lake edge or its line's mouth gets a
  re-anchor proposal with `band` "0-5m" / "5-25m"; placement unchanged; only < 5 m is also a conflict (a
  source end / minted cut: sliver-only, as before). Expect more proposals than conflicts after the rebuild.
- A merged place keeps every id AND name: new `SectionBoundary.names` / `RegistryBoundary.names` (every
  other label at the place; registry.json writes it when non-empty). Alias-loss holes closed: the
  registry's flowing-polygon fold kept only the last copy of a shared boundary (now unions aliases +
  names); a standing place read one piece's copy of its bound (now unions both); `_coincident`'s border
  case dropped `source` (read back as authored) and the cut's name; an area point placed WITH another
  point is now cut in the aliased call, so a lake-side alias keeps its id; `minted_id` reads the family
  by its first separator. Review app: aliases via `bare_split_id` (a `lake:` ref no longer became a bare
  number), `names` served, label "canonical (also: …)", an extent naming an alias marks the cut used, a
  merged splits.json id is no longer listed "new/pending".
- Tests: `test_merged_place_binds.py` (sectionizer → registry → `resolve_extent`: every alias id binds the
  same sections as the canonical id, upstream/downstream/between; mutation-checked — dropping member ids
  in `_place_boundary`, registry aliases, or `answers_to`'s alias half fails it); sectionizer tests for
  the 25 m band, names in any order, border source, `minted_id` (mutations: radius 5, no `anchored`
  gate, dropping the standing name each fail one).
- No POLICY_VERSION bump: no reach code changed; the graph/registry differences ride the pending
  rebuild that 34 already marks.

Left: atlas rebuild, reach run, rule AND gauge binding diff reviewed (subtract an old-vs-old diff: the
atlas is not deterministic); every moved binding explained before promote.

Close pairs (owner 2026-10-25, code only; side atlas pending):
- Park boundaries — cause: an `area_boundary` split cuts BOTH ends of the park stretch on every line
  (first-enter + last-exit) whatever its label says, and a curated point sat on one of those ends.
  Baker Creek: the "upstream boundary" split also cut the downstream end (7093.95 m), 0.03 m from the
  "downstream boundary" point (7093.92). Tweedsmuir: the Atnarko crosses the park ONCE (enters from the
  east at 63793.1 m, inside to its mouth), so "eastern" and "upstream" are one place by geography; the
  tributary split cut it again 0.02 m from the point. Fix: `SplitAnchor.side` ("upstream" = where the
  water enters the area, "downstream" = where it leaves) picks one end on the NAMED line (descendants
  keep both, so their inside flag stays closed); `exclude_gnis` leaves a named water to its own split.
  splits.json: Baker area split `side: upstream` (-> 8066.6 m), Tweedsmuir area split `exclude_gnis
  [17209]` (still flagged inside). Verified on real data (Baker, Atnarko extracts).
- Guard: `anchors.same_entry_conflicts` — two cuts of ONE splits.json entry within 5 m on one line
  raise in `resolve_split_defs` (the build stops, both ids named); `SplitDef.entry` = the waterbody
  name. Different entries may share a point (Thompson mouth, Coal Ck bridge, Meziadin, Halfway).
- Corpus measure (live splits.resolved.json, every curated area_boundary split vs any other cut
  < 25 m): only 4 curated area_boundary splits exist; failing pairs = Tweedsmuir eastern/upstream
  0.02 m and Baker/Pinnacles downstream/upstream 0.03 m. Others: Wood River/Hamber on the Region 4
  line 0.0 m (one place, fine); Moyie U.S. border vs area:4 12.25 m (below).
- Border: the outline already IS the regions' outer edge (`bc_boundary`: exact union of the wmu units
  the regions dissolve from; checked exactly equal, and the Moyie crosses both at 69.942 m from its
  line start). The 12 m came from the HAND point. New authored anchor `{"type": "border"}` lands ON
  the border pass's own cut (`border_cuts`; `coord` only picks the crossing); placement joins it with
  the border cut, and the region cut follows the border: one boundary, curated id canonical, kind
  `split`, `border:`/`area:4` aliases. Moyie switched (42628.3 -> 42640.55 m). Test:
  `test_close_pairs.py::test_moyie_us_border_and_the_region_4_line_are_one_boundary`.
- Report: `close_pairs.py` — the build writes `close_pairs.json` + prints a count by kind: every
  curated cut 5–25 m from any other boundary (curated, gauge, area, border, length, lake edge, line
  end) on its line, read off the finished graph. Not a gate.
- Expect in the side atlas: Baker loses the 7093.95 area cut on the mainstem (point stands alone),
  Tweedsmuir's Atnarko 63793 is cut by the point only, Moyie's border/region/curated are one boundary
  (anchor digests change for these 3 entries: registry resync per memory).
