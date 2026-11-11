# T665 design: cut creeks where they leave the tidal polygon; take lines wholly inside it off the map

Design only (2026-11-11). Nothing built, no code written. Ticket: `tickets/T665-…md`; measurements: `tickets/T660-…md`
(A–J), `T660-owner-followup.json`, `T660-inside-named.json`. Line numbers are as of commit d2dc3e5fc.

## The short version
- No new stage. The atlas already has a tidal shore stage (`build.cut_shores`, T660). T665 changes the RULE inside it
  and adds one sidecar. Everything after the cut (reach, tiles, bundle proof) keeps the T660 path.
- "Removed" does not mean deleted from the graph. A line wholly inside the polygon stays in the atlas and joins the
  tidal set, exactly like the piece below a tidal limit: not drawn, bound by no rule, answered as tidal water. So no
  item id, blue-line id or curated reference breaks, and tributary walks still run on the full network.
- Expected: 206 lines tidal by the inside rule; about 169 shore cuts (down from 532); 5 more creeks tidal whole.

## 1. Where it belongs

Cuts today, all in `pipeline/atlas/build.py::sectionize` (L904–924), in this order:

| sub-phase | line | what | module |
|---|---|---|---|
| `cut_border` | L951 | B.C. border | `splits/border.py`, `clean_cut.decide` |
| `cut_curated` | L975 | `cuts.json` (the DFO cuts live here), gauges, tidal `cut` rows (`gauge_cuts` L802–834) | `splits/anchors.py` |
| `cut_braids` | L1072 | braid channels across a curated cut | `splits/braid_sweep.py` (`sweep` L667) |
| `cut_areas` | L1102 | parks, regions, MUs | `splits/clean_cut.py`, `area_splits.py` |
| `cut_length` | L1198 | 25 km cap | `splits/length_splits.py` |
| `cut_shores` | L1275 | T660 shore cut | `splits/shore.py` |
| `alias_tidal_mouths` | L927 | `mouth` rows named, never cut | `splits/tidal.py` |
| sliver gate | L1381 | stops on any cut piece under 5 m | `splits/sliver_gate.py` |

Every family is placed and cut by the same two functions: `sectionizer.place_points` (L530) and `split_graph_at`
(L898). Lakes are never cut (a cut in a lake becomes an alias, `_alias_onto_lake` L594; lake splits are deferred).

**Reuse, do not add:**
- `build.cut_shores` (L1275–1326) stays the one tidal-shore stage: same place in the order, same `shore:` family
  (`sectionizer.AUTO_SOURCES` L76), same braid gate, same `shore_cuts.json` record.
- `splits/shore.py`: `shore_cuts` (L79–117) is rewritten to the new rule. `crossing_run` (L51), `share_in` (L120),
  `MIN_SHARE` and the 50 % test go away. `read_tidal_polygon` (L144) stays.
- Polygon geometry comes from the clean cutter, not a new sampler: `clean_cut.decide_chain` (L254) already returns
  "which stretches of this blue line are inside this polygon", exact to the edge, ignoring dips under 5 m, treating a
  5 m run at a line's end as the end (`decide_rejoin` L179–217), and `onto_existing` (L308) snaps a cut onto a boundary
  already standing within 5 m. Parks use it today.
- Reach: `reach/outside.py` `crossing_pieces` (L384) and `tidal_crossing_owner` (L409) are unchanged in logic: "the
  piece at or below a shore cut is tidal when every outlet is tidal". Add one owner kind for the inside lines.
- Not used: new rows in `tidal_limits.json`. Tempting (a `cut` row per creek), but 33 of the creeks already hold an
  owner `mouth` row there (one row per line), 9 are added streams with negative ids the file refuses, and frozen rows
  would not follow when the owner moves a river's limit. A rule in the build follows by itself.

## 2. The rule

One pass of the clean cutter over every blue line against the DFO polygon gives, per line, its inside stretches.
Two rules read that result.

**Rule A: a line wholly inside the polygon is tidal water.** Every stream stretch of the line is inside (dips outside
under 5 m ignored, as for parks). No condition on where it flows. Named or not (owner: all 206).
- Atlas writes `inside_tidal.json` (blk, sections, metres, name, items). `atlas/sidecars.py` gets the same writer so an
  already-built atlas can be given the file without a rebuild (same inputs).
- Reach: `outside.tidal_water` (L334) adds owner `inside:<blk>` for those sections; `is_cut_owner` (L428) accepts the
  prefix, so the tiles (`deliver/tiles/tidal.py::tidal_pieces` L37) and the bundle proof (`bundle/undrawn.py` L53) take
  the same set with no second implementation.
- The 15 near misses (95 % or more inside) are NOT included, as the ticket says.

**Rule B: a creek that joins tidal water is cut once, where it leaves the polygon.**
1. Candidate: the line's mouth is inside the polygon, the line has an outlet (it joins another water), and its water,
   followed down while still inside the polygon, reaches a river that carries a `split`, `cut` or `whole` tidal limit,
   or a Rule A line. This is only a filter (which creeks are worth cutting). It does not decide tidal.
2. Cut point: the top of the inside stretch that starts at the mouth. Exact polygon edge, a geometric point.
3. The reach decides, as today: the piece below the cut is tidal exactly when every outlet is tidal water off the map
   (a tidal piece, a whole tidal water, a Rule A line, or another such piece). Joining above a river's limit: cut, but
   freshwater. No 50 % test anywhere, so nothing is skipped.

**Edge cases (each lands in `shore_cuts.json` with its reason; none is silent):**

| case | what happens | measured |
|---|---|---|
| Line only touches the edge, or dips in under 5 m | ignored: no inside stretch | `DIP_M` |
| Exit within 5 m of the mouth | no cut; the line stays whole and freshwater (a cut on a line's end breaks both halves; same guard as `tidal.refuse_end_cuts` L177) | 0 known |
| Exit within 5 m of the line's top | no cut; the whole line is Rule A (owner ruling 4: slivers go) | 5 of the 177 (exit = line length) |
| Exit within 5 m of a boundary already there (curated cut, park edge, length cut) | the shore cut IS that boundary: an alias, no new section (`onto_existing`; `crossing_pieces` already reads aliases) | not measured |
| A lake or pond on the line before the exit | the tidal stretch ends at the lake's lower edge (lakes are never cut) | not measured, expect ~0 |
| Line leaves and re-enters (3 lines) | cut at the first real exit; an outside run of 25 m or less is bridged (`rejoin_m`); the stretch higher up stays freshwater. See decision D3 | 356337576: exit 144 m, back inside up to 6.3 km |
| Mouth outside the polygon (Tlell 2, Chemainus 1) | no cut, listed `mouth outside the polygon` | 3 |
| Freshwater receiver (the 191 rivers) | not a candidate: not cut at all. See D1 | 371 cuts dropped |
| Creek flows into a Rule A line | candidate; cut at its own exit; tidal below. See D2 | not measured |
| Braid: a side channel joining on both sides of the cut | it is its own blue line with its own mouth inside the polygon, so Rule B cuts it at ITS exit. The braid sweep still runs on all shore cuts and must find nothing left; if it does, the build stops and names it (no quiet skip: ruling 1 forbids skipping). `A_SHORE_A_BRAID_SPANS_IS_NOT_CUT` is retired | T660: none of the 97 spanned shores joins tidal water (measured at the old cut point) |
| Polygon over land (Barnston Island) | lines there follow the polygon. Known and accepted with the 206 | — |
| Added (municipal) streams, negative ids | same rule; 9 of the 12 new cuts are these. Check they have a chain in the cutter pass | 9 |

**Names, ids, handles**
- Item ids: none changes, none disappears. Blue-line ids: same.
- Shore cut id stays `shore:<blk>:<whole metres>`. All ~169 get a new id because the point moves; no curated file
  names a `shore:` id (checked: none under `data/curated`).
- Section ids: the mouth piece keeps `<blk>:0`; the piece above gets a new id at the exit measure. On the 371 dropped
  cuts the two pieces become one again (the pre-build-27 id). Section ids never leave the bundle, so this is allowed.
- Rule A lines keep their sections and ids; only their tidal flag changes.

**Curated references and owner hand decisions** (measured on the live registry; nothing is deleted)
- 53 named items own a Rule A line. 52 keep other sections. **One becomes tidal whole: Brae Slough
  (`blk:356232808`), an owner-found name in `waters/found_names.json`.** It stays in the registry and search, answering
  as tidal water like the Fraser delta channels (D5).
- Files naming one of the 53 items: synopsis regions 1, 2, 5, 6 (15 matched entries), `dfo_salmon/places` regions 1,
  5b, 6 (11), `inseason/notice_waters.json` (1), `waters/continuations.json` (1), `steelhead_waters.json` (41),
  `review/proposal_overrides.json` (1). All keep resolving; each loses only the inside sections.
- **33 of the 177 creeks hold an owner `mouth` row** in `tidal_limits.json` (Nunns, Rogers, McCoy, Como …). The row
  says "nothing tidal"; after the cut the piece below the shore is tidal. The rows are not edited. The name
  `<creek>__tidal_boundary` should move from the old mouth to the shore cut (`tidal.alias_mouths`). No rule or cut names
  any of the 33 aliases (checked). See D4.
- No gauge, curated cut or tidal row sits on a Rule A line (T660 F).
- **Report, written by the dry run and again by the build: `T665-orphans.json`**: every curated reference (file, key,
  item) whose water is cut or made tidal, with sections and metres before and after, and a status: `unchanged`,
  `partly tidal`, `wholly tidal`. A `wholly tidal` row that is an owner hand match (DFO place, notice water, gauge, found
  name) stops the promote until the owner has seen it.

## 3. Downstream: what to re-check, and with what

| area | what can move | check |
|---|---|---|
| Rule bindings | 26 named Rule A lines carry a water rule by name (Bella Coola 9, Kitimat 5); zone rules lose the tidal metres; 371 mouths change section ids | `python -m pipeline.atlas.reach.binding_diff <before> <after> --json`. Per rule: metres lost = its tidal metres; no rule longer; `report.tidal_emptied` empty; unresolved count unchanged (25) |
| Tributary walks | must not move: the walk runs before tidal water is taken out (`TIDAL_PIECE_HAS_NO_TRIBUTARIES`) | same diff, read per rule, not atlas parity alone (a split change once moved a walk) |
| Unplaced / straddling pieces | a new cut can leave a piece unplaced | `build.straddle_gate`, `build.unplaced_gate` (both stop the reach run) |
| Tidal boundary rules | the 33 `split` limits and rules naming them: unchanged. 33 `mouth` names move | `test_tidal_cut.py`, `test_tidal_limits.py`; `atlas.promote` refuses a moved boundary a rule names |
| In-season notices | one notice water loses sections | re-resolve (`pipeline/regs/inseason/resolve.py`, `matches.py`); the release stopped on this once (d2dc3e5fc) |
| DFO salmon places | places lose tidal sections; none may empty | reach `dfo tidal` report; `reach/dfo_salmon.py` stops on a `drowned` place (L134). Then count the owner's DFO marks that read "rule changed" before and after (T361) |
| Gauges | a gauge on a now-tidal piece | `bundle/undrawn.py::tidal_gauges` (L65), `test_tidal_gauges.py`; `python -m pipeline.tools.audit.check_curated` (never regenerate matches) |
| Search / registry | Brae Slough wholly tidal; unnamed lines have no search entry | look it up in the built bundle; `registry_digest` moves, bundle refuses a mismatched run |
| Tiles | Rule A lines and tidal pieces not drawn; `tidal_water` layer unchanged (they are inside the polygon) | `bundle/undrawn.py::check` (L100) proves tiles = run; `vintage.report(strict=True)`; tiles and bundle from the same atlas |
| Review app Tidal tab | reads `tidal.resolved.json`, which does not change shape | `test_curation_review_tidal*.py` (no edits there) |
| App | no code change; fixtures re-cut after promote | `node app/tools/regs-recut/sync.mjs`, `scripts/fast-fail.sh` |

## 4. Verification

**Dry run first (before any build).** As T660 did: the new stage run in memory on a copy of the live graph, through
`scripts/heavy.sh`. It prints and writes: Rule A lines, cuts, each not-cut reason, braid sweep cases, the D2 count, and
`T665-orphans.json`. Rule A can also be proven end to end without a rebuild: sidecar on a copy of the live atlas, then
a reach-only run and binding diff.

**Counts to expect** (from T660; the dry run replaces them with exact numbers, which the tests then pin):
- Rule A: 206 lines / 66.4 km (103 named, 103 unnamed), plus the 5 creeks whose exit is their top if they are not
  already among the 206. The named 103 must match `T660-inside-named.json` line for line. The unnamed 103 were not
  saved as a list: compare the count and kilometres, and explain any difference (exact edge vs 10 m samples).
- Rule B: 177 creeks join curated tidal water = 3 mouth outside + 5 whole + **about 169 cut** (161 existing, moved; 12
  new; 1 formerly "under 50 %"), plus the D2 creeks. Shore cuts in the atlas: 532 → about 169.
- Against build 27: 101 cuts move up (+14.0 km tidal), 5 down (−0.2 km), 55 under 25 m.

**Owner diff report** (one page plus maps, before promote): lines made tidal by Rule A with the rules each loses; cuts
moved, added, dropped; the three re-entering lines; the orphans report; before/after maps in the T660 style for Skeena
estuary, Fraser delta, Bella Coola, Steveston, Brae Slough.

**Tests to add** (in `test_shore_cuts.py` and one built-atlas file; add to `smoke.txt`):
- Synthetic, one per table row in section 2: exact exit point; one point 6 m outside is not "wholly inside"; a 4 m dip is;
  exit 4 m from either end; exit 3 m from a standing boundary aliases it; re-entry beyond 25 m stays freshwater;
  freshwater receiver not cut; creek into a Rule A line.
- Mutation pins: two policy flags (`A_LINE_WHOLLY_INSIDE_THE_POLYGON_IS_TIDAL`,
  `A_CREEK_INTO_TIDAL_WATER_IS_CUT_AT_ITS_POLYGON_EXIT`); with either off, a test on the reach OUTPUT (`tidal` owners)
  must fail.
- Pinned against outputs, not the record (built atlas + run, `needs_*` marker, slow). These recompute from geometry
  and never read `shore_cuts.json` or `inside_tidal.json` for the expected answer:
  1. no stream section outside the tidal set lies wholly inside the polygon (10 m samples, the T660 E test);
  2. every `crossing:` section lies inside the polygon and the section above it starts outside;
  3. no freshwater section with its lower end inside the polygon has all its outlets tidal, except the listed
     `not_cut` rows;
  4. every `inside:` and `crossing:` section is absent from the tiles (already `undrawn.check`).

**Release gates this will trip (expected, not to be worked around):**
- `POLICY_VERSION` 36 → 37 with a changelog line (`reach/cache.py` L129).
- Binding diff stops the release until `--accept-diff` (owner reads the report first).
- `atlas.promote` will report a net boundary loss (371 shore cuts gone): `--accept-boundary-loss` after D1.
- Sliver gate, straddle gate, unplaced gate, DFO drowned: must all pass clean.
- Add the shore rule to `pipeline/docs/RULINGS.md`; update `pipeline/docs/MAP.md`, AGENTS rule 12/56 wording.

## 5. Open decisions (each with the default the build will use unless told otherwise)

- **D1. Drop the 371 shore cuts into freshwater rivers?** They make nothing tidal. Default: drop (fewer sections, no
  binding change). Keeping them means two cut rules in one stage.
- **D2. A creek that flows into a removed (Rule A) line: cut at its polygon exit too?** Default: yes, otherwise a blue
  stub runs from the polygon edge to where the removed line was (the original T660 complaint). The dry run gives the
  count and a map before anything is built.
- **D3. The 3 lines that leave the polygon and come back.** Default: cut at the first real exit (outside runs up to
  25 m bridged); the stretch higher up stays blue over the shading. The other reading, "highest exit", would make
  about 3.5 km of creek on land tidal on 356337576.
- **D4. The 33 creeks with an owner `mouth` row.** Default: rows untouched; the "tidal boundary" name moves to the
  shore cut; the list goes in the report.
- **D5. Brae Slough becomes tidal whole.** Default: keep its found name and search entry, answered as tidal water.
- **D6. The 3 creeks whose mouth is outside the polygon (Tlell 2, Chemainus 1).** Default: no change (they stay blue
  into removed water), listed in the report.

## Steps (one change set, for the next atlas rebuild with T664 and the nine DFO cuts)

1. `shore.py`: new rule on `clean_cut.decide_chain`; record rows with reasons; unit tests (S).
2. Dry run on the live graph via `heavy.sh`; write counts, `T665-orphans.json`, D2 count; reconcile with 206 / 177 (M).
3. Owner: D1–D6 with the dry-run numbers and maps.
4. `build.cut_shores`: wire the new rule, write `inside_tidal.json`; same writer in `sidecars.py`; `alias_mouths`
   moves the 33 names (S).
5. `reach/outside.py`: `inside:` owner, report row, diagnostics; `POLICY_VERSION`; mutation tests (S).
6. Rule A on the live atlas without a rebuild: sidecar, reach-only run, binding diff (M, heavy.sh).
7. Atlas rebuild beside the live one (`python -m pipeline build --atlas`), parity, reach run, binding diff, built-atlas
   tests, orphans report (L, heavy.sh).
8. Owner diff report and maps; `--accept-diff`; promote; in-season re-resolve; fixtures re-cut; RULINGS, MAP (M).

## Not verified here
- No code was run against geometry: the cutter's exact-edge result may differ from the 10 m samples by a few lines.
- The D2 count, the lake-on-the-line count and the alias-onto-a-boundary count are unknown until the dry run.
- That a braid sweep finds nothing at the new cut points (T660 measured it at the old ones).
- That search shows a wholly tidal item the way it shows the delta channels (read from the T212 path, not opened).
