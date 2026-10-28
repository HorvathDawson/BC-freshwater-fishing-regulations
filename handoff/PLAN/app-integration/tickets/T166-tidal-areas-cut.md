# T166 — Tidal areas cut: turn the DFO tidal polygon into clean cuts across each stream

Status: PARTIAL — step 5's build path is in (Phase 1, 2026-10-28); the owner is marking; apply + atlas rebuild next.

Source: owner 2026-10-23: "another ticket to add is doing a tidal areas cut. this will be a little bit tricky because our
dfo tidal layer was cut mostly to the right spot but has a lot of areas where it cuts away only some of the stream
(doesnt follow it very well) and we need to fix the blobs to work around that. this is a good after cleanup task".
Related: T98 (the tidal RULES spec — SUPERSEDED 2026-10-28, folded in here as step 5), T78 (tidal gauges).

## The problem
The DFO tidal boundary (data/source/bc_fisheries_data.gpkg layer `tidal_boundary`, one province-wide polygon; read by
pipeline/regs/dfo_salmon/untangle.py and added_streams/build_dataset.py) is mostly in the right place, but where it
crosses a river it often takes only PART of the channel: the boundary wanders across a stream or river polygon instead
of crossing it cleanly, leaving slivers ("blobs") of the same reach on both sides. Clipping sections by it directly would
split a river lengthwise and give one bank tidal rules and the other freshwater rules.

## Owner refinement (2026-10-23)
"maybe for that ticket we could find every named stream it is near and then where it crosses? we can assume unnamed
streams tidal boundary is at mouth maybe but named streams we can find real boundary we could also look in the salmon
regulation for more context on all named streams in there to make sure we captured all boundaries that imply tidal
location." So the census is driven by NAMED streams:
- **Named streams:** every named stream (GNIS) the tidal polygon touches or comes near (a buffer, e.g. 2 km of its
  boundary) gets its own crossing found and cut (steps 1–3 below).
- **Unnamed streams:** the tidal limit is assumed to be at the MOUTH — no crossing work; the whole unnamed stream is
  freshwater down to where it meets tidal water. (Listed as a default the owner can revisit, Q151.)
- **DFO salmon regulations as a second source:** the DFO tidal-boundary notices and every DFO salmon entry
  (data/curated/regulations/entries/dfo_salmon/, the dossier/splitwork tools of T164) are read for locators that imply a
  tidal limit ("tidal boundary", "tidal waters", "from the mouth to …", "below the … bridge (tidal)"), and each named
  stream they mention is checked against the polygon's crossing: agreement confirms the cut; disagreement is listed for
  the owner; a stream the DFO text names but the polygon misses is added to the census.

## Work
1. Census: every stream line and river/lake polygon the tidal boundary crosses; classify each crossing — clean (crosses
   the channel once, near-perpendicular), partial (cuts only part of a channel or polygon), wandering (runs along a
   channel), braid (several channels).
2. One rule turns each crossing into a CUT ACROSS THE CHANNEL: the tidal limit on that blue line = one route measure
   (e.g. the upstream-most point where the polygon covers the whole channel width, or where the boundary meets the main
   flow line), the same cut applied to every braid channel and the river polygon at that measure; blobs on the wrong
   side are reassigned to the side of their reach. Never a lengthwise split.
3. Output curated data, not geometry guesses at build time: one tidal cut point per crossing in the splits store (kind
   `tidal`, with the evidence and a "Verify: proposed (tidal)" stamp so each lands in the review tool's S0 queue for the
   owner), plus a `tidal` flag on the sections below it. A map artifact (like the slough page) to check the proposals,
   showing the DFO polygon, the proposed cut and the reassigned blobs.
4. Atlas rebuild with the cuts → binding diff (expect only section splits at the cuts) → tiles in the same vintage.
5. **Remove the tidal piece (owner 2026-10-28; T98 folded in):** every section below an accepted tidal cut is REMOVED
   from the freshwater map — no freshwater rules, licences or parts there (the Fraser below the Mission CPR bridge
   included; RULINGS K7 applies, K8 retires, C-2 closes). Order matters (tributary-impact.md): expand tributaries on the
   full network FIRST, then drop the tidal pieces — never walk the freshwater-only graph (else 29 rules lose tributary
   trees: Skeena r1 130 lower-estuary tribs, Nass r1 252, Bella Coola 394 …). A stream entering a tidal piece stays
   its river's tributary. Binding diff must show only (a) tidal-piece sections dropped, (b) straddling sections split;
   flag kitimat_river.r1 (its whole extent lies below the proposed Kitimat cut — review that candidate first).
   What the page/tap shows on tidal water (K7's note, tidal gauges T78) rides with this step.

## Acceptance
Every NAMED stream near the tidal boundary has a reviewed cut (or a stated reason it has none); every DFO locator
that implies a tidal limit is matched to a cut or listed; unnamed streams default to the mouth; every crossing classified; no river split lengthwise; every proposed cut reviewed (S0); a test pins a sample of crossings
(Fraser at Mission, Squamish, Skeena, Cowichan) to one cut each.

Depends: cleanup band done; T212 accepted marks. Size: M–L (atlas rebuild). Owner: confirm the proposed cuts.

## Outcome — Phase 1: marks -> curated tidal splits -> the tidal piece out of every binding (2026-10-28)

The path, end to end (no curated data changed yet; the owner is still marking in the Tidal tab):

1. **Marks -> proposals** (`pipeline/regs/review/proposals.py`, T185's path): a tidal mark in force with
   Place y and its one cut is a decision. Above the mouth -> one `new_split` flagged `"tidal": true`, labelled
   as its water's cuts are ("tidal boundary"; a T212 "mouth (tidal boundary)" 500 m up is no mouth), the
   candidate's confidence unless the mark set one, shared with any other mark naming the same cut. On an
   existing split (reuse: Tlell sign, Fraser CPR bridge) -> `tidal_flag` (edit `split_flag`, the cut stays). At
   the mouth (<= 1 m: Cowichan, Copper Creek) -> `tidal_mouth`, listed, no edit, nothing tidal. A wrong / unsure
   tidal mark -> `tidal_followup`, listed, never applied (Bear River: "look more online"). The owner's earlier calls
   (`tidal/owner-decisions.json`: Capilano, Cowichan) are marks too; a later mark wins.
2. **The flag through the atlas**: `SplitDef.tidal` -> `SplitPoint.tidal` (anchors, braid sweep) ->
   `splits.resolved.json` rows carry `tidal` (`RESOLVED_KEYS`); the sidecar migration carries it for a reused cut
   flagged after a build (no new cut, no rebuild for a flag alone).
3. **The tidal piece** (`reach/outside.py`: `tidal_splits`, `tidal_cut_owner`, `tidal_water`): `downstream_of` the
   tidal split on every item carrying it, the same resolver a rule's extent uses; no tributary
   (`TIDAL_PIECE_HAS_NO_TRIBUTARIES`). Owner `split:<id>` in `tidal.jsonl` -> the bundle's `tidal` table -> status
   TIDAL (grey) and the K7 note on tap, no rules, no licensing (the Nitinat path; `tables.py` accepts a cut owner).
   A tidal split no item carries, or one the resolver cannot place, stops the run. Pieces it cannot place stay
   freshwater and are listed (`report.tidal_cuts[].unplaced`).
4. **Walk first, then drop**: the subtraction is `build._without_tidal`, after the walk on the full graph — a creek
   entering tidal water stays its river's tributary (pinned + a mutation). A row marked tidal keeps only its own water.
5. **Guard**: a rule left with no water is unresolved `tidal`, listed in `report.json` `tidal_emptied` and printed
   `!!` by the reach CLI. BEFORE applying: `python -m pipeline.tools.audit.tidal_preview` lists the rules every
   section of which lies below a marked cut (kitimat_river.r1 shape, pinned) from the live run, no graph.
6. Reach stage key holds the tidal split ids (`pipeline/build.py`); `side tidal` writes ONE side copy with every
   tidal proposal (`python -m pipeline.regs.review.proposals side tidal --out <side>`); `apply_proposals
   --side-checked <id>` lands each after the side atlas + binding diff.

Tests: `test_tidal_cut.py` (12), `test_review_tidal_proposals.py` (8). Preview on 13 marked cuts (2026-10-28):
0 rules lose all water; 14 river rules lose or straddle one section.

Left for Phase 2 / later: the review app's `reuse._tidal` still reads the rows only (its backend reloads on edit
while the owner marks — change it after); the Proposals tab shows the new kinds without words; RULINGS K7/K8/C-2
text once built.

### Phase 1b — off the map (owner 2026-10-28, decisions.md)
The tidal pieces are NOT drawn: the tile export skips every section of the reach run's own set
(`tiles/tidal.py` `tidal_pieces` = `tidal_cut_owner` on the atlas), lists them in `undrawn.json` (`tidal`), and the
bundle proves tiles and reach agree (`bundle.undrawn.tidal_problems`; gauges on a removed piece are printed,
`tidal_gauges`). The new `tidal_water` tile layer = the DFO `tidal_boundary` polygon + each removed piece's footprint
the polygon misses; the app draws it (subtle teal, under the water) and a tap opens "Tidal water — DFO rules"
(253850b9: `tidalSheet`, `TidalSheet`, `pressTidal`; tap order obstacle → water → tidal). Tiles key holds the tidal
split ids. Tests: `test_tidal_cut.py` (+3), tile inventory / feature-id / stage-key tests.
