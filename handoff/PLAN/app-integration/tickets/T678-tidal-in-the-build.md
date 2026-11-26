# T678 — Tidal in the build: one decision, made once, read everywhere

Status: IN PROGRESS (coast cut in the build + one tidal record: addbfd819, 26873ad75; built, promoted (a5fe40636) and released in r-0a9930fb8826 with T685; plan 6 answered by the owner 2026-11-12: no manual list, nothing to build; left (Claude): plan 2, plan 5 and the retirement of the polygon-rule readers (`shore.polygon_rule`, `HELD_*`, `atlas_tidal`, `inside_tidal.json`), each medium; the retirement's wait for the next promote is over (promotes 2026-11-13 onward); reasons in the deleted T678-progress.md (git history) 2026-11-12 next-atlas section)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T678 | Tidal in the build: one decision, made once, read everywhere (cut rule: DFO written boundary, then coast crossing, polygon for shading only) | IN PROGRESS | addbfd819, 26873ad75, a5fe40636, 8e8c3e79f | owner 2026-11-11; built, promoted and released (r-0a9930fb8826, with T685); plan 6 answered (no list); L7 2026-11-20: plan 2, plan 5 code and the legacy retirement landed (no … (detail: ticket file) |

Source: owner 2026-11-11: "We need to make sure all the tidal logic and stream removal happens in the build of the
graph etc if possible. Or where other tidal logic is we want to avoid it being a weird post processing step." And:
"Also the cuts used for tidal should happen where cuts happen etc."

## Owner answers and rulings (2026-11-11) - current, read this first

**Cut rule, final order of sources (the owner's edits do not outrank the default).** Proposed rank 2 was "your curated
split or polygon edit on that river"; owner, verbatim: "No to this unless the curated splits are the DFO ones. Like
all the splits added in region 1 and 2 especially." So the rule is:
1. a DFO written boundary (sign, bridge, written line), which includes curated split rows that encode one (most of
   the Region 1 and 2 splits);
2. otherwise coast crossing: where the line enters the sea or a tidal river (mouth-line prototype in the session
   scratchpad `t665/mouthline/`; numbers in its `tables.md`, FWA inputs it needs: line ends, edge type 1450 water
   crossings, coastline/water polygons);
3. the DFO subarea polygon for map shading only.
Polygon hand edits (122; 73 touch a line; the candidate honours 22) and non-DFO curated splits carry no authority over
the cut. Honouring them is no longer a build defect to fix. The 1,126 "at the mouth" rows in tidal_limits.json
(2026-10-28) were auto-generated, not owner decisions, and carry no authority either. The owner's own work is 33 split
rows, 10 whole-water rows and 122 polygon hand edits. The held lists (`shore.py:69-230`) are to be re-derived from
where the new method fails, not carried over.

**A - fresh stub at a line's top:** a fresh run under 25 m at the top counts as tidal. Ewen Slough (356355111) and
356242813 become wholly tidal. Use the same 25 m as the in-line gap bridge (replace the 5 m at `shore.py:645`).

**B - lines wholly in tidal water:** keep them in the graph and mark them tidal in the build. Do not delete.
Downstream readers take the flag; none works it out again.

**C - five held lines with a build-27 cut and nothing tidal below** (354087703, 354154029, 356341234, 360445710,
360861076): still open; goes on the held-rivers page (to be rebuilt around the lines the new method leaves open).

**The tidal Fraser.** Verbatim: "We still want all channels and stuff of Fraser below it's tidal boundary as tidal".
Every channel and arm of the Fraser below the Fraser's own tidal boundary is tidal, for its length (it is not cut at
its own mouth). Tributary creeks that join the tidal Fraser (e.g. McLennan Creek, Chester Creek) keep their own
boundary at their mouth unless the owner says otherwise. **Correction, same day:** "Sloughs aren't channels. Sloughs
could be really non tidal?" Sloughs are NOT covered by the rule; each slough (and each unclear unnamed line, e.g.
Katzie Slough off the Pitt) is decided one by one by the owner, with nothing pre-filled. Build rule, decided in the
atlas where tidal is marked; no post-processing list. Fraser lines below Mission (13 once suggested tidal): the owner
ticks each on the held-rivers page.

**San Juan River (owner ruling):** the tidal boundary is the San Juan Bridge, by DFO's wording, which the owner
quoted: "In that portion of Subarea 20-2 at the mouth of the San Juan River north of the beach camp boundary marker at
the large alder tree, located at 48°33.514' N and 124°24.272' W and south of the Pacheedaht Campground and a boundary
marker located at 48°33.599' N and 124°24.233' W, at the well-established stump in the sand, shoreward in a easterly
direction of this boundary and downstream of the San Juan Bridge". Source:
https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/tidal-maree/a-s20-eng.html. This is a rank-1 DFO written boundary. **ACCEPTED by the owner on 2026-11-11** ("No just accept that bridge please"): the curated split goes in as accepted, the owner's explicit decision with DFO's wording as the source. No review step.

**Kitimat:** reading still open. DFO's line Eurocan dock - MK Bay kiosk is only the seaward edge; where tidal water
ends upstream is not written anywhere found. Owner picks: keep polygon exit / boundary at DFO line / ask the Kitimat
DFO office.


## Owner 2026-11-12
San Juan: the main-stem bridge is at the river's mouth, so the boundary stays at the mouth; the side channel is tidal below the Deering Road bridge (681 m up it). The 10 curated splits not traced to DFO (Qualicum, Toquaht, Sooke, Chemainus, Campbell River Area 13, Nitinat, Yorkson, Chambers, Noons, Nass): "Keep all as my decisions"; they stay, recorded as owner boundaries (the owner explicitly accepting them overrides "non-DFO splits carry no authority" for these ten). Kitimat: automatic mouth line. Marble: "Mouth for marble river (signs near varney bay) could add it has signs to cut.. for tell say 1.5km upstream bridge option" - Marble at the mouth with a note that signs stand near Varney Bay; Tlell at the bridge 1.5 km upstream (the DFO option naming it; Claude's reading, told to the owner). The four held creek pieces (360586479, 360665787, 360675720, 360812872): the mouth line decides; shown in the T678 diff. Ucona's first 33 m under the Strathcona Park rule: fine. lake:329461947 folds into the Okanagan River Oxbows: yes. "Dewdney Slough" search name finds Nicomen Slough: yes, apply. Still open for the owner: Fraser sloughs and 7 unnamed lines; the 46 estuaries; the five held build-27 cuts.

## Done
- addbfd819, 26873ad75: the coast cut in the build + one tidal record (`tidal_sections.json`, owners `split:` / `water:` / `inside:` / `crossing:`); 9b0d6f4ae (candidate `full_t678b`): the ten splits `authority: owner` and govern, San Juan side channel tidal below the Deering Road bridge (a `cut` row), Kitimat by the mouth line, the four creek pieces at FWA's edge-1450 crossing, Tlell unchanged (the 1.5 km DFO split governs), Marble at the mouth (note in the drafts file). Promoted a5fe40636, released in r-0a9930fb8826 with T685.
- Split classification: settled by T685 (`tidal_split_authority.json` holds the authority, `coast_held` no longer generates, `coast_owner.json` `held_now_governed` is empty, no split unclassified).
- Plan 8 (store fact for key-0 tidal sections) = T672 1b, 83b1477ba.
- Plan 6 (record what the prune took): owner 2026-11-12, no manual list, nothing to build. Count on the live atlas: 1,204 FWA lines / 1,827 edges with no graph piece, none named; 1,133 inside tidal river polygons (edge type 1450: Fraser 385, Skeena 290, Nass 158 …), 71 in the sea; a list changes no output.
- 8e8c3e79f (lane L7, 2026-11-20): plan 2 (rank-1 resolvers into the atlas), plan 5 code (`tidal` on the graph node) and the retirement of the polygon-rule readers. Proof on the live atlas (no rebuild): the atlas's own resolver (`splits/tidal.tidal_cut_owner` / `tidal_whole_owner`) reproduces all 110 rank-1 rows of `tidal_sections.json` (36 cuts, 16 whole items), and `coast.tidal_owners` on top rebuilds the whole 283-row record identically; `stamp_nodes` stamps 283 nodes, `node_owners` reads back exactly the record, `read_tidal` on an `on_nodes` record equals it; reach `binding_diff` empty, `tidal.jsonl`, `outside_bc.jsonl` and both licensing files byte-identical.
- The 2026-11-11 map of every tidal decision point and its defects list (polygon rule, held lists in `shore.py`, throwaway registry) are superseded by the above; git has them.

## Left
1. **Plan 5's stamp runs in the wave-4 atlas build** (node stamp: the reach's binding subtraction and the tiles' skip read the node; owner B: keep and mark, do not delete); then the record-only read path retires.
2. **DFO tidal pages re-scrape (ready, not moved into the repo)**: in the session scratchpad `t665/rescrape/` (read `DIFF-TABLES.md`: old 2026-10-28 copy vs live 2026-11-11, per page and per block). Move `merged/` to `data/source/dfo_tidal_areas/2026-11-11/`; run `extract` then `propose`; fix the extractor so the 8 missed river texts and Nanaimo Cedar Bridge come through; check the Tidal review does not reopen waters already decided.
3. Open for the owner, one by one (held-rivers page, rebuilt around the lines the new method leaves open): C, the five held build-27 cuts; Fraser sloughs and 7 unnamed lines (nothing pre-filled); the 46 estuaries drawn as river where the mouth methods disagree.

Depends: T665, T677. Size: L.
