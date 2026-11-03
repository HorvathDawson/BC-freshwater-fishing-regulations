# T326 — Lock tidal: `waters/tidal.json` is the one tidal truth

Status: PARTIAL (151b9f76a, dc93125ab: tidal.json, write-through, mouth aliases; release r-d0ad76a6e6a1). Left: the 8
follow-up marks + Mamquam (owner), and folding proposal_overrides.json into splits.json.

Replaces `tidal_waters.json`, the splits tidal flag, `decided/tidal.json` and handoff
`curation-review/tidal/owner-decisions.json` (4 records of one fact). Also fold `review/proposal_overrides.json` (split
confidence) into the split records. Blocked on: 7 open tidal marks + Mamquam (owner). Depends: T325. Size: S–M.

## Tidal ids (2026-11-02, assessed, no change made)
Of the 1,162 tidal decisions in force: 1,128 put the boundary at the mouth (no cut), 32 are existing splits.json splits
(they keep that split's id), and 2 have no usable point. The other 8 tidal marks are open follow-ups. **None creates a
new cut.**

The split-id function (d9b7c7c40) is deterministic from the anchor (`<water>__<label>`; a tidal cut becomes
`…__tidal_boundary` unless it has a named place). So a tidal cut that is applied later gets a stable id, and no id
needs to be stored.

Decided (owner wants it clean): do NOT stamp split ids onto decided/tidal.json. The clean end state is this ticket:
waters/tidal.json, one record per water, with the boundary given as `mouth` | `split: <existing id>` | `new cut`
(anchor). A new cut gets its id only when it is written to splits.json.

## Mouth-tidal decisions still get a named tidal boundary (owner 2026-11-02)
"The no-cut items: we still want 'tidal boundary' as a variant name at the mouth, saying it is tidal too, like when
splits get combined."

Plan: use the existing coincident mechanism (`sectionizer._coincident`: a cut at the same place as another boundary
becomes that boundary's ALIAS, as region/border/area cuts already do). For each of the 1,128 mouth decisions, emit a
tidal anchor at the water's mouth (`<water>__tidal_boundary`) that aliases onto the mouth boundary: the confluence
with the receiving water, or the sea end. No new section, no new cut. The mouth boundary gains the name "tidal
boundary", and the water reads as tidal there.

Guard: a cut at a line's END must never split the water (memory end-cut-halves-the-water). The 58 m sliver case had a
cut at the end that broke both halves, so a mouth anchor resolves to an alias or is refused, never a cut. Prove it with
an atlas parity diff: 0 new sections, +1,128 aliases. Rides the next atlas build.

## Design (2026-11-02, chosen before coding)
Three shapes were compared for the ONE tidal file and the "tidal boundary" variant on every item.

- **A (chosen): `waters/tidal.json`, one row per water keyed by its blue line, `limit` = `mouth` | `split` | `cut` |
  `whole`.** Read by ONE stage, the atlas (`pipeline.atlas.splits.tidal.load`, through `CURATED.waters.tidal`). The
  atlas writes the resolved answer as the sidecar `tidal.resolved.json`; reach, tiles and the review app read only
  that sidecar (`reach.outside.tidal_splits` / `tidal_waters(build)`).
  - `mouth` (1,128): no cut, ever. The atlas puts the alias `split:<water>__tidal_boundary` on the boundary standing at
    the line's lower end (`sectionizer._coincident`, the region/border mechanism); where the mouth is a natural end
    (no boundary object) the item gains a registry boundary `<item>__mouth` (kind `outlet`, ref `mouth:<blk>`) whose
    alias is the tidal id. 0 new sections.
  - `split` (32): names an existing splits.json cut; the atlas marks it tidal in the sidecar. The `tidal` flag leaves
    splits.json.
  - `cut`: the point (blk, measure, coord, label) stays in tidal.json; the atlas adds it as a curated cut whose id is
    the deterministic `split_base` + `assign_ids` (d9b7c7c40). Nothing is stored twice and no id is stored.
  - `whole` (10, from `tidal_waters.json`): the item is tidal along its whole length.
  - Guard: a tidal row never cuts a line end — a `cut` within `SLIVER_M` of the line's lower end is refused by the
    atlas (`tidal.refuse_end_cuts`), and a mouth row never becomes a SplitPoint.
  - Write-through: tidal becomes a FILE domain (`apply.WRITERS["tidal"]`, `pipeline.tools.tidal_limits`), written in
    the request like sloughs; rows without a mark (owner-authored `whole` rows) are kept unless a newer decision on the
    same water replaces them.
- **B (rejected): everything in splits.json** (a mouth as a zero-length split flagged tidal, whole waters as a flag).
  It puts 1,138 non-cuts into the cut file, and the cutter would have to learn to skip them: the exact
  end-cut-halves-the-water hazard, one bad flag away.
- **C (rejected): tidal.json for mouth/whole only, tidal flags stay on splits.json.** Two files still own one fact
  (which cut is tidal), which is the duplication this ticket removes.
- Also rejected: storing item ids or split ids for new cuts (items change between atlases; ids follow from anchors),
  and a graph bound on natural mouths (it would change every rule's tributary seeding and every span's `mouth` token).

Retired: `waters/tidal_waters.json` (folded in as `whole` rows), the `tidal` key of splits.json (moved to `split`
rows), handoff `owner-decisions.json` (all 3 calls are superseded by later marks; the export no longer reads it).
`decided/tidal.json` stays only as the export the tidal writer reads, like every other domain's; no build reads it.

## Result (2026-11-03)
Side build gate: sections 4,594,880 → 4,594,880 (handles 822db448 unchanged, so no old-vs-old noise), boundaries
+1,126 (one `<item>__mouth` per mouth row, alias `<water>__tidal_boundary`), 1,126 items changed, nothing else; rule
binding diff empty. The side build first also gave basin/region/park items a mouth boundary (+2,465); fixed in
dc93125ab and the side registry cleaned the same way before promote. Every mouth was a natural end (0 on a standing
boundary). Promoted; live build → r-d0ad76a6e6a1 (salmon/3, tiles and bundle same vintage).

## proposal_overrides fold (2026-11-03, chat 2)
Sandy Pool's call (medium) is already on its splits.json record (`cowichan_river__white_triangle_boundary_sign_loc`,
`confidence: medium`). The other three overrides (Quatse weir ±100 m, Stamp lagoon-outlet move, Little Qualicum
Falls park boundary) are Claude's PROPOSALS from the owner's notes, not yet accepted, so they cannot be folded into
splits.json. Left: owner accepts/rejects those three in the DFO tab; then fold and retire the override file.
