# T326 — Lock tidal: `waters/tidal.json` is the one tidal truth

Status: OPEN (owner 2026-10-30; T324 design approved, plan in handoff/PLAN/data-flow.md §4–5).

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
