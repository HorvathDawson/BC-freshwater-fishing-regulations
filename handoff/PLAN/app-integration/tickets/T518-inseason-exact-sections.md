# T518 — An in-season notice shows on exactly the sections it covers, not its whole part

Status: OPEN — after T517 (contiguous parts); owner 2026-11-08.

Owner 2026-11-08: "Shouldnt we make it only go on the parts it covers?" (5 notices end partway through a part, e.g. the
Fraser part 2 notice covers 25 of 28 sections; today the page shows the banner flagged "on part of this stretch").

## What
- `places` already carries per-section coverage at publish (`some` = part partly covered). Carry the covered sections'
  own ends (the notice's extent, `short_label`'s words) so the banner says where it applies ("From A to B"), and the
  water map highlights just those sections, not the whole part.
- Parts are NOT cut at in-season ends: notices are temporary, parts follow the regulations (T517).
- One load path: banner, KeepTab, water map and screen reader read the same coverage.
- Proof: the 5 partial notices on live staging (Fraser part 2, Pitt parts 0 and 2, Kaslo, Snehumption) show their own
  ends and highlight only their sections; a fully covered part shows no "part of" wording.

## Related
- T517 removes the old-release fallback (`inSeasonParts` mark-graph walk): owner 2026-11-08 "The back up paths for older
  releases can be removed in general too", since no other user has the app yet.
