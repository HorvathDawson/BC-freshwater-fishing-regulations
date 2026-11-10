# T518 — An in-season notice shows on exactly the sections it covers, not its whole part

Status: DONE — ece44d1f3, 46ce5a22d, fd09e65af; carried by T520, whose owner look covers it (2026-11-09 audit)

Was: IN REVIEW — carried by T520 (ece44d1f3, 46ce5a22d, fd09e65af), in build 22 (3d807829d); owner: OWNER-TODO "Phone check with build 22" (a notice's "Where:" + map outline, Fraser part 2).
Earlier: IN REVIEW — carried by T520 (ece44d1f3, 46ce5a22d, fd09e65af): banner says "Where: <ends>", the water map outlines exactly the covered sections; shots/T520-*.png light + dark (Fraser part 2, Pitt part 0). Proof pending: the next APK.

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
