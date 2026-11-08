# T519 — The Fraser's exit reads Morey Channel (its own side channel), not the sea

Status: OPEN — found 2026-11-08 by the T517 lane; `test_part_geo::test_the_fraser_reaches_the_sea_and_its_side_channels_flow_into_it` fails on the live bundle with or without T517.

## Found
- T59 fixed the Fraser "flowing into" Annacis Channel (its own side channel). Since T497 renamed unnamed connectors, the
  Fraser's `water_exit` is Morey Channel: same shape as T59 — a side channel that flows back into the river is taken
  as the river's exit.
- User-visible wherever "flows into" is shown for the Fraser (and maybe other mainstems whose connectors T497/T510 named).

## Do
- Exit pick must never choose a water that flows back into the item (the T59 rule), whatever its name; check every
  `water_exit` that changed since T497 on the live bundle.
- Fold into the T517 bundle rebuild; the pinned test passes again.
