# T519 — The Fraser's exit reads Morey Channel (its own side channel), not the sea

Status: IN REVIEW — 935ed7b37, 254befbeb, d3bb57acb, a804a5d78, in r-d14bdb180f55 (52298df0a); owner: OWNER-TODO "Phone check with build 22" (the Fraser's exit reads the sea, not Morey Channel).
Earlier: OPEN — fixed in the T517 side build (topology `channel_of_o`: a smaller stream on the main river's own watershed code is its channel, never its exit); 3 exits change (`T517` Results); owner review with T517 before promote.

## Found
- T59 fixed the Fraser "flowing into" Annacis Channel (its own side channel). Since T497 renamed unnamed connectors, the
  Fraser's `water_exit` is Morey Channel: same shape as T59 — a side channel that flows back into the river is taken
  as the river's exit.
- User-visible wherever "flows into" is shown for the Fraser (and maybe other mainstems whose connectors T497/T510 named).

## Do
- Exit pick must never choose a water that flows back into the item (the T59 rule), whatever its name; check every
  `water_exit` that changed since T497 on the live bundle.
- Fold into the T517 bundle rebuild; the pinned test passes again.
