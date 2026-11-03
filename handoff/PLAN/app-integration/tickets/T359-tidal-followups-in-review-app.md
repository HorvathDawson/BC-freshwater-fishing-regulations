# T359 — Tidal follow-ups visible in the review app

Status: OPEN (owner 2026-11-03)

Owner 2026-11-03: "make it so the review app is able to show the tidal follow ups and I will review. I thought we
already confirmed they were good before."

The tidal writer (`pipeline.tools.tidal_limits`) leaves 8 tidal marks out of `waters/tidal.json`. The Tidal tab counts
any marked row as done, so these 8 never come back to the owner:

| Target | Last mark | Why the writer holds it |
|---|---|---|
| tidal:354151091 Caycuse River | Place y 10-29, DEM point 1,925 m above mouth | `tidal_point` knows mouth / line_end / words / place only; a DEM point is none of them |
| tidal:360848786 Dempster Creek | Place y 10-29, DEM point 550 m | same |
| tidal:355994265, 355994324, 355994870, 355992068 | unsure 10-29, note "this entire channel is tidal" | unsure decides nothing; the tab has no "whole water" answer |
| tidal:356570000 | unsure 10-29, "this is us border it crosses" | unsure |
| tidal:360868849 (Bear River) | wrong 10-28, "most likely the split is at the bridge" | wrong decides nothing |

## Work
1. Backend: the Tidal queue gets a `followup` filter and a `followup_why` per row, from the writer's own `skipped`
   list (one source of the reason; no second classifier).
2. Frontend: a "Follow-ups (n)" filter in the Tidal tab; each row shows its why and the owner's last note.
3. A "whole water is tidal" answer on a tidal mark, which the writer turns into a `whole` row.
4. A Place y on a point the owner chose (the DEM point) — owner decision: does a confirmed DEM point become a `cut`?
   Until answered, the row stays a follow-up with that question as its why.

Nothing is written to tidal.json by Claude; the owner's re-marks write through.
