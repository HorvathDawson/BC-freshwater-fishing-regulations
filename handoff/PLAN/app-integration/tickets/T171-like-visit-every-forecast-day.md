# T171 — "Like your visit?": every forecast day uses the flow forecast (no weather-only gaps between forecast days)

Status: OPEN

Source: owner 2026-10-23: "in the how similar is a spot to when the spot was added section sometimes it has gaps and
says weather only despite gauge forecast on both sides". T51's known gap (its follow-up "+2/+4 flow"): the strip's
coming days read the feed's percentile only at the fixed HORIZONS (+1, +3, +5, +10 — `reading(row, "discharge", h)` in
`flowAhead`, app/packages/data/src/spots/likeVisit.ts), so +2 and +4 are scored on weather alone even though the models
forecast them.

## What the feed already has (measured 2026-10-23, data/generated/gauges/feeds/<station>.json)
`forecasts.<MODEL>.series` (CLEVER hourly for 10 days; COFFEE 5 days; ELF 30 days) and `change.days` (per-day ratios);
only the RANKED percentile is cut to the four horizons (index `ahead`).

## Work
1. Feed (where the data lives): publish a discharge PERCENTILE for every forecast day (daily mean of the series, ranked
   against the same climatology/day-of-year as `ahead`), choosing the model per day by the same one rule T143/T144 use
   (each quantity's first model that forecasts it; ELF only where no flood model reaches, carrying its low-water
   caveat). In the station file (or a compact `aheadDays` in the index if the size allows — measure).
2. App: `flowAhead` reads that per-day series; a day is "weather only" ONLY when no model reaches it (said so); the
   strip's "flow" factor names the model when it is ELF.
3. Tests whole-value (Chilliwack: +2 and +4 now carry flow; a station with only ELF gets the caveat; a day past every
   model stays weather-only); feed test pins one per-day percentile against the horizon value on the same day.
Ships: feed republish (local now; R2 owner) + app. Size: S–M.
