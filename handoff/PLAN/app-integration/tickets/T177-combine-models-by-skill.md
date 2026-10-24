# T177 — Combine CLEVER and COFFEE for every forecast day (horizons and Like-your-visit), weighted by measured skill

Status: OPEN

Source: owner 2026-10-24 (Q160): "should combine models for change. look into which models is best to figure out how to
combine." Today the +1/+3/+5/+10 horizons and T171's `aheadDays` take the FIRST model that answers a day
(`change.day_model`), while the Change already combines CLEVER and COFFEE (geometric mean of ratios, equal weights —
T144 found no published head-to-head verification).

## Work
1. MEASURE which model is better, from our own data: the feed keeps every model run it fetched (`forecasts.<MODEL>`,
   "925 model runs", runs carried forward) and the observed daily means (`daily`, ECCC 30-day archive). Score each model's
   forecasts against what then happened, per station × lead day (1–5, 10) × season where data allows: bias, error on
   the percentile scale, and hit rate on the low/normal/high band the app shows. Archive runs going forward if the history
   is too short (a small append-only store in the feed), and say how long until the score is trustworthy.
2. DECIDE the combination from the score: equal weights where neither is better; skill weights (e.g. inverse error) where
   one is; per station or per region/season only if the data supports it. Write the numbers and the rule into this
   ticket and decisions.md.
3. ONE function combines a day — used by the Change (T144), the horizons and `aheadDays` (T171) — replacing first-model
   for days both reach; COFFEE stays rescaled to the gauge before combining (T171); ELF stays a last resort with its
   low-water caveat, never beside a flood model.
4. Tests whole-value; a no-second-copy pin (one combine); measured feed diff (which stations' horizons move).
Ships: feed republish (local; R2 owner) + app words if the panel names the combination. Size: M (measurement first).
