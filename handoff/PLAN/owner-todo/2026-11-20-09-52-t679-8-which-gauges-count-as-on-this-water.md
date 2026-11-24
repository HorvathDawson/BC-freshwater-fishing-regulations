order: 300

## 2026-11-20 09:52: T679 #8 — which gauges count as "on this water" (lane L8; ANSWERED 09:59, see above)

Two parts of the app decide whether a gauge is on the water you are reading, by different rules: the gauge list's
"ON THIS WATER" heading (the app: the gauge's water is this water or one it continues as, `water_continue`) and the
lead-gauge ranking (the pipeline: the same, but walked along the water, so a lake on the way ends it). T679 #8 moves the
decision into the data, once. Before building it, the one rule to keep:
(a) DEFAULT: the ranking's (along the water, a lake between ends it) — a gauge on the far side of a lake would move from
"ON THIS WATER" to "ESTIMATING FROM OTHER WATERS"; (b) the heading's (same water, lakes or not) — the ranking would start
treating such a gauge as the water's own. Not measured yet: how many gauges move under either (needs the atlas graph;
a side run measures it before anything ships). Measured: over 1,587 panels the heading's rule differs between a panel's
sections in 12, so the flag is shipped per section (like the small-water warning), not per gauge.
No picture yet: the list of gauges that move comes with the measurement.
