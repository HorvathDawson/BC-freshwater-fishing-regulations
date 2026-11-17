# T825 — 251 side-channel sections are led by a gauge far upstream (lower Fraser side channels read Red Pass)

Status: IN REVIEW (list built 2026-11-17 on lane/t813-t825; owner: OWNER-TODO "T825 side-channel gauges"; no gauge data changed).

Source: the T823 worker's distribution over the live bundle: 251 led sections are 10× or more BIGGER than their lead
gauge, almost all side channels. Main case: lower Fraser side channels led by 08KA007 (Fraser at Red Pass), 750 to
1,300 km upstream; others on the Homathko, Similkameen, Kootenay and Bridge. Cause (worker's reading): a side channel
carries a small FWA line magnitude (e.g. 3), which passes the network's drainage floor, so the lead is picked by the
line's magnitude, not the river's. Effect: a spot or card there reads a gauge on another stretch of the river.
To do: lead a side channel by its main channel's panel / the river it belongs to (the panel area T823 already uses),
never by its own line magnitude; list the sections whose lead changes for the owner before it ships (gauge
representativeness is policy).

## The list (2026-11-17, lane/t813-t825; nothing changed in gauge data)
- Measured on the live bundle (r-46e4355a7277): 245 sections whose `section_gauge` station's modelled drainage is
  under a tenth of the section's own (`section_panel.area_km2`, the area the map colours with). The ticket's 251 was
  an earlier measurement. 9 rivers: Homathko 84, Fraser 81 (08KA007 Red Pass), Similkameen 37, Kootenay 25, Bridge 19,
  Zeballos 7, Guichon 2, Sumas 1 (not a side channel). 241 are FWA side-channel lines (adopted by
  `panels.mainstem_candidates`), 3 are river polygons (Fraser 1, Similkameen 2), 1 a main line (lower Sumas).
- PROPOSED RULE (for the owner): a side channel, slough or wide pool reads the nearest gauge on its own main river that
  measures flow and drains about as much country (a tenth to ten times); if the river has none, it reads none.
  Placement: each section projected onto its main river's line (route metres from the mouth), gauges on the same
  blue line, active, with discharge in `gauge_stats` ("Flow leads"), nearest by along-river distance.
- Result: 244 get a nearer gauge (e.g. lower Fraser sloughs -> Agassiz / Mission / Port Mann; Homathko -> at the
  Mouth; Kootenay -> Fort Steele, 3 -> Corra Linn of which 2 across Kootenay Lake), 1 gets none (lower Sumas).
- Owner page: scratchpad t813-t825/T825-side-channel-gauges.html; data t825_final.json beside it (scripts t825*.py).
- Next, after his answer: implement in the shed (a side channel sized by its main channel, not its line magnitude),
  rebuild the bundle, diff `section_gauge` against this list, promote nothing in `gauges/matches.json` (not touched).
