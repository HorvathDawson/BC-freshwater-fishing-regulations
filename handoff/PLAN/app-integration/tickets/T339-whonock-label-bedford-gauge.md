# T339 — 08MH044 cut still labelled "Whonock"; Bedford Channel lost its gauge

Status: DONE (live with the next bundle + release) (e21cdbeac) 2026-11-04 (was: OPEN (loose end turned into a ticket, 2026-10-31).)

After the 0.4.0 rebuild the 08MH044 cut moved Whonnock → Albion (HYDAT name "Fraser River at Albion") but the cut
label still reads "Fraser River At Whonock". Bedford Channel lost its "fair" gauge 08KA007 (Fraser at Red Pass, ~900 km
upstream — probably correct to lose). Rename the cut from HYDAT; confirm Bedford Channel's no-gauge is right. Size: S.

## Done (2026-11-04)
`gauge.name` now comes from HYDAT's STATIONS (bundle_tables `hydat_station_names`; a station HYDAT lacks stops the build):
the BC stations layer lagged two renames — 08MH044 WHONOCK → ALBION, 08HB083 5TH AVENUE → 5TH STREET (all 2,324 others
agree). Bedford Channel losing 08KA007 is right: Fraser at Red Pass drains 1,700 km² some 900 km upstream.
