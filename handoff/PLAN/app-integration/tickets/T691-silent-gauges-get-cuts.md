# T691 — A silent gauge station ECCC still calls Active gets its cut (Q113)

Status: IN PROGRESS — 1935c1dc9; left (Claude): rides the final atlas build (32 new gauge cuts); read the build's sliver
gate and gauge placement lines for the 32

Source: owner 2026-11-12, answering Q113 (T122), verbatim: "silent doesn't mean retired so we should add cuts Incase they
start transmitting again?". RULINGS P53. The coordinator's reading (silent but not retired = cut; retired/discontinued =
none) is the rule built.

## The 38, by the roster's own fields
All 38 are `active: true` (ECCC STATUS Active) and `realtime: false` in `data/source/bc_hydrometric_stations.json`: silent,
none retired. 1,858 roster stations are not active (retired); they stay uncut.

## Built (1935c1dc9)
`gauges.consume.cuts.split_defs` asks `liveness.may_speak` (was `speaks_now`). Cut set 391 → 423: **32 new cuts**. No
matches.json row changes (all 32 rows already exist; 25 auto, 7 reviewed). Tests: `test_gauge_splits.py`
(`test_a_silent_station_is_cut_like_a_transmitting_one`, mutation-checked: back to `speaks_now` fails it),
`test_gauge_matches_record.py` follows `may_speak`.

New cuts: 08CG001 Iskut below Johnson · 08DA012 Kelly Ck · 08EG012 Exchamsiks · 08HDX05 Quinsam auxiliary (same coordinate
as 08HD005: joins that cut as an alias) · 08JA015 Laventie · 08JB013 North Beach Ck (matched to "Beach Creek") · 08KB006
Muller · 08KE018 Fraser at South Fort George · 08KH030 Penfold · 08LG028 Nicola at Douglas Lake outlet · 08MC049 Borland ·
08MF035/038/072/073/074/075/076 lower Fraser (Agassiz, Cannor, Laidlaw, Harrison Mills, Herrling Island, Lower Kent, Hunter
Ck; 3.1-12 km apart) · 08MH152 Bertrand at the border · 08NB016 Split · 08NE008 Beaton · 08NH016 Duck · 08NH115 Sullivan ·
08NH126 Duncan below the dam · 08NH138 Humphries · 08NM240 · 08NM241 · 08NN028 Lost Horse · 08NP003 Howell · 08OA003
Premier · 08PA004 Skagit at the border · 10CD003 Raspberry.

No cut (and why): 08ND017 Kinbasket Lake (lake gauge, never cut) · 08FE002 Kemano Powerhouse, 08ND025 Revelstoke Project
Outflow (no match row) · 08JA013 Skins Lake Spillway, 08NE126 Arrow Reservoir Outflow, 08NJ158 Kootenay Lake Outflow (match
rows marked `trust: wrong`, no water). These five need a human match call (OWNER-TODO).

## Meanwhile and effects
- A deliver on an atlas built before 1935c1dc9 (the live `full`, the current `full_next`) STOPS at the gauge record check
  ("did not cut"): the cut set changed. That is the guard working; the final atlas build clears it.
- Size: at most +32 sections (~0.06 % of ~49.5 k; 08HDX05 merges, so fewer); no gate measured until the build.
- Watch in the build: 08PA004 sits 1,015 m from its matched line (cut tolerance 400 m) — the resolver will most likely make
  no cut there; 08LG028 (lake outlet), 08NH126 (below a dam), 08MH152/08PA004 (border) sit near other boundaries and should
  join them (`sectionizer` merge), not sliver.
- In the app meanwhile: the 38 are not on ECCC's real-time list, so the feed never asks them and has no reading to show as
  current; a silent gauge shows its muted dot and says so in words (T586 F10, T581). Not re-checked on a device.
