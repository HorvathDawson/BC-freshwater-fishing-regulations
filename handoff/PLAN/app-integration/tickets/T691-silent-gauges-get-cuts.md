# T691 — A silent gauge station ECCC still calls Active gets its cut (Q113)

Status: DONE (1935c1dc9 phase 1; 1965900be + 8a1ce30f1 phase 2; released r-f749475d0962 2026-11-13)
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
- Final atlas build 2026-11-12 (`full_next`, not promoted): 30 new gauge split ids; 26 new cut places; 4 joined a boundary
  already there (08HDX05 = 08HD005's place; 08MH152 the border cut; 08LG028 Douglas Lake's outlet edge; 08NH126 the Duncan
  Dam end); 08PA004 (1,015 m) and 08JB013 (520 m) made no cut, over the 400 m `PROXIMITY_M`. Long-river length cuts
  re-spaced around the new cuts (Fraser x4, Nechako, Muller, Iskut: 6 ids gone, 1 new; no curated rule names any). Sliver
  gate clean. Every binding change on the cut lines is the same metres under new section ids.

## Phase 2 proposals — NOT applied (2026-11-13, pipeline lane)
`~/bcfish-session-backup-2026-11-12/gauges/t691_proposed_rows.json` (Claude, 2026-11-13) says "NOT accepted", and no owner
approval is recorded here, in OWNER-TODO (the five-station row is still open) or in the ledger, so **no row was applied**
and `data/curated/gauges/matches.json` is untouched. Two of the rows also need code that does not exist yet: a `cut_at`
field (where the CUT goes, the published station point unchanged) and `reads: "below"` (the station speaks for the
stretch below its cut). Pictures beside the file (`08*.png`). For the owner, in plain words:

| station | what the row would change |
|---|---|
| 08PA004 Skagit at the border | Keeps its match; adds a cut place at the Ross Lake inlet (a boundary already there), so the gauge finally gets its cut (today none: 1,015 m from the line). Needs `cut_at`. |
| 08JB013 North Beach Ck above Allin Ck | Moves the match from Beach Creek (520 m away) to Beach Creek's unnamed north fork (24 m away), 805 m above the fork's mouth. NOTE: HYDAT 2026-10 lists it Discontinued — under P53 a discontinued station gets no cut, so this may be "leave uncut" instead. |
| 08ND025 Revelstoke Project Outflow | New match: the Columbia, cut on Revelstoke Dam (joins the dam boundary); reads the Columbia from the dam down to the Hwy 1 bridge. Needs `cut_at` + `reads`. |
| 08NE126 Arrow Reservoir Outflow | Replaces the "wrong" match: the Columbia, cut on Keenleyside Dam; reads the dam down to the old Robson ferry landing. Needs `cut_at` + `reads`. |
| 08NJ158 Kootenay Lake Outflow (Corra Linn) | Replaces the "wrong" match: the Kootenay River, cut at Kootenay Lake's edge at Corra Linn Dam; reads Corra Linn down to Brilliant Dam. Needs `cut_at` + `reads`. |
| 08JA013 Skins Lake Spillway | Replaces the "wrong" match: the unnamed 636 m spillway channel (Ootsa → Skins Lake), cut at its foot (Skins Lake edge); reads the whole channel. Needs `cut_at`. |
| 08FE002 Kemano Powerhouse | Stays unmatched (the powerhouse water is only part of the river below the tailrace; any cut hands it the wrong water). |

Each accepted row also needs its `match_calls.json` decision; applying is surgical (one row each, `promote --only`), then
an atlas rebuild. Owner: OWNER-TODO "T691: five silent gauge stations need a match call".

## Phase 2 — OWNER ANSWERED 2026-11-13 (by dialog, 15:11 UTC; main failed to record it, found again in the transcript)
The "no owner approval is recorded" note above was wrong: the owner answered every one the morning of 2026-11-13.
Verbatim:
- 08JB013 North Beach Creek above Allin Creek: "rematch no cut and add name variant to the fork". So: match moves to
  Beach Creek's unnamed north fork (24 m), NO cut (discontinued, P53), and the fork gets "North Beach Creek" as a name
  variant. (Asked again by mistake the same evening: "Fix match, no cut", same answer.)
- 08PA004 Skagit: "Ross Lake level gauge". So: it is a level gauge of Ross Lake (its point is inside the BC arm of the
  lake, level only): match it to Ross Lake as a lake gauge; lake gauges are never cut. This REPLACES the proposed row
  (river match + `cut_at` the Ross Lake inlet).
- The five dam/outflow gauges: "Accept all five proposals": 08ND025 Revelstoke → Columbia from Revelstoke Dam down
  8.1 km; 08NE126 Arrow → Columbia from Keenleyside Dam down 7.4 km; 08NJ158 Kootenay Lake outflow → Kootenay River,
  Corra Linn Dam to Brilliant Dam (22.9 km); 08JA013 Skins Lake Spillway → the 636 m channel Ootsa → Skins Lake;
  08FE002 Kemano Powerhouse → stays unmatched.
Status: DECIDED, not yet applied. Needs `cut_at` + `reads: "below"` code, each row's `match_calls.json` decision,
surgical rows in `data/curated/gauges/matches.json`, then an atlas rebuild. Do not ask the owner again.

## Phase 2 — applied (2026-11-13, pipeline lane): 1965900be
Code: `cut_at`, `reads: "below"`, `cut: false` (StationMatch / Decision / promote / split_defs / nodes_at_cuts / shed
walk). Rows (surgical, the 7 stations only, built by `promote.build`): 08JB013 -> the north fork, cut: false, fork named
"North Beach Creek" (name_variants, source gauge); 08PA004 -> Ross Lake lake gauge; 08ND025 / 08NE126 / 08NJ158 cut at
the dam, reads below; 08JA013 cut at the channel foot; 08FE002 verdict none. Candidate atlas `full_next` + side run
`data/generated/side/t691` (checks/ATTRIBUTION.md): all 4 new cuts join standing boundaries; placements exactly as
proposed. Promoted, patch applied (8a1ce30f1) and released: r-f749475d0962 (LATEST, staging).
