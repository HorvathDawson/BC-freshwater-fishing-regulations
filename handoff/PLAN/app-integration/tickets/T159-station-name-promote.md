# T159 — Gauge auto-promote checks a station's name against every name the matcher accepted

Status: DONE (ce1cf83c, 06b83665) — 7 promoted by the owner; 08MH079's right match proposed below (a note, no promote)

Source: T125 item 8 "gauges 9"; Q118 decided by Claude 2026-10-23 (decisions.md owner-question triage).

## Work
promote.evidence checks a station's name only against the match's display name, while the matcher accepts any of the
node's names (Arrow Reservoir vs Lower Arrow Lake), so such stations never auto-promote. Add a StationMatch field for the
name the matcher used, regenerate match.json, and list the stations that would promote; the OWNER runs promote with
--only per station and --accept-demotions review (memory gauge-regenerate-drifts). Size: S.

## Done (Claude, 2026-10-28)
- `StationMatch.matched_name` (pipeline/gauges/matches.py): the normalised name the matcher matched on, set only
  when it is not the display name. match.py records it; `promote.evidence` accepts either name for `name_agrees`
  (`name_is_exact` stays about the display name); a promoted/candidate row carries `matched_name` only when set, so
  untouched rows keep their bytes. Tests: test_gauge_promote.py (2 new, mutation tried).
- Raw match regenerated against atlas/full (read-only proposal, gitignored):
  `data/generated/gauges/match.t159.json` — 2014/2324 matched, 23 rows matched on a non-display name.
- Would newly auto-promote (curated candidate `name_agrees` -> fresh `auto`; `--only` over these 8 demotes nothing):

| station | ECCC name | atlas name | matched on | note |
|---|---|---|---|---|
| 08GB008 | PAQ LAKE NEAR MADEIRA PARK | Lily Lake | paq lake | 228 m |
| 08HA050 | STOWELL LAKE ON SALTSPRING ISLAND | Lake Stowell | stowell lake | 209 m |
| 08HA051 | WESTON LAKE ON SALTSPRING ISLAND | Lake Weston | weston lake | 107 m |
| 08HA057 | MAXWELL LAKE ON SALTSPRING ISLAND | Lake Maxwell | maxwell lake | 136 m |
| 08JA027 | NECHAKO RESERVOIR AT TAHTSA REACH | Tahtsa Reach | nechako reservoir | 246 m; wsc 100-567134-865081-421069 -> 100-567134-865081 (same wbk) |
| 08MH079 | LONZO CREEK (NORTH FORK) NEAR KILGARD | Marshall Creek | lonzo creek | 442 m |
| 08MH080 | LONZO CREEK NEAR ABBOTSFORD | Marshall Creek | lonzo creek | 4 m |
| 08MH100 | WHONOCK LAKE NEAR WEBSTER CORNERS | Whonnock Lake | whonock lake | 86 m |

The other 15 matched_name rows are already `reviewed` (Arrow/Revelstoke/Duncan/Nechako reservoirs etc.) and stay so.

## Owner: run (Claude does not run promote)
```
python -m pipeline.gauges.generate.promote --matches data/generated/gauges/match.t159.json --only 08GB008 08HA050 08HA051 08HA057 08JA027 08MH079 08MH080 08MH100 --dry-run
python -m pipeline.gauges.generate.promote --matches data/generated/gauges/match.t159.json --only 08GB008 08HA050 08HA051 08HA057 08JA027 08MH079 08MH080 08MH100
```
Drop any station you disagree with from `--only` (08JA027's wsc moves one level; 08MH079 sits 442 m off). `--only`
refuses on a demotion; none expected, so `--accept-demotions` is not needed (it is a bare flag — only add it after
reading the dry-run's demotion list). Then commit data/curated/gauges/{matches,candidates}.json; the bundle picks
the rows up on the next deliver.

## Owner ran it (2026-10-28, 06b83665)
Promoted 7: 08GB008 08HA050 08HA051 08HA057 08JA027 (accepted, the wsc moving one level) 08MH080 08MH100. Skipped
08MH079 — owner: "there is a stream it is near that it is matching, not Lonzo itself".

## 08MH079: the stream it sits on (proposal only, not promoted)
"LONZO CREEK (NORTH FORK) NEAR KILGARD", -122.195557, 49.057781, ECCC drainage 2.77 km². The matcher took the nearest
line carrying the name — the Lonzo/Marshall mainstem (blk 356317779, wsc 100-064535-223850, "Marshall Creek"), 442 m
away, the same line as 08MH080 (4 m). The station's own line is FWA-unnamed, so a name pass could never pick it:

| blk | FWA name | wsc | order / magnitude | from station |
|---|---|---|---|---|
| **356314888** | unnamed | 100-064535-223850-103684 | 2 / 3 | **69 m** |
| 356305212 | unnamed | …-223850-103684-237584 | 1 / 1 | 245 m |
| 356103415 | unnamed | 100-064535-223850-063075 | 1 / 1 | 415 m |
| 356317779 | Marshall Creek (matched) | 100-064535-223850 | mainstem | 442 m |

Proposed: **blk 356314888 = Kilgard Creek** — Abbotsford's stream layer (`data/source/municipal/abbotsford_streams.geojson`)
names this line "KILGARD CREEK" (2.1 km overlap, 75 m from the station); its wsc is a Marshall tributary (the "north
fork" of the Lonzo system); the station is 507 m up its 747 m segment; magnitude 3 ≈ 2.4 km² vs ECCC's 2.77 km².
Atlas node `356314888:0` is in the registry.

To apply (only on the owner's yes): promote has no "force a line" flag; the designed route is a `bind` verdict in
`data/curated/gauges/review.json`:
```json
"08MH079": {"verdict": "bind", "station_name": "LONZO CREEK (NORTH FORK) NEAR KILGARD", "matched": "Marshall Creek",
            "wsc": ["100-064535-223850-103684"], "reviewed": "2026-10-28",
            "note": "on Kilgard Creek (Abbotsford layer), FWA-unnamed blk 356314888, a Marshall tributary; 69 m; mag 3 ~ 2.77 km2"}
```
then `scripts/heavy.sh` → `python -m pipeline.gauges.generate.match` and `promote --only 08MH079` (row: trust bound,
resolved_by review; its `name` stays the ECCC name, not "Kilgard Creek").
