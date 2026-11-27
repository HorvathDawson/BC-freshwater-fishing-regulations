# T510 — channels that change water (owner review list, a PROPOSAL — nothing promoted)

**Measured on the side build 2026-11-08** (code c4ce4b8b4 + T517; atlas `data/generated/side/t517/atlas/full_next`,
reach run `data/generated/side/t517/reaches/full_next`), against the live atlas `data/generated/atlas/full` and reach
run `data/generated/reaches/full` (r-6dce77a24e6a). Section handles identical in count (1,958,281).

**Rule bindings, every section** (`binding_diff` merge-walk, all rows): **6 sections** differ —
the 2 connectors with their own rules (below), 2 slough-fold lakes, and one Columbia section whose id moved
(`356570372:1834527` → `356570372:1835154`: the same rules, a cut 627 m up the line where the Baldy lake now folds into
the channel). The 11 name-only connectors keep exactly their rules. **No other rule binding moved anywhere.**

**Water (item) changes: 17 sections** — the 13 predicted connector sections, the 2 lakes, and the re-cut Columbia section.

## The rules change (owner's attention: two LOOSEN night and steelhead closures)

| section | old → new water | rules − / + |
|---|---|---|
| 355991900:0 (1,543 m) | Fraser River → **Strawberry Slough** `gnis:16210` | − Fraser `fraser_river_upstream_of_mission` r1 (dead-fin-fish bait for sturgeon only), **r3 night closure (1 h after sunset to 1 h before sunrise) — LIFTED**, **r5 steelhead closed Aug 15–Dec 31 — LIFTED**, r6 (commercial vessels), bait.r3 · + strawberry_slough.r1 (8 km/h) |
| 355993053:0 (1,211 m) | Pitt River → **Widgeon Slough** `gnis:27696` | − pitt_river.r2 (10 km/h at Grant Narrows), **pitt_river.r4 night closure (Lower Pitt) — LIFTED**, bait.r3 · + widgeon_slough.r1 (5 km/h) |

## Slough-fold lakes (the guard the ticket asked for)

| section | old → new | rules |
|---|---|---|
| lake:329220703 (2.4 ha, on Baldy connector 356364756) | no water (an unnamed lake) → **Baldy Channel** `gnis:7958` | − the lake rules (bait.r5, barbless r2) · + the Columbia rules the channel carries (columbia_river r3–r5, r10, r12–r14) and the zone's stream rules (single barbless hook, spring stream closure, winter release, bait.r4) |
| lake:329292341 (0.9 ha, on Williamson connector 355991970) | no water → **Williamson Slough** `gnis:8341` | becomes **tidal water** with the slough (its channels already are): loses every freshwater rule (zone 2 quotas, provincial) — tidal water carries none (T166) |

## Name only, same rules (9 channels, 11 sections)

| section | old → new water |
|---|---|
| 355991968:0, 355991970:0, 355991970:594 | Fraser River → Williamson Slough `gnis:8341` (tidal) |
| 355994264:0 | Fraser River → Annacis Channel `gnis:10494` (tidal Fraser) |
| 356364756:0, :337, 356364766:0, 356364767:0 | Columbia River → Baldy Channel `gnis:7958` |
| 356365647:0 | Columbia River → Back Channel `gnis:17696` |
| 356366354:0, 356366359:0 | Columbia River → Hotsprings Channel `gnis:8203` |

Prune: no pruned blk revived (handle count identical; no new section ids besides the one re-cut). Strahler: no main
river changed water.

---

## The proposal as written before the build (kept for the record)
# T510 — channels that would change water (owner review list, a PROPOSAL — nothing moved)

Measured 2026-11-08 on the FWA gpkg (stream ends) and the live atlas `data/generated/atlas/full` + reach run
`data/generated/reaches/full` (the build behind r-6dce77a24e6a). No side build was run: this is the list the rule below
would move, from the same end-walk `names.follow_connections` does. "Rules − / +" compares the channel's rules today with
the rules every existing section of its new water carries.

**Proposed rule (option A in the ticket):** an unnamed channel whose one end reaches ONLY the main river and whose other
end reaches ONLY one other named water of the same watershed code (a "connector") takes that other water. Today T497
leaves every channel that reaches the main river with the main river.

## Rules change (2 channels)

| blk / section | length | old item → new item | rules − / + |
|---|---|---|---|
| 355991900:0 | 1,543 m | Fraser River → **Strawberry Slough** `gnis:16210` | − Fraser `fraser_river_upstream_of_mission` r1 (dead-fin-fish bait for sturgeon only), **r3 (closed 1 h after sunset to 1 h before sunrise)**, **r5 (steelhead closed Aug 15–Dec 31)**, r6 (do not impede commercial vessels), bait.r3 (sturgeon bait, Fraser in R2) · + strawberry_slough.r1 (8 km/h speed) |
| 355993053:0 | 1,211 m | Pitt River → **Widgeon Slough** `gnis:27696` | − pitt_river.r2 (10 km/h at Grant Narrows), **pitt_river.r4 (night closure, Lower Pitt)**, bait.r3 · + widgeon_slough.r1 (5 km/h speed) |

## Name only, same rules (9 channels, 11 sections)

| blk / section | length | old item → new item |
|---|---|---|
| 355991968:0 | 1,964 m | Fraser River → Williamson Slough `gnis:8341` |
| 355991970:0, :594 | 1,134 m | Fraser River → Williamson Slough |
| 355994264:0 | 1,581 m | Fraser River → Annacis Channel `gnis:10494` (tidal Fraser) |
| 356364756:0, :337 | 1,174 m | Columbia River → Baldy Channel `gnis:7958` |
| 356364766:0 | 3,180 m | Columbia River → Baldy Channel |
| 356364767:0 | 2,825 m | Columbia River → Baldy Channel |
| 356365647:0 | 907 m | Columbia River → Back Channel `gnis:17696` |
| 356366354:0 | 5,374 m | Columbia River → Hotsprings Channel `gnis:8203` |
| 356366359:0 | 1,151 m | Columbia River → Hotsprings Channel |

Total 11 channels, 13 sections, 22.0 km. Province-wide only 10 watershed codes hold two named waters and an unnamed piece
(the lower Fraser, Pitt, Columbia), so this is the whole province.

## Left alone by the proposed rule
- A channel with one end on BOTH the main river and the slough (they meet at the same point): 15 more (Williamson 2, Greyell Slough 1,
  Annacis 1, Widgeon 1, Baldy 3, Hotsprings 7 — 7 of them pruned). Its ends do not say which water it is.
- Connectors into a curated water (`name_variants`, e.g. Jeperson Side Channel blk 355994564, 355992188): curation decides.
- 13 candidate blks have no section today (pruned as short braids). A rename can make the prune keep them — T497 saw 20
  loops survive until `prune._has_own_name` was fixed — so the side build must check that no new sections appear.

Method limits: FWA lines only (the municipal added streams are not in it), chains not broken at lakes, direct end contact
walked through unnamed same-code pieces as `follow_connections` does. The side build is the real count.
