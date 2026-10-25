# T183 — A piece that straddles a region line carries both regions' rules (reverses T115)

Status: OPEN. Owner 2026-10-24 (braid review, then confirmed when asked about the conflict with T115): "Those straddle
pieces are supposed to get both regions regulations" → "Both regions (reverse T115)".

## Work
- `pipeline/registry/regions.py` `region_sections` (T115's one home-filtered membership) becomes raw membership: a
  section is in every region whose polygon it lies in (by length > SLIVER_M, so a touch is not membership). Keep ONE
  function (T115's unification stays; only its answer changes), its grep pin, and the steelhead twin's
  `area_family_sections`.
- The page: a water whose piece is in two regions shows both regions' rows (check the regs page groups them clearly;
  zone base + named overrides per region — base + deltas invariant).
- Binding diff vs live: every change must be a region-wide rule landing on a straddling piece (expect T115's 12 Fraser
  pairs + 3 steelhead-twin pairs + 1 licensing pair back, plus others). The card gate stays at 0 (two regions' quotas on
  one piece must still produce one decided answer per fish — if they conflict, report it, don't pick).
- RULINGS row superseding T115's; AGENTS rule updated.

Depends: after the T67 braid merge (both move reach bindings; one attribution per rebuild). Size: M (reach + deliver).

## Outcome (2026-10-24) — code done; NOT promotable yet: the card gate is 62 (8 keys)

Commits: 43ae9276 (code + tests), 7afa9f16 (RULINGS Z4/Z14 + superseded row, AGENTS 59), 140c024d (Z1 attribution).

**The change.** `registry.regions.region_members` (the ONE membership; T115's grep pin kept) returns the raw
membership: `STRADDLERS_IN_EVERY_REGION = True` (False = T115's home filter, the mutation pin). The zone rules, the
per-region row limit, the steelhead twin's `area_id`/`outside_area`, `area_family_sections("region")`,
`section_regions` (so T140's lift regions and the bundle's `region_place`) all follow. A touch is not membership:
`regions.holds(area, inside_m, otherwise_m)` — the atlas's membership pass (`border.mark_inside_areas`) now needs
> `SLIVER_M` (5 m) of a line inside a REGION polygon (other areas keep their 1 m). That gate is ATLAS-side: it takes
effect at the next atlas rebuild. On today's atlas 3 members are slivers and still carry both regions (below).
`region_home.json` / `section_home` / the export's `home_region` are display-only now. POLICY_VERSION 31 → 32.

**Side reach run** (`data/generated/scratch/t183/reach`, digest 9911e0d430f625a1; 3,379 bound / 26 unresolved as
live) — `binding_diff` vs `reaches/full`: 193 rules, 0 unresolved changes, 2 licensing records. Every change is on
one of the 58 straddling STREAM pieces (all 58 gain; no lake changes — lakes were already in both):
1,386 rule–section pairs gained, 3 lost, 0 rescoped.

| region pair | pairs gained | pieces |
|---|---|---|
| 1+5 | 17 | 1 |
| 2+8 | 163 | 7 |
| 3+5 | 149 | 7 |
| 3+8 | 20 | 1 |
| 4+8 | 42 | 2 |
| 5+6 | 135 | 6 |
| 5+7A | 300 | 14 |
| 6+7A | 492 | 18 |
| 6+7B | 68 | 2 |

By shape: 1,293 = the non-home region's zone (`z<r>:`) rules; 20 = per-region rows of the non-home region
(below); 73 = provincial rules whose extent is a list of regions (`zp:steelhead` r1/r2/r4 ×21 — its regions
1,2,3,5,6 — and `zp:spear_fishing` r2/r3 ×10). Nothing of another shape.
- T115's Fraser pairs back: 418405 (home 3) `r5:fraser_river@5-2` r2/r3/r4 **+ r1, r6** (both `within_area:
  area:region:5`, which read the home filter even before T115 — so 14, not 12); 434930 (home 5) `r3` r1/r4/r12;
  674465 (home 5) `r7:fraser_river@7-9` r1/r2; 698232 (T115's 699253 after the T67 atlas; home 7A) `r5` r2–r5.
- Other rows: `r6:west_road_blackwater_river_s_tributaries@6-1` r1–r3 on 356354998:10 (5+6, home 5);
  `r7:liard_river_watershed…@7-53` r1–r3 on 359573055:796354 (6+7B, home 6).
- Steelhead: the provincial BASE (r1/r2/r4) now binds 21 straddlers it did not (presence `possible`, or known);
  on 698232 the base replaces T115's lost twins, and on West Road 356364550:15264 the base replaces its twins —
  the 3 LOST pairs are `zp:steelhead` r1b/r2b/r4b there, and licensing `steelhead_targeting_known` −1 /
  `steelhead_targeting` +21 is the same swap. Same rules, base instead of twin.
- Slivers (≤ 5 m inside the second region; leave at the next atlas rebuild by `regions.holds`):
  359554864:0 Contact Creek 2.8 m in Region 6 (31 pairs), 360256685:0 4.5 m in Region 5 (17), 356554283:1098
  4.8 m in Region 8 (21). **Owner check:** RULINGS E9's own example is "Contact Creek's 619 m also takes Region
  6" — that is this 2.8 m sliver; the SLIVER_M gate will take Region 6 away from it at the next atlas build.

**Side deliver** (`data/generated/scratch/t183/stage`, steps run one by one so the outputs survive):
**card gate 62 problems on 8 keys** (`deliver all` refuses to package). One answer per fish IS decided (the
engine keeps a number for every fish); the card's GROUP cannot say it: the card builds the trout/char group on
ONE region's total (the lower entry id wins "same as peer"), and the other region's stricter sub-cap, which
belongs to the hidden identical total, is left out of the group — so the headline says 5 where 2 may be kept.
| key | water (pieces) | regions | the two tables' rules | engine | card says |
|---|---|---|---|---|---|
| 1052 | Fraser River 356364114:418405, :434930 | 3+5 | z3 "Trout/char: 5", "4 from streams" · z5 "Trout/char: 5", "2 from streams" | 2 | 5 |
| 759 | Spanish Creek (5) | 3+5 | same | 2 | 5 |
| 210 | Big Bend Creek (6) | 6+7A | z6 "Trout/char: 5", "3 Dolly Varden/bull trout and/or lake trout", "1 trout from streams July 1-Oct 31" · z7a "Trout/char: 5", "2 from streams" | 2 (Jan) / 3 (Jul, Oct) | 4 / 5 |
| 408 | Nechako River (12) | 6+7A | same | 2–3 | 4–5 |
| 539 | Contact Creek (2, incl. the 2.8 m sliver) | 6+7B | z6 as above · z7b "Trout/char: 5", "2 from streams" | 2–3 | 4–5 |
| 736 | West Road River 356364550:209816, :234412 | 5+6 | z5 "2 from streams" · z6 "1 trout from streams July 1-Oct 31" | 1 | 2 (and a worked count naming brown trout) |
| 750 | Similkameen River 356570170:199385 | 2+8 | z2 "1 over 50 cm (2 hatchery steelhead over 50 cm allowed)" · z8 "1 over 50 cm" | 1 over 50 cm, steelhead counted | Region 2's "2 if one is a hatchery steelhead" |
| 969 | Copper Creek, Bonnevier Creek (6) | 2+8 | same | same | same |
Not picked. Proposed fix (deliver, a ticket of its own): when a total loses `same_as_peer` to the other region's
identical total, its sub-caps join the winner's group (they cap the same fish). The 2+8 steelhead allowance is the
one that reads like a real disagreement — Region 2 lets a second hatchery steelhead over 50 cm be kept, Region 8 does
not; stricter-wins (Z4) says 1 — owner may want to confirm.

**Base + deltas (Z1)** over EVERY one of the 218 straddlers (not a sample), 4 days × every fish: 16,788
differences, all attributed after 140c024d (before it 29 were "unexplained": a water row lifting one region's
spring closure lets that region's identical quota speak and the other's show as same-as-peer — the row's doing;
`base_deltas.attribute` now follows a displacer the override revived). No carve-out over the whole bundle.
`test_rulings_pinned` passes on the side bundle.

**The page.** Rows are per fish, each the decided line, scoped to the table whose line won (on the Fraser 3+5
piece every row reads Region 3's; on Copper Creek rows come from both Region 2 and Region 8). The other region's
identical line is in the row's sources as "Same as …, the other region's identical line" and every rule's
`applies` names its own table ("From the Region 5 table — all of Region 5"). There is no per-region grouping on the
page; with the card fix above, the trout/char row would still read one table's badge with the other's cap inside.

**Tests.** Unfiltered: reach / regions / answers / registry / rulings suites 1,412 pass on the live data; the 2
`test_region_homes` bundle tests updated for T183 fail against the live bundle and pass on the side bundle; 1
pre-existing (`test_answers_year_strip_live` 128 vs 130 — the branch head re-pinned it to 128 in 2ac14284).

**Promote** (after the card gate is 0 — not before; `pipeline build` would write the reach run before deliver
refuses): `scripts/heavy.sh build 40 -- .venv/bin/python -m pipeline build` (reach + deliver; no atlas, no tiles).
The scratch runs: `data/generated/scratch/t183/{reach,stage}` in the T183 worktree.

### Separate: Skeena River Ecological Reserve side channel and Skeena River 2 (asked after T67)
`360216962` (4,386 m; pieces :0, :96, :1752, :3522, :3840, cut by the reserve boundary) leaves the mainstem from
`360887278:66554` (the mainstem between 66,554 m and 67,917 m) at its top (:3840 receives at 3,849–4,386 m) and is
also fed by `360216952:0` (the Exchamsiks host channel BELOW its Exchamsiks cut at 1,017 m); every piece drains back
into `360887278:44465` at 63,193.8 m / 63,430.1 m. The Exchamsiks cut is at 67,917 m on the mainstem (T67 braid cut
`skeena_river__exchamsiks_river_into_skeena_river`). So the whole channel lies BELOW the Exchamsiks confluence. The
book's Skeena River 2 is "from Exchamsiks River to 1.5 km upstream of Kitsumkalum River" (p.53): **losing section 2
on :96, :1752, :3840 is correct per the book.** What is still wrong is the other way: the curated extent starts at
the reserve's lower mainstem crossing (63,480 m, LS-1's "over-requires, the safe direction" because the Exchamsiks cut
was only on a side channel then), so 360887278:63480 and :66554, 360216952:0 and 360216962:3522 still carry Skeena
River 2 though they are below the Exchamsiks. Now that T67 put the Exchamsiks cut on the mainstem at 67,917 m, the
extent can start there — a catalogue proposal for the owner (no tool writes the catalogue).
