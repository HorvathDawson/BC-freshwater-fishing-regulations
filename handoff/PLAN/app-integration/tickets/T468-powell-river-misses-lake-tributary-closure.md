# T468 — Powell River above Powell Lake misses the lake's "incl. tributaries" closure

Status: OPEN — fix built 2026-11-07 (14294b999), not promoted; reach rerun in data/generated/scratch/t468. Owner: promote with the next release.

Owner, 2026-11-07: "POWELL LAKE 2-12 No Fishing Nov 1-Mar 31 … powell lake is closed including tributaries in
november but powell river (above the lake) is not showing as closed". In the screenshot the inlet river NE of the
lake draws base rules; every other inflow draws closed.

## Cause
- Entry `r2:powell_lake@2-12`, rule `powell_lake.r1` (No Fishing Nov 1–Mar 31), `includes_tributaries: true`,
  `matched: [wbk:329566910]`. Live run: 1,189 sections (lake + 1,188 tributaries). None of the Powell River item
  `gnis:22753` (no row of its own) is in it.
- FWA draws the Powell River through the lake on ONE blue line, blk 360879873: `360879873:0` is the lower river
  (lake outlet → sea), `360879873:53452` the river entering the lake's head. The lake's through line
  (`tributaries._through_blks`) is therefore 360879873, and `may_enter` refuses the inlet `360879873:53452` as
  `river_continuing` (probe on the live graph: every other one of the lake's ~90 inflows → None, this one →
  `river_continuing`). That refusal is the Koocanusa rule: a lake must not swallow the river threading it.
- That rule is right when the river has rows of its own (Kootenay, Columbia), wrong when nothing else in the tables
  speaks for it. Not the Strahler guard, not a split alias, not a shadowing row, not the registry name drop.

## Fix (built)
`tributaries.A_LAKE_ROWS_UNROWED_THROUGH_RIVER_IS_A_TRIBUTARY` + `through_inlets()`; applied in
`reach.build._expander` (`_through_river_with_the_lake`). For a lake that is the ENTRY's own water, the inflow refused
as the river continuing goes with the lake, it and everything above it (streams only, minus carve-outs and own
water), unless it has a row of its own (`own_rows`, the 2026-09-30 confluence ruling's test) or the rule's words
exclude it. A lake at the top of a river reach is unchanged. Each case is reported as a `through_river`
diagnostic. POLICY_VERSION 34 → 35. Test `pipeline/tests/test_lake_through_river.py` (fails on the old code; mutation pin).

## Scope (other lakes hit the same way)
Scan of the live run: 19 (rule, lake) pairs in 9 entries where a walking rule's lake has a through-line inflow
outside the reach. Of these, 6 entries are lake rows whose inlet has no row → they change (preview with
`build_reach` on the live graph):

| entry | inlet | sections added |
|---|---|---|
| r2:powell_lake (r1) | Powell River | +587 |
| r2:goat_lake (r1) | Eldred River (above Goat Lake) | +337 |
| r4:duck_lake (r1–r5) | unnamed | +80 each |
| r1:anderson_lake (r4) | Dove Creek | +5 |
| r2:twin_lakes (r1) | unnamed ×2 | +2 |
| r4:new_lake (r1) | Angus Creek | +1 |

Unchanged: Hatzic (the inlet is the row's own water), Atnarko/Tenas Lake (a lake at the reach's top, not the row's
own), Peace/Halfway River (the Halfway has its own row). Full-run binding diff: see "Rerun" below.

## Rerun (done 2026-11-07, not promoted)
Reach-only rerun in `data/generated/scratch/t468/reaches/full`; bundle `data/generated/scratch/t468/bundle.sqlite`.
`binding_diff` vs live: **15 rules, all gains, 0 lost, 0 unresolved changes, 0 licensing moved**
(`data/generated/scratch/t468/binding_diff.txt`). The 6 lake rows above, plus 3 "X Lake's tributaries" rows the scan
missed (their live binding does not hold the lake):
- `r4:little_slocan_lake_s_tributaries` r1–r3: +374 each, Little Slocan River above the lakes. **OWNER CHECK:** this row
  EXEMPTS its waters from the Slocan River's closure, so here the gain OPENS 374 sections (from the Slocan's
  incl.-tributaries closure to a June 15–Oct 31 bait ban). By p.80 the river feeding the lake is its tributary, but
  this is the opening direction.
- `r2:wahleach_jones_lake_s_tributaries` r1 (No Fishing): +20, Wahleach Creek above the lake.
- `r4:premier_lake_s_tributaries` r1 (No Fishing): +10, unnamed inlet.

12 lakes have a through-river with a row of its own and stay out, unchanged: Buttle, Campbell and John Hart (Campbell R.),
Chehalis, Connor (Forsyth Cr.), Duncan, Lake Revelstoke (Kinbasket), Lower Arrow (Upper Arrow), Tanya (Takia), and others.

Related: Koocanusa/Kinbasket (`_through_blks`), receiving-river-not-a-tributary, confluence ruling 2026-09-30. Size: S.
