# T89 — Rules that bind no water: 10 unmatched synopsis rows + 3 rules with no extent

Status: PARTIAL — d9ce1d20 + review fix a799a3a5 (lane wf_4b4f5207-c7a-4). The 3 no-extent rules + the Endako (R7) row are fixed in the
catalogue and pinned; ships with the next reach rerun + deliver (no atlas). LEFT: 8 rows (22 rules) whose printed MU
holds no registry water of their name — Mackenzie Creek 4-21, Frog/Hidden/Secret/Square Lake 5-6, Redfern Lake 5-15,
Squirrel Lake 6-1 — need the water located and named in the registry (owner Q-A below), then an atlas registry
rebuild + reach + deliver.

Source: memory `unplaced-extent-rules`; `data/generated/reaches/full/report.json` lists 33 unresolved rules. 5 are
expected (`*.r5b`/`r6b`/`r9b` steelhead quota siblings); 2 are working exclusions (Columbia, Kootenay carry no trib rule).
The rest render nowhere:
- 10 entries `matched: []` ("no scoped item is in the registry"): `r4:mackenzie_creek@4-21` "No Fishing";
  `r7:endako_river@7-12` "No Fishing Nov 15-June 30"; R5 Frog, Hidden, Secret, Square Lakes (@5-6) and Redfern (@5-15);
  `r6:squirrel_lake@6-1` + its r5 twin; `r8:hidden_lake`, `r8:square_lake`, `r7:square_lake`. Most have same-name
  registry items (ambiguous name).
- 3 with no extent: `nanaimo_river.r5`, `nilkitkwa_lake.r4`, `fulton_river.r3`.

## Work
Match each row to the item in that row's region and MU; author the 3 extents or mark them `undrawn_part`; reach rerun
only (no atlas rebuild); deliver.

## Acceptance
`rule_unresolved.jsonl` holds only the by-design rows; a needs_bundle test pins the count with an allow-list (a new
rule binding nowhere fails); Mackenzie Creek and Endako (R7) show the closure in the app.

Depends: ride the next reach rerun (T56 or later). Size: M.
Owner: only where several candidates fit. Default: the candidate in the row's MU; if more than one, ask.

## Done (2026-10-22, lane wf_4b4f5207-c7a-4)
Measured against the live atlas (`data/generated/atlas/full`) and bundle; curated edits dry-run first, region files
backed up, diff = exactly the four rows below (`write_entryfile`, whole-file validated).
- `nanaimo_river.r5` "Artificial fly only upstream of the westernmost of the two Nanaimo Lakes ("Second" Lake),
  including tributaries" → `upstream_of nanaimo_river__nanaimo_lakes_2`. The river's two "Nanaimo Lakes" boundaries are
  wbk:329101304 (lon −124.160) and wbk:329101307 (lon −124.216, FWA variant "SECOND NANAIMO LAKE"): the western one.
  Probe (`build_reach` on the live graph via scripts/heavy.sh): bound, 4 mainstem + 566 tributary sections; the
  ambiguous lake cut takes the inlet (upstream_of → highest, as Pitt/Silverhope). Streams entering Second Lake directly
  are not walked (seeded above the lake).
- `nilkitkwa_lake.r4` "Rainbow Alley … open all year" → `between [babine_river__babine_lake,
  babine_river__nilkitkwa_lake]` on `item_id gnis:17687` (Babine River, variant "Rainbow Alley") — the stretch
  `babine_river.r3` binds. Advisory, so it decides nothing; it shows the row's words on Rainbow Alley.
  **Review fix (a799a3a5):** the row's SCOPE was the lake alone, so the run's entry clip (`entry_scope` → `_clip`)
  emptied r4 (`empty_after_scope` — the first probe passed no clip). The row's `extents` now name the lake AND the
  Rainbow Alley stretch (as Upper Arrow Lake (drawdown area) names the Columbia). Probe with the run's covered + clip:
  r4 binds the 3 Babine mainstem sections (360886970:108816/110644/110732 — the mainstem part of `babine_river.r3`'s
  7, the other 4 being its tributary walk); r1–r3 still bind only `lake:329026685`. Pinned corpus-wide: no rule
  extent names a water outside its row's scope.
- `fulton_river.r3` "Angling permitted in Fulton River opposite the spawning channels, as signed" → `undrawn_part`
  "opposite the spawning channels, as signed" beside `[whole]`, `includes_tributaries: false` (a place on the river,
  AGENTS 13). A note on the Fulton's 10 sections, never a colour; it lifts nothing (which closure it would lift is not
  printed).
- `r7:endako_river@7-12` → a `See` TWIN of `r6:endako_river@6-4` (as the seven MU 6-1 lakes under Region 5). FWA puts
  all 20 Endako sections in Region 6 (none in 7A/7B), and matching the 7A copy would make the river `shared`, hold the
  copy to 7A and leave it binding nothing again. The closure already shows on every Endako section through the Region 6
  row (probe: r6 binds all 20). `WINDOWS_HELD_ELSEWHERE` lists its "Nov 15-Jun 30" (held by the twin).
- The 3 entries the ticket listed from an older run — `r8:hidden_lake`, `r8:square_lake`, `r7:square_lake` — are
  already matched (wbk:329663960, wbk:328985756, wbk:329102648) and bind; `r5:squirrel_lake@6-1` is already a twin
  pointer.

Tests: `pipeline/tests/test_t89_rules_that_bind_no_water.py` (17): the allow-list (7 by design + 22 awaiting a water =
29) with mutation pins both ways (a new unresolved rule fails; an allowed rule that binds makes the list stale); the
four rows' place fields compared whole; the Nanaimo western-lake, Rainbow Alley cuts, Endako-in-Region-6 and
"no water of that name in the printed MU" facts measured on the atlas (`needs_atlas`); the bundle's unresolved set ==
the allow-list (`needs_bundle` — FAILS on the current bundle until the reach rerun + deliver below). Pins updated:
`test_see_pointers.py` (76 pointers, 54 pointer rows, 8 twins, Endako 7A = twin), `test_rule_dates.py`
(WINDOWS_HELD_ELSEWHERE).

Rebuild this needs (merger, one batch): reach rerun on the live atlas (no atlas rebuild, no tiles), then
`python -m pipeline.deliver` (bundle → status index → export) and verdicts. Diff the reach run `--against` the old one:
expected per-rule changes only `nanaimo_river.r5` (+570), `nilkitkwa_lake.r4` (+3), `fulton_river.r3` (+10, note),
`r7:endako_river@7-12::endako_river.r1` gone; `rule_unresolved.jsonl` 33 → 29; `tributaries_pending` 2 → 0 (both
were Nanaimo r5 and Fulton r3: Nanaimo now binds with its walk, Fulton's undrawn part never walks). The Nilkitkwa
row's scope widens (lake + Rainbow Alley), so its r1–r3 must stay on `lake:329026685` alone — check that too.
Then run `test_t89_rules_that_bind_no_water.py` (with the bundle) — it must pass.

## Left — owner question
**Q-A (T89, owner): where are these eight waters?** The book prints each in an MU that holds no registry water of its
name; every same-name registry candidate lies in another MU, 75+ km from the printed one (Mackenzie Creek 4-21: 4-8,
4-31, 7-17, 8-5/8-6; Frog Lake 5-6: 1-10, 3-29, 7-30; Hidden Lake 5-6: 5-15, 6-8, 3-35, 2-8, …; Secret Lake 5-6: 3-30,
8-7; Square Lake 5-6: 5-3, 6-4, 7-12; Redfern Lake 5-15: 7-42; Squirrel Lake 6-1: 5-1 — that one is Paddy Lake,
name_variants says so). A lead for Redfern Lake 5-15: the registry has **Ruth Redfern Creek** (gnis:28333, MU 5-15,
−120.58, 52.13) — the lake may be an unnamed FWA lake on that creek; owner to confirm before any match. They are most likely unnamed FWA lakes/streams (the 5-6 four with identical "No Ice Fishing;
trout quota 1" rows look like one cluster in the West Chilcotin, beside the 6-1 Basalt/Chipmunk/Gatcho lakes).
**Recommended default: leave all 22 rules unbound** (on the allow-list as AWAITING_WATER) — binding a same-name lake
elsewhere puts a quota or closure on the wrong water. To fix one: locate it (a map/coordinates from the owner or a
gazetteer), add a `name_variants.json` record naming its FWA wbk/blk (or an `ungazetted.json` point), rebuild the
registry (atlas), match the row, shrink `AWAITING_WATER_ROWS`. Mackenzie Creek's "No Fishing" is the one closure
among them (acceptance item "Mackenzie Creek shows the closure" waits on this).
