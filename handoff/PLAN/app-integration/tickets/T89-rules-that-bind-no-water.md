# T89 — Rules that bind no water: 10 unmatched synopsis rows + 3 rules with no extent

Status: OPEN.

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
