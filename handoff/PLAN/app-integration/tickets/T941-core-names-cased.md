# T941 — Search rows and neighbour-water names show a water's name cased, as the water page does

Status: IN PROGRESS — lane/lb-rules-leftovers done; left (Claude): main chat review + merge.

Source: main chat 2026-11-20 (review of the T926 number-names lane). A water's shown name is stored in the registry as
its source gives it, and some sources shout: "GREY HORSE LAKE #1", "KITWANGA MILL POND". The water page and the map
label case it with the one caser (`pipeline/deliver/bundle/place_names.py` `display_case`), but the core file's
`item.name` ships as stored, so a search row (`app/packages/data/src/release/queries.ts` ~l.50, the name search) and a
neighbour water's name (`app/packages/data/src/regs/reads.ts` ~l.636, an exit or a continuation) read it in capitals.
Measured on the live release r-6258ffded676: 25 waters whose core `item.name` is all capitals; T926's number-name pass
adds 8 more ("PIGEON LAKE #2", "UNNAMED CAYOOSH CR", …) with the next atlas registry.

## Work
- Case the core file's `item.name` at bundle export with `display_case` (the one caser), never in the registry: the
  registry keeps each name as its source gives it (the matchers key it, T926 review).
- Check the search key still finds the water by its stored spelling (aliases, `name_key`), and that no two waters'
  shown names collide after casing.
- The export, the water page and the map label are unchanged (they already case).

## Acceptance
No search row or neighbour name shows a water's name in capitals where its water page shows it cased; search finds the
same waters (probe rerun); a test pins one shouting name end to end.
Depends: none (a data release ships it). Size: S. After this release unless it is small.

## Result (lane/lb-rules-leftovers, 2026-11-20)
- `pipeline/deliver/bundle/place_names.py` `case_core_names(con)`: `display_case` on core `item.name`, `name_key`
  re-stored with `stored_key` (the stored spelling's search key; a name whose key would change is refused), and the
  water's `name_source` row moves with its name (the app joins `name_source.name = item.name`). Called once from
  `_core_extra` in `pipeline/deliver/release/build.py`, so the core-<r> slices get it too. Registry, bundle and export
  (`official_name`) untouched.
- Side deliver base 52f647546 (r-39e0f6ef6117) vs after (r-b2cf828d8f11), same section handles: 26 core names change,
  all case only, nothing else in core: GREY HORSE LAKE #1, O'ROURKE LAKE, MITE LAKE, "MOSS POTHOLE" LAKES x7, KITWANGA
  MILL POND, FISH LAKE (unnamed lake approx. 2 km northwest of McClinchy Lake), LOST LAKE, JERRY SULINA PARK POND,
  KINGLET LAKE, HART LAKE (Fort St. James), WENTWORTH LAKES, HALL ROAD POND, "BLUEY LAKE POTHOLES" x7, BLUEY 1. The
  regional slices carry the cased names (checked core-4, core-6); unmatched `name_source` rows are the same count
  before and after in every slice. About 8 more follow with T926's next registry.
- Collisions: one new same-name, LOST LAKE (wbk:329242402) now reads like 13 other Lost Lakes (already told apart by
  "near X"); the Moss Pothole and Bluey groups were the same name among themselves before. Search ties are ordered by
  `m.name`, so tie order among equal names may shift slightly.
- Tests: `pipeline/tests/test_t941_core_names.py` (with a mutation test); end-to-end pin
  `test_release.py::test_cores_water_names_are_cased_as_the_water_page_cases_them` (KITWANGA MILL POND) run on the
  side-after bundle: test_release + test_release_slices + t941 33 passed.
- Shots: `T941-shots/search-row-before.png`, `search-row-after.png` (KITWANGA MILL POND -> Kitwanga Mill Pond).

## Review leftover (lane LF, 2026-11-20, d60b72ba6)
`case_core_names`: an item holding a `name_source` row for both "X" and display_case("X") hit the (item_id, name) key
when its name's row moved. Now the name's own row takes the shown spelling and the other row is deleted first.
Test: `test_an_item_holding_both_spellings_keeps_one_row_its_names` (IntegrityError without the fix). Measure: 0 such
items in the live bundle and in the l9-strictest side bundle; no data changes.
