# T941 — Search rows and neighbour-water names show a water's name cased, as the water page does

Status: OPEN.

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
