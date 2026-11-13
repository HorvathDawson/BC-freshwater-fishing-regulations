# T687 — Search "Vedder River": the "see" pointer opened the Chilliwack River, not the Vedder

Status: IN PROGRESS — fixed in the bundle (see below); left (Claude): shows with the next release (deliver rebuilds `see_pointer`), check on the release's search.

Source: emulator run on build 29 (app 0.7.7), 2026-11-12. Search "Vedder River", Open -> "Chilliwack River, Vedder
Crossing Bridge -> Tamihi Rapids Bridge" (`bcfish-session-backup-2026-11-12/apk-077/02-cold-start-then-vedder-regs.png`,
`03b-licence-tab-vedder.txt`).

## What happened (measured on r-0a9930fb8826 and the staging web app)
- The water itself is right. `gnis:3062` Vedder River is its own item (aliases: Vedder Canal), parts "From the mouth up
  to the Vedder Crossing Bridge" (12.72 km + the 0.23 km below the bridge); the Chilliwack (`gnis:8634`) starts at the
  bridge; `water_continue` joins them there. The search row "Vedder River · Stream · Region 2 · near Vedder Crossing"
  opens the Vedder (whole Map, then its mouth part), as `openWater.searched` + `targetFor` say.
- Search ALSO lists the book's pointer row above it: "In the regulations — Vedder River: see Chilliwack / Vedder
  Rivers", button "Open Chilliwack / Vedder Rivers". That button opened `/water/chilliwack-river?view=map&whole=1`
  (reproduced on staging web, phone width). The emulator driver tapped the first "Open …" it found, which was this one;
  the Regulations view then showed the Chilliwack's lowest part (Vedder Crossing -> Tamihi).
- Cause: `pipeline/deliver/bundle/rules/see.py` `see_pointer_rows` opened the target row's FIRST matched water. The row
  "CHILLIWACK / Vedder Rivers" matches `[gnis:8634, gnis:3062]`; the pointer row "VEDDER RIVER" itself matches
  `gnis:3062`. So a reader who typed the Vedder's own name landed on the Chilliwack.

## Fix (mechanical: the water the pointer names)
- `see.opens(ce, te)`: the one water of the target row that the pointer row itself names; else (none, or several) the
  first, as before. Measured over the corpus's 50 pointers: exactly 5 move — Vedder River -> gnis:3062, Bear Lake ->
  Bear Lake (wbk:329101330, not Cowichan Lake), Cedar Lake -> Cedar Lake (wbk:329522925, not the Sumallo), Mara Lake ->
  Mara Lake (wbk:329518146, not Shuswap Lake), Waneta Reservoir -> the reservoir (wbk:329262640, not the Pend d'Oreille).
  Garbutt Lake and Cameron Lakes name several of their target's waters: unchanged. No rule, binding or part moves:
  only where one search button lands.
- Tests: `test_see_groups.py::test_a_pointer_opens_the_water_it_names_in_a_row_of_several` (the five, with a mutation
  that puts the old answer back and sees exactly those five move); `test_every_pointer_is_said_by_the_names_the_book_prints`
  now pins the Vedder and Waneta rows to their own waters.
- Needs a release: the `see_pointer` table is written by deliver (bundle) and copied into `core`. The current release
  r-0a9930fb8826 still opens the Chilliwack from that button.

## Not changed
- The pointer's label still reads "Open Chilliwack / Vedder Rivers" (the row it points at, the book's heading), while
  the button now opens the Vedder River. That is how every pointer reads (Mara Lake: "see Shuswap Lake").
