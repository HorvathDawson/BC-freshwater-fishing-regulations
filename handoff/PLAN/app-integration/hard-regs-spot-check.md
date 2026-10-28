# Hard regulation cases: spot-check list

Waters whose regulations are hard to show correctly. Check each one in the app after any change to the regs page, the
answers tree or the salmon card. Open a water with `?regs=<item>` (T221: the URL now follows the water on screen), or
search for it. Each entry gives what to look for and how it should read. Add a case when you meet a new one.

| # | Water (link) | What to look for | Expected reading |
|---|---|---|---|
| 1 | **Dala River** (`?regs=gnis:4688`, Region 6) | Trout and char: Region 6's shared "1 over 50 cm" cap, and the stream's "1 brown trout, cutthroat trout, rainbow trout or steelhead in all" count | The count line sits **above** the size box (T221: it counts a rainbow trout, and the box does not hold the rainbow). The box ("Of your 5 trout and char, only 1 may be over 50 cm (20 in)") holds brook, lake, brown/cutthroat and steelhead. Rainbow trout comes **after** the box with "No more than 1 counting any steelhead, cutthroat trout or brown trout you keep". The "must be 30 cm (12 in) or longer" lines are plain text, with no coloured rail. |
| 1b | Dala River: Salmon | DFO lists "Dala River — including tributaries", but that place is not drawn on the map | **One** salmon table. The partial-coverage line leads it, then the Chinook / Coho / Sockeye · pink · chum rows ("Check DFO"), then the "Salmon — closed" row, which names "Dala River — including tributaries · Location not yet mapped". There is no second box. |
| 1c | Dala River: the water screen | A water with one part and no continuation | Search opens **Regulations**, not the map. The views are Regulations and Conditions only (no Map tab). |
| 2 | **Vedder River** (`?regs=gnis:3062`) ↔ Chilliwack | A river that changes its name (`water_continue`) | Search opens the **Map**, with the whole water framed. ▲ / ▼ crosses into the Chilliwack and replaces the frame. One back returns to the map. The Map view stays, even on a part at the end of the chain. |
| 3 | **Kildala River** (`?regs=gnis:8573`) | A water with one part and salmon open for release only | Search opens Regulations with no Map tab. The at-a-glance table shows Salmon as "release only" and Trout and char as "5 a day". |
| 4 | Kitimat states (`x14p0/1/2`, conformance `KITIMAT_RULED_IDS`) | Owner ruling 2026-10-21 (a) differs from page v36 | The tree follows the store's tree, not the old prototype. |
| 5 | Lois Lake (T206 `LOIS_FOLD`) | The hatchery rainbow rule is retired and folded into the steelhead row | Hatchery rainbow trout answer to r4. The group is r1, and Kokanee is the next row. |
