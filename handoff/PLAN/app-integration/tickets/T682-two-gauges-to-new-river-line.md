# T682 — Move two gauges to the new river line (08NK016, 08MH028); the other nine and the 17 lake-outlet gauges keep today's placement

Status: OPEN

Source: owner 2026-11-12, T678, Owner To-Do row `gauges-11-other-line`, "Take the suggestion". RULINGS P49.

## Work
- Elk River near Natal (08NK016) and Fraser River at Steveston (08MH028) move to the new river line (Elk: the line 40 m away; Steveston: the Fraser, not the Cannery Channel stub).
- Keep today's line for the other nine: Peace near Taylor, Seymour above Lakehead, Klinaklini East Channel, Eve below Kunnum, Bowron, Quesnel near Quesnel, Thompson at Kamloops, Seton, Fraser at Albion. The 17 lake-outlet gauges keep today's placement.
- Edit `data/curated/gauges/matches.json` surgically: promote the two reviewed rows only, never regenerate (a match+promote rerun demoted 25 stations once). Diff to prove only those two moved; bundle and tiles share a vintage.

Depends: the promoted T678 atlas. Size: S.
