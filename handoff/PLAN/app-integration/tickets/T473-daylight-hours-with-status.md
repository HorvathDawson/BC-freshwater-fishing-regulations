# T473 — "Daylight only" says today's real hours, with the status

Status: DONE 2026-11-07 0f4b0063a (owner: "could display daylight only text here better too").

Under the year bar's status the line reads "Daylight only · fishing 07:04–18:37 PT": sunrise minus the rule's "before sunrise"
minutes to sunset plus its "after sunset" minutes, at the water's point on the picked day (`core/sun.ts daylightRange`,
`@app/ui useDaylight`, drawn by `YearStrip` `hours`). The rule's own words ("Daylight only: closed from 1 h after sunset to
1 h before sunrise") stay as a small secondary line with its Rule book link (`TimedLines` `hoursSaid`). No sun point: the plain
rule line as before (the same rule without hours, not a fallback).
Offsets: NOT hard-coded. They are read from the item's own words (`caveats.ts daylightOffsets`, the rule's `where`
"60 min after sunset–60 min before sunrise"); the data carries no numeric field, only that string, which the existing
T318 line already parsed. A numeric pair on the caveat item (pipeline) would remove the parse; not invented here.
Tests: `Edges.test.tsx` (hours with and without a point), `sun.render.test.tsx` (`useDaylight`).
Size: S.
