# T1023 — "Flows into X" should open the receiving water's section on the map, not its regulations

Status: DONE (641725521, dd48e15d8) — `exitAction` opens the receiving water on its Map (`view: "map"`) at the mouth's part; a water with no Map view still opens on its first view. Tests: waterMap.test, back.test updated. NOT checked on a device/emulator.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1023 | Staging report: "Flows into X" should open the receiving water's section on the map, not its regulations (Liumchen Creek) | DONE | 641725521, dd48e15d8 | staging report 2026-11-23 (`c9abc20745908d89`) |

Source: STAGING report `c9abc20745908d89`, 2026-11-23 05:43Z, Android 0.8.5 (staging build), water · map, Liumchen Creek
(`gnis:2500`) part 0, zoom 13.3. Verbatim: "When I click flows into x it should go to that section in the map (section
it goes into) not open the regulation tab for the water body. Because that makes it feel disconnected and hanky".

## Work
1. From a water's Map tab, the "Flows into <water>" label/link (T985 #2) opens the RECEIVING water on its Map tab,
   framed on the part the mouth joins (the release's `water_exit` mouth place), that part selected — not its
   Regulations tab. Same from the bar's exit line. One load path (`one-load-path-per-screen`).
2. Back returns to the creek's map where it was.
3. Test on the shell path; emulator check on the device pass.

Depends: none. Size: S–M. Owner: no.
