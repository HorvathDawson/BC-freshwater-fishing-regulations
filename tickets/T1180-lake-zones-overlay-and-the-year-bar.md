# T1180 — Lake zones: a hatched overlay and a zones file in the release, never tiny cuts

Status: OPEN — design done, picks open; the app year-bar fix can ride the re-cut; owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1180 | Lake zones: a hatched overlay and a zones file in the release, never tiny cuts | OPEN — design done, picks open; the app year-bar fix can ride the re-cut; owner: see ## Owner | — | picks first (seven); then the year bar and "Closed from" show part-closed days (app only); overlay layer + `zones/1` file in 0.11.0; small closures drawn as overlay + words, never cut |

Source: the owner, 2026-11-26: "yes to app fix and draw the ones we can but the closed p[arts we need to handle properly. like i dont think we should
split up lakes super tiny for example the babine circles of closure and juyst visual and have a text reference saying to look at map or whatever.
could we look into this more to make sure it is cvlean". The design: the planning chat's notes `lake-zones-design` (with `lake-zones-inventory`):
43 closures on 35 lakes. Babine's circles are ALREADY an overlay (`salmon-circles.ts`, since 0.9.0); only the Gullwing zone is a cut. Related: T812, T885, T281.

## Work
1. **App fix (can ride the 0.10.0 re-cut; app only, about 60 lines):** the year bar and the "Closed from" line show part-closed days
   (`strip.ts`, `YearStrip.tsx`, words), for all 43 zones.
2. **Overlay (0.11.0):** curated `lake_zones.json`; deliver step `zones/1` (rings and day runs; no atlas, reach or tiles);
   `not_yet_mapped.zone`; the app layer generalises `salmon-circles.ts` (hatch + edge, drawn only on the zone's days); a tap inside
   says so at the top of the lake page; legend entry; search suffix "· part closed". Installed 0.9.x apps ignore the new file.
3. **Never tiny cuts:** a lake is cut only if the zone is a water of its own (the design's threshold); circles, bays and bridge
   strips are overlay + words ("Open, except the marked area ...: see the map"). Sign-only zones: a dashed pin and words.
4. Then data only: draw the 21 zones from data we hold (12 first), find the outside points (4 bridges, 6 named points, ...).

## Acceptance
A part-closed day shows on the year bar; no lake is cut below the threshold; the lake keeps one answer; old apps read the release.
Severity: medium (a lake answers "open" on the map while part of it is closed). Model: Opus. Size: M (1), L (2). Owner: yes (the picks).

## Owner

- [ ] 2026-11-26 — The seven picks of `lake-zones-design` (recommended first): overlay not cuts; Babine keeps the Gullwing cut and its circles, the Pinkut circle becomes a zone; the year-bar fix in the 0.10.0 re-cut; a zone not drawn on its off days; sign-only zones as a dashed pin + words; Shuswap map A as an overlay; "· part closed" on the search row. Your direction is recorded in `docs/OWNER-RULINGS.md`.
