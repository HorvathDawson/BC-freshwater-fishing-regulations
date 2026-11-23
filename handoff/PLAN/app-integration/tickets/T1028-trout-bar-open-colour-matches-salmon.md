# T1028 — The trout schedule bar's "open" colour should be the salmon bar's

Status: DONE — 391b37ddb. The Trout and char year bar's keep run now wears `palette.salmon.open` (as `SalmonRect`), a token, so light, dark and colour-blind follow; the map's colours are untouched. Test in TroutCard.test (all three palettes). NOT seen on a device; before/after side-by-side shots NOT taken (no local web build with release data in this lane).

Source: owner 2026-11-23 ~00:05, verbatim: "Also the schedule bar for trout should have same open color as salmon (doesn't need to match map)..?"

## Work
1. `TroutYearBar` keep run: the salmon bar's open fill, through the shared token (AGENTS 28/29), light + dark + colour-blind.
2. Map colours unchanged. Shots of both bars side by side into `T1028-shots/`.

Depends: none. Size: S. Owner: no.
