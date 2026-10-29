# T238 — offline regions manager (phone)

Status: OPEN — not started.

Source: owner 2026-10-28 — approved proposal P4 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
1. A "Offline data" page (More tab): each region (1–8) with its size (tiles + basemap + release slices), on/off, and the
   total storage used; delete a region's files; "keep everything" stays the default.
2. Tiles: per-region pmtiles extracts (or byte ranges of the province archive) — decide with T85/T202 (install size).
3. Map: a region not kept draws online-only (online) or the outside-BC paper with a "not downloaded" note (offline).
4. Update service (T231) downloads only kept regions.
## Acceptance
On a phone: turn off 3 regions → storage drops by their size; the map there says "not downloaded" offline; turn one back
on → it downloads; tests over the in-memory phone store.
Depends: T231, T85/T202 (tile split). Size: M.
Owner: yes — default region set on first install (default: all, ~434 MB, with the choice offered on first run, T248).
