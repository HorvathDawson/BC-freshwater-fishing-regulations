# T1264 — The map comes back to the same spot after a reload

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1264 | The map comes back to the same spot after a reload | OPEN | — | web: the map's centre and zoom in the page address as query parameters, read back on load; not assigned to a release |

Source: the owner, 2026-11-27 (planning chat), verbatim: "when you reload a webpage at a location it should be able to go back to
same spot in map." Asked where the position lives (the page address / the browser only / both), his answer, verbatim: "adress but as
query params?" (2026-11-27). Reading (the planning chat's): in the page address, as query parameters, not a hash.

## Work
1. The web app writes the map's centre and zoom into the address as query parameters while the map moves (replace, never a new history
   entry per pan; the back rules of decisions.md section 8 stay), and reads them on load before the default camera.
2. AGENTS rule 5: a link may carry a place (a position, or an FWA stream key + distance), never a section id or dense id.
3. The parameters live beside today's link parameters (`app/apps/mobile/src/startLink.web.ts`); a link that opens a water still wins.
4. A shared link opens at that spot too (the same parameters).

## Acceptance
A reload at any zoom lands on the same centre and zoom; back and forward behave as today; a test pins the parameter names and the
"a water link wins" order; no section id appears in any address.

Depends: none. Size: S. Release: not assigned. Owner: no (answered).

## Owner

- [x] 2026-11-27 — Where the map position lives across a reload: the page address / the browser only / both → "adress but as query params?" (2026-11-27)
