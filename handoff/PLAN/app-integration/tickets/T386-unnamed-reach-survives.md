# T386 — An unnamed reach survives a data update and opens from a link everywhere

Status: OPEN (owner 2026-11-04: "yes we want this … also make sure the links refresh opens the unnamed streams", in 0.5.0).

From T369: an unnamed reach open during a data update is CLOSED, not carried — carrying it (and resolving an unnamed
`?blk=&m=` link on the phone) needs each section's stream key + measure span in the native bundle (`section_measure`;
the web has the `lines` file). Work: the pipeline writes it (release, phone-held), the app resolves blk + m → the
section in the release it holds (the same pure resolver as named links), the update keeps the reach open at the same
place, and a reload / start link opens it (web and phone). Tests: update carry, link round trip, offline.
Rides the ONE release rebuild of 0.5.0. Size: M.

## Owner 2026-11-04: reuse the links' data
The web already opens `?blk=&m=` links from the `lines` file (T369, web-only). The phone has no address bar, but a data
update needs the same resolution in-app. Decision: the phone downloads `lines` too (drop it from `contract.WEB_ONLY`;
~1.3 MB gz on the offline total) — ONE resolver and one table for web and phone, no `section_measure`. Later, optional:
app links (a shared https link opening the installed app).

## Pipeline half done 2026-11-04
`contract.WEB_ONLY` is empty: `lines` ships phone-held (no `web_only` mark; counted in the offline total). Live release:
phone-held 15.90 MB gz + lines 1.28 MB = 17.19 MB (gate 20 MB). Tests: test_section_line.py. Takes effect with the one
rebuild. LEFT (app, after the T382 clock lane leaves `ui/place.ts`): retire `tools/native-no-links.test.ts` (links are
no longer web-only), give the phone's loader the line reads, `carry.ts` carries an unnamed reach by its place
(old release `sectionLine` → `sectionAt` in the new), and a native start/reload of `?blk=&m=` opens it.
