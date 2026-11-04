# T386 — An unnamed reach survives a data update and opens from a link everywhere

Status: OPEN (owner 2026-11-04: "yes we want this … also make sure the links refresh opens the unnamed streams", in 0.5.0).

From T369: an unnamed reach open during a data update is CLOSED, not carried — carrying it (and resolving an unnamed
`?blk=&m=` link on the phone) needs each section's stream key + measure span in the native bundle (`section_measure`;
the web has the `lines` file). Work: the pipeline writes it (release, phone-held), the app resolves blk + m → the
section in the release it holds (the same pure resolver as named links), the update keeps the reach open at the same
place, and a reload / start link opens it (web and phone). Tests: update carry, link round trip, offline.
Rides the ONE release rebuild of 0.5.0. Size: M.
