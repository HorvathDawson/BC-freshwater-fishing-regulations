# T79 — Coming-soon layers: imagery corridor P0 and pools validation (audit N21, D26)

Status: OPEN. Owner needed (go / no-go).

Source: owner 2026-10-21: "No offline 10 m base (10m is useless)" — stream-imagery/SPEC.md still offers the 10 m T1
tier and §12 has 8 open questions (10 m pack, buffer, sources, Province licence, server option, caps).
pools/README.md: "not validated yet … needs about 10 hand-checked pools". The Layers sheet shows Corridors, Pools and
runs, Relief as "coming soon" with no build ticket.

## Work
Drop T1 from the SPEC; answer §12 with defaults; measure pools precision/recall on 2–3 known rivers; record go/no-go in
decisions.md; turn a "go" into build tickets.

## Acceptance
SPEC updated, go/no-go recorded with numbers, measurements filed.

Depends: after the cleanup waves (T61–T63). Size: M.
Owner: yes. Default: no offline imagery until after the first native release; skip the 10 m pack; 250 m buffer;
online-only on web; pools stays a study until the precision numbers are in.
