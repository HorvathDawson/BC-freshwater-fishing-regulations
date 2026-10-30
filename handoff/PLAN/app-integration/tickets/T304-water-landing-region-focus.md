# T304 — A water page landing reads its own region's files (fast hand-over on slow lines)

Status: OPEN (T282 finding 2026-10-31).

Source: T282 measured /water/vedder-river on Fast 3G: the 647 kB bundle is in at ~0 s, but the hand-over waits for the
whole core (1.4 MB) + whole water (2.2 MB) + regs-2 + sql.js wasm because no map region is focused on a landing, so the
T256 per-region slices aren't used (~37 s).

## Work
The prerendered water page names its region (e.g. `<meta name="cift:region" content="2">` / its item id); the app focuses
that region before the first read so core-<r>/water-<r> serve the landing. Measure Fast-3G "app took over" before/after.

Depends: T256, T269, T282. Size: S. Owner: no.
