# T182 — Release file splits for the web's first search, tap and Conditions (from T129)

Status: OPEN. Source: T129's measured walk on r-bb4331929428 (its ticket has the byte table) — proposals that change the
release format, so not done there.

Biggest first:
1. An unnamed tap fetches all of `conditions` (2.88 MB) only to walk `section_down` for "Tributary of …": give
   `section_down` (628 kB) its own file → unnamed tap 8.53 → ~6.3 MB.
2. "Show on the map" fetches all of `water` (2.74 MB) for a bbox in `part_geo` (578 kB): split `part_geo` out.
3. First search fetches `near` (1.99 MB) only for each result's "near X": carry each water's nearest town in core →
   first search ~3.08 → ~1.15 MB.
4. Conditions opens core (1.09 MB) to find lake gauges, and `gauge_clim` is 1.44 MB of `conditions`: split both.
5. Feed `index.json` on R2: `Content-Encoding: gzip` + short Cache-Control in the (not yet written) feed sync.
Not worth it: splitting region files (core + water are 87% of a tap). Owner call: prefetch core + wasm (1.75 MB) after
first paint?

Each split: new file id in the release contract, the loader reads it lazily, `WEB_BUDGET` gates tightened to the new
numbers, the pinned request walk (`app/tools/request-walk.mjs --pin`) re-cut, phone (offline) unaffected.
Size: M per item. Owner: Q164 (prefetch).
