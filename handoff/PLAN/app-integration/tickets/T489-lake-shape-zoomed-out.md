# T489 — Lakes lose too much shape when zoomed out

Status: OPEN — owner 2026-11-07 (0.6.9 on the phone).

Owner: "lake polygons lose almost too much shape when zoomed out."

## Do
- Find the simplification per zoom for the lake layer (tile build: tippecanoe/simplify settings in the tiles stage;
  T461 did the same exercise for the tidal polygon) and measure vertex counts + tile sizes per zoom.
- Proposal: gentler simplification at low zooms (e.g. z6–z10) within the tile-size budget; side-by-side shots of a few
  well-known lakes (Harrison, Stave, Pitt, Okanagan, a small lake) before/after. Ships with a tiles rebuild (next
  release); never rebuild the live tiles to test (AGENTS 17) — build beside.
- Fast-fail (AGENTS 20a): a check on minimum shape kept (e.g. area/perimeter ratio vs source) per zoom, if cheap.
