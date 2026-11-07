# T493 — A creek shows a gap where its small lake drops out zoomed out

Status: OPEN — owner 2026-11-07 (Foley Lake on Foley Creek).

Zoomed out, a small lake leaves the tiles before its creek does. The creek's line stops at the lake's shore and starts
again on the far side, so the creek looks broken.

## Owner decisions (2026-11-07)
- **Fix:** at zooms where the lake is hidden, draw the creek's line through the lake (along the lake's own centre
  line / the FWA connector through the polygon). At its usual zoom the lake is drawn as it is now.
- **Colour:** that stretch takes the LAKE's status (it is the lake's water), not the creek's.
- **Tap:** a tap on that stretch opens the lake.

## Do
- Find the lake's min zoom in the tile build and where the stream layer drops the in-lake connector (FWA's
  through-lake lines). Count how many lakes on a drawn stream are hidden at each zoom (z8–z12) to size it.
- Ship the connector as a stream-layer feature from the lake's min zoom down, carrying the lake's item (its
  status key and tap target), shown only below the lake's min zoom (one source of truth: tiles, no app geometry —
  AGENTS 28, layers only via `layers.source.json`).
- Shots before/after: Foley Lake/Creek and 2–3 others, at the zooms where the gap shows. Build beside; never rebuild
  live tiles to test (AGENTS 17). Ships with a tiles rebuild (with T489, same stage).
- Fast-fail (AGENTS 20a): a tile check that no drawn stream breaks at a hidden lake, if cheap.
