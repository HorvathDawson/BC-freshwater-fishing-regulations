# T113 — One name caser: map labels say "O'rourke Lake", the card says "O'Rourke Lake"

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding answers 6
(CONFIRMED on `data/generated/tiles/layers/*.geojsonl` and `bundle.sqlite`). Related to T61 2.8 (one name
normaliser, which handles matching keys); this ticket is display casing.

## Decision being unified
"How is a water or place name cased for display?"

## Where it is decided today
- `pipeline/deliver/tiles/names.py` `display()`: plain title-casing.
- `pipeline/deliver/bundle/place_names.py:80` `display_case`: handles Mc/Mac/O', numbers and suffixes.

How they differ: `display("O'ROURKE LAKE")` gives "O'rourke Lake" and `display_case` gives "O'Rourke Lake"; "#14A"
becomes "#14a". The shipped layers contain "O'rourke Lake" (`lake.geojsonl`), "Mcdonald Creek Park"
(`park.geojsonl`) and about 90 more miscased features. 35 `entry.name` values in the bundle are still all capitals and
only the export re-cases them.

## Work
- `display_case` is the only caser; `tiles/names.py` imports it.
- Gate: bundle and tile names never shout (no all-capitals name of more than one word) and map label == card name for
  the same item.
- Pin: a grep test that no other module title-cases names (`.title()` / `capitalize` on names) in `pipeline/deliver/`.

## Acceptance
Proof is a reviewed behaviour change: the list of names that change in tiles and bundle (expected about 90 + 35).

Rebuild: tiles + deliver. Tiles and bundle must share a vintage (memory `tiles-bundle-same-vintage`), so this rides
the tiles window (T64). Depends: T64 timing. Size: S. Owner: no.
