# T113 — One name caser: map labels say "O'rourke Lake", the card says "O'Rourke Lake"

Status: PARTIAL — code done 2026-10-24; the tiles-window build, shots and after-numbers are left (see Outcome).

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

## Outcome
Code done 2026-10-24 (tiles window); takes effect at the next tile build (labels) + deliver.
- `tiles/names.display` = whitespace + `place_names.display_case` (the export's card caser), so label == card by
  construction. `read/source.py`'s two `.title()` calls go through `place_names.words_display` (same caser).
- Gate: `tiles/export._writer` fails the export on any tile name `is_shouting` would flag. Pin:
  `test_one_name_caser.py` (cases, a grep that `.title()`/`.capitalize()` live only in `place_names.py`, and the
  export gate by mutation).
- Behaviour change, measured on the live registry + layers: `handoff/PLAN/app-integration/T113-name-changes.tsv`:
  18 registry items (O'Rourke Lake, "Moss Pothole" Lakes ×7, Hart Lake (Fort St. James), #14A …) and 85 admin
  features (Akamina-Kishinena Park, McDonald Creek Park, Dune Za Keyih Park [A.K.A. Frog-Gataga Park] …).
- The 35 all-capitals bundle `entry.name` values are KEPT as the book's spelling: the export shows them
  `display_case`d and keeps the raw one as `official_name` (export `checks.py` already refuses a shouting shown
  name). Casing them in the bundle would drop `official_name`; not done.
- Not unified: `atlas/graph/names._display_case` (atlas-side node labels; the tile re-cases them through `display`).

## Outcome (2026-10-28 sweep, 699c1392)
Swept `display_case` on Mc/Mac/O'/D'/hyphen/St./(No. 2)/First Nations cases: all right except "2ND" -> "2Nd" (fixed: ordinal
suffix after a numeral stays lower, "2nd") and a digit inside a word (JAJI7EM, Skwxwu7mesh) now keeps the book's spelling
instead of "Jaji7Em". Test added in test_one_name_caser.py (13 pass; 335 across the name-touching suites). Committed before
T182's deliver run, so it picks the caser up. Still owed (rebuild, not run here): tiles build + deliver, then re-measure
T113-name-changes.tsv (a few "Nth"-names and digit-in-word names change). Stays PARTIAL on that rebuild only.
