# T205 — Curation review: zone entries without a map, species and sizes per rule, no pilot, a compact check widget

Status: DONE (7a55e651)

Source: the owner on the local curation review app (T189/T191/T164), 2026-10-26.

## Built
1. **Zone entries load and draw nothing on the map.** A zone rule holds region-wide; the reach is far too large.
   No reaches/splits request is made for a zone entry, the map column is a note, and the "show on map" button is not
   shown on a zone rule.
2. **Species and sizes in each rule block** (`RuleSpecies`): the rule's `species`, `species_except` and `lengths`
   exactly as stored, one line per species with its size ranges (`min–max cm → take n`; null = open). Empty fields
   are said, not hidden ("no species stored", "no size range stored").
3. **The pilot is removed**: the toggle, the pilot ordering, the pilot progress, `GET /api/pilot`, `reuse.pilot_targets`
   and the `pilot` query. Marks (and their `seconds`) are kept. The gate's `pilot` commands (pipeline) are untouched.
4. **PDF source image for zone entries**: already served — `evidence.locate` finds a zone entry's passage by text on its
   printed chapter page and crops it (`kind: passage`). Where the words are not found the pane says so ("Not located").
5. **Compact check widget**: one row of four segmented y / n / ? controls (Text, Rules, Place, Answer), the long
   questions in each label's tooltip and a "what do these mean" fold; keys t r p a, y n ?, Enter unchanged.
6. **A bigger "Show on map" button** on each water rule block.
