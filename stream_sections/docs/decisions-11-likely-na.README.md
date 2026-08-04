# decisions-11-likely-na.json — human-review verdicts to apply LATER (with Dawson)

**Do not auto-apply.** This is a parked batch of human-review verdicts exported from the review
UI (`decisions (11).json` from Downloads, 2026-08-04). It captures the **likely not-applicable /
wrong / defer** calls that we still need to fold back into
`stream_sections/docs/14-locators-to-curate.json` — to be done together in a later session.

## Shape
Map of `locator_id -> { verdict, ts, note? }`.

## Contents (165 entries)
| verdict | count | meaning when applied |
|---|--:|---|
| `not_a_split` | 77 | set `status = not_applicable` on that locator |
| `wrong` | 60 | the auto-proposal/point is wrong — needs re-resolution (keep todo, flag) |
| `defer` | 28 | park it (`status = deferred`) with the note |
| with notes | 124 | many carry a human note (e.g. "lake needing splitting.") |

## Next step (later, together)
Write a small applier that, per entry, updates the matching locator: `not_a_split` -> `not_applicable`,
`defer` -> `deferred`, `wrong` -> keep todo + `[review: wrong] <note>`. Cross-check against the
already-migrated schema (anchor_kind/resolver_hint, no split_id) before writing. Verify a sample
by hand first — some `not_a_split` on lakes actually mean "lake needing splitting" (a docs/15
lake-internal split), which is `deferred`, not `not_applicable`.
