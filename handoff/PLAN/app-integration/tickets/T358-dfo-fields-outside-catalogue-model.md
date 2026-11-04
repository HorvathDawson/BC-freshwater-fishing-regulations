# T358 — DFO rule fields that are not in the catalogue model

Status: DONE 2026-11-03 — items 1–2 with T356 (salmon/4); item 3 by the owner's rule (below) (found by T350 lean pass, 2026-11-02). The DFO rules are `CatalogueRule`s (typed.py), but these ride
beside the model (`extra="forbid"` would refuse them):

- **`shared`** (`pipeline/regs/dfo_salmon/quota.py:221`, T349): a prose scope string ("Management Units 5-6 to 5-11")
  on a retention rule. The freshwater model removed `aggregation_domain` for exactly this ("wants a structural link like
  `within`, not a sentence", catalogue_parts/rule.py:288-296). Give the model one structural "pooled across" field and
  use it on both sides.
- **Header sizes** as `cm` / `min_cm` on notes and the bucket (`header.py`, `quota.bucket`), not `lengths`
  (`LengthBand`) as the freshwater rules carry minimum / slot sizes.
- **Worker-only `when.open_ended` and rule `check`** (`app/workers/feeds/src/dfo/typed.ts:500, :526`): "until further
  notice" and "dates unparsed" exist only in the TS reading; Python's `When` has neither. The worker's default
  (robust) mode also ranks dated rows over open-ended ones (`holdsKind`, salmon.ts:37), a rule Python does not have.
  Decide one `When` shape (with T350's worker split).

Size: M.

Unblocked 2026-11-03: its dependency (T332 merged 0c276aa10 / T350) is DONE.

## Done 2026-11-03 (with T356)
- **Pooled across:** `CatalogueRule.pooled_across: [area ids]` (validated: `area:` ids, a retention count above zero).
  `quota.pool_of(loc)` reads the location's own `within` areas where its words say "in the aggregate" (an aggregate
  location with no `within` area raises); `mark_shared` sets the field; `pool_words` gives the tree's `whose`
  ("Management Units 5-6 to 5-11", same words as before). The prose `shared` field is gone. No freshwater rule uses it.
- **Header sizes:** a note's length is `lengths` (LengthBand, validated) instead of `cm` (`header.FORMAT` dfo-header/2,
  worker header.ts too); the bucket's `min_cm` is gone (sizes are `regions[r].sizes`). Typed files stamp
  `header_format`; `deliver.salmon.load_headers` re-reads an older-format header from its dated cached page. The
  worker's `upgrade()` converts a carried note's `cm`.
- **Not done — item 3, one `When` shape (owner decision):** Python reads "Apr 1 until further notice" as `unparsed`
  (verdict `check`: 6 rules in the typed files on disk — R1 1, R2 1, R4 1, R8 3; the 3 "To be determined" stay `check` either way); the worker's
  robust mode reads it as open-ended from its start (`when.open_ended`, dated rows outrank it) and flags
  unreadable cells with `check`. Proposed: add `open_ended` to Python's `When` and port the worker's reading into
  `typed.py` / `verdict_on` (answers change on those rows: check → open/closed after the start), drop the worker's
  `check` (it is `when.unparsed`). Do it with the T350 worker split.

## Item 3 done 2026-11-03 (owner: "until further notice is closed, always — a change comes as a new rule")
`typed.to_rules`: a date cell that says "until further notice" gives the row NO window (in force all year), never
`unparsed`. The 6 rules on disk (R1 Oct 15 non-retention FN0952; R2 upper Fraser no fishing; R4 non-retention; R8 sockeye,
chinook ×2 no fishing) now answer every day: closed, or release only for the 2 non-retention rows. A dated row that opens
the fish still wins on its days (`verdict_on`: opens before closures). The worker already read them as in force from
their start with dated rows outranking (`dates.ts`, `holdsKind`), so the two now agree except Jan 1 → the start date,
where the worker has the row not yet in force unless nothing else holds; left for the T350 worker split. "To be
determined" stays `check`. Typed files re-typed (`python -m pipeline.regs.dfo_salmon.feed`, which crashed run as a module:
`_header_format` was defined below `__main__`, fixed).
