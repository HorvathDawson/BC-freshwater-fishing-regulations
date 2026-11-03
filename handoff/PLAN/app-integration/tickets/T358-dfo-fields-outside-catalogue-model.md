# T358 — DFO rule fields that are not in the catalogue model

Status: OPEN (found by T350 lean pass, 2026-11-02). The DFO rules are `CatalogueRule`s (typed.py), but these ride
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
