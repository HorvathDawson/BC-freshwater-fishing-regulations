# T183 — A piece that straddles a region line carries both regions' rules (reverses T115)

Status: OPEN. Owner 2026-10-24 (braid review, then confirmed when asked about the conflict with T115): "Those straddle
pieces are supposed to get both regions regulations" → "Both regions (reverse T115)".

## Work
- `pipeline/registry/regions.py` `region_sections` (T115's one home-filtered membership) becomes raw membership: a
  section is in every region whose polygon it lies in (by length > SLIVER_M, so a touch is not membership). Keep ONE
  function (T115's unification stays; only its answer changes), its grep pin, and the steelhead twin's
  `area_family_sections`.
- The page: a water whose piece is in two regions shows both regions' rows (check the regs page groups them clearly;
  zone base + named overrides per region — base + deltas invariant).
- Binding diff vs live: every change must be a region-wide rule landing on a straddling piece (expect T115's 12 Fraser
  pairs + 3 steelhead-twin pairs + 1 licensing pair back, plus others). The card gate stays at 0 (two regions' quotas on
  one piece must still produce one decided answer per fish — if they conflict, report it, don't pick).
- RULINGS row superseding T115's; AGENTS rule updated.

Depends: after the T67 braid merge (both move reach bindings; one attribution per rebuild). Size: M (reach + deliver).
