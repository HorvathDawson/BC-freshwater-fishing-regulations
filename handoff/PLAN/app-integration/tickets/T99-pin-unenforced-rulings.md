# T99 — Pin the rulings nothing enforces (RULINGS §16) and fix the stale citation (C-4)

Status: OPEN.

Source: pipeline/docs/RULINGS.md §16 and memory `base-plus-deltas-invariant` ("section == region base + named
overrides, every difference attributable; this is what stops tail-chasing").
- ⚠ NOT ENFORCED: Z1, the base-plus-deltas attribution test (removed with the table in 2026-09). (K8 is T98.)
- ◐ curated data with no test: Z2 Mill Lake; Z10 Peaceful/Goose; Q20 Adams; L14 Stellako dates; L16 Chimney; T8 spot
  closures; G8 Columbia/Salmon; Li1; Li10; E13 Goat and Q12 side channels; E14; C3. (K5 is T100.)
- C-4: `test_zone_decisions.py::test_steelhead_over_50_are_not_in_the_trout_one_over_50` cites the withdrawn 2026-09-28
  "mentions char" ruling; cite C2.

## Work
Z1: a needs_bundle test over the release: for a stratified sample of sections in every region, the page's rules are
the region base plus the rows bound to it, each difference named by one row (T57 does this for unnamed waters; share
the helper). One whole-value test per ◐ ruling, each mutation-pinned (seeded from an output, never from the input —
memory `checks-seeded-from-input-launder`). Update the "Enforced in" column.

## Acceptance
RULINGS §16 lists only K5 (T100) and K8 (T98); each new test fails on a mutation of the curated row it pins.

Depends: none (shares a helper with T57). Size: M. Owner: no.
