# T118 — One main line: spans pick a water's stem by length, continuation by section count

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding cross 2
(PLAUSIBLE, partly checked on `bundle.sqlite`).

## Decision being unified
"Which sections are this water's main line?"

## Where it is decided today
- `pipeline/deliver/bundle/spans.py:85` `main_stem`: by length.
- `pipeline/deliver/bundle/continuation.py:109` `_main`: by section count; `topology.mouth_downs` reuses it.

How they differ: Ecstall River has 37 of its 42 sections `off_stem`, so the count-based line differs from the
length-based stem. Its exit still reads Skeena, so today the impact is on evidence only, but any water where the two
lines end at different mouths would get a different exit and continuation.

## Work
- `main_line(water)` in a shared `pipeline/deliver/bundle/lines.py`; spans, continuation and `topology.mouth_downs`
  call it. Do it with or after T108 (same modules).
- Pin: a grep test that no other module in `deliver/bundle/` picks a stem.

## Acceptance
Proof: the `water_exit` and `water_continue` diffs before and after; expected empty or each change named.

Rebuild: deliver. Depends: T108 (same files). Size: S. Owner: no.
