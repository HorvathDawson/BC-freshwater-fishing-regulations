# T115 — One region membership: Region 5 rules bind on Fraser pieces whose home is Region 3

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding reach 1
(CONFIRMED in `rule_section.jsonl`).

## Decision being unified
"Which sections are in region R?"

## Where it is decided today
- `pipeline/regs/reach/extent.py:41` `area_sections` → `regions.in_region`: home-filtered.
- Raw polygon membership instead:
  - `pipeline/regs/reach/outside.py:140` `region_sections` → `region_limit`
  - `_membership`
  - the steelhead twin's `area_id` (`pipeline/regs/reach/build.py:326`)
  - `pipeline/regs/reach/extent.py:995` and `:1066`

How they differ: section `356364114:418405` (home R3, inside polygons 3 and 5) carries `r5:fraser_river@5-2` rules
r2/r3/r4. That is 12 rule–section pairs on 4 Fraser pieces getting another region's rules.

## Work
- One home-filtered `region_sections(regions, registry, graph)` in `pipeline/registry/regions.py`, plus
  `area_family_sections(reg, kind)` for the steelhead twin. Every place above calls them.
- Pin: a grep test that `region_limit` / raw polygon membership is read only inside `regions.py`.

## Acceptance
Proof is a reviewed per-rule binding diff from a side-dir reach run: exactly those 12 pairs removed, nothing else
(anything else is listed with names and why before promoting).

Rebuild: reach + deliver (no atlas). Depends: none (can share a reach rerun with T102/T111, diffs separate).
Size: S. Owner: no.
