# T108 — One "goes on as": continuation and spans disagree on whether water A continues as water B

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding cross 1
(CONFIRMED: `water_continue` joined to the span lower-end tokens in `bundle.sqlite`).

## Decision being unified
"At its lower end, does water A go on as water B, meet B at a confluence, or end?"

## Where it is decided today
- `pipeline/deliver/bundle/continuation.py:132` `_goes_on`: tolerances 100/200/1000 m.
- `pipeline/deliver/bundle/spans.py:143` `lower_end_into`: tolerances 100/250 m plus a magnitude test.

How they differ: 11 of the 17 `water_continue` pairs say "goes on as" while spans say `mouth`. They include Pass→Screw,
Rochester→Holmes, Jr 4-1-2→Jr 4-1 (which the `spans.py` docstring gives as a *not*-continuing example), Finch
Connector→Logger's Lane and Westway Ditch→Magnolia Trib 1. Jeperson Side Channel continues into itself. The "goes on as"
button and the span end words can contradict on one page.

## Work
- `continuation.goes_on(a) -> YES | CONFLUENCE | NO` with one constant set. `spans.lower_end_into` reads it instead of
  deciding again. No water continues into itself.
- Bundle pin: every `water_continue` pair has a `continues:`/`confluence:` end token, and every such token has a pair.
- Grep pin: no other module in `deliver/bundle/` defines continuation tolerances. Tolerance constants also overlap
  T61 2.10; keep them in one place.

## Acceptance
Proof is a reviewed behaviour change: the 17 `water_continue` pairs and every `span_end` token, before and after, each
change named. The bundle pin passes; the self-continuation is gone. The p15 owner ruling stands (keep the 10 municipal
continuations; reversed continuations refused).

Rebuild: deliver. Depends: none (see T118 for the shared main line). Size: M. Owner: no.
