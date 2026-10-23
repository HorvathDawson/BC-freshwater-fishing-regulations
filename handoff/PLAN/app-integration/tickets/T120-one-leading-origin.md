# T120 — One leading origin: weekday rows take the most permissive keeper

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding answers 3
(CONFIRMED code divergence; live impact narrow).

## Decision being unified
"When two origins answer for the same fish (e.g. H and W), which one leads?"

## Where it is decided today
- `pipeline/deliver/answers/rows.py:2243` `_week_answer`: takes the most permissive keeper (H keep 2 / W keep 5 gives W;
  H release / W closed gives W), despite a docstring that says it uses `main_res`.
- `rows.py:614` `main_res` and `year.leading_answer:45`: copies of each other.

How they differ: on a weekday row, a stricter answer can lose to a looser one, so an angler can read a bigger quota than
the main row gives.

## Work
- `common.lead_origin(h, w)` in `pipeline/deliver/answers/common.py`; `main_res`, `leading_answer` and `_week_answer`
  call it. If a "stricter" mode is really wanted somewhere, it is a named mode of the same function.
- Pin: a grep test that no other function in `answers/` compares origins to pick a leader.

## Acceptance
Proof is a reviewed behaviour change: the answers diff touches weekday rows only, each change named. A fixture with H
release / W closed leads with the same origin as the main row.

Rebuild: deliver. Depends: none (do alongside T104, same modules). Size: S. Owner: no.
