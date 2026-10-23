# T120 — One leading origin: weekday rows take the most permissive keeper

Status: DONE — 3ef3b666. `common.lead_origin` is the one function (main_res, year.leading_answer, _week_answer call it); AST pin + mutation tests in `pipeline/tests/test_answers_lead_origin.py` (smoke). Answers diff: 0 strings changed — the rebuilt answers are byte-identical to live (sha 0c52b8ce…; the 3 live `week` rows, Lower West Arm kokanee, have agreeing origins), so the fix is latent today; card gate 0 problems; no rebuild needed. Left: the app keeps its own TS port `mainRes` (app/packages/core/src/regs/card.ts:169, used by grouphead.ts and caveats.ts) — a second copy of this decision on the app side → owner question Q-T120 below (default: fold into T125).

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

## Owner question
- Q-T120: the app's `mainRes` (core/src/regs/card.ts) re-decides the leading origin from the stored H/W answers for
  one-line views. Should the pipeline store the leader per fish (so the app only reads it), or is a TS mirror pinned
  by parity acceptable? Default: leave it as the documented page-v36 display selection and add it to T125's latent
  duplicate list.
