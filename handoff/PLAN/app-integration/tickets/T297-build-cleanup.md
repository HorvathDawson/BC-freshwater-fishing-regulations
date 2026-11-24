# T297 — Build clean-up, done together with T295 (faster full rebuild) and T266 (marks → data exporter)

Status: DONE — proved 2026-11-08 with T295: the one release command took r-25a89ce9f8e6 to staging. Was: IN REVIEW (2026-11-05, T408 re-scope, 9bc734bfa) — one release command (`pipeline release --staging`, `--prod <r-id>` human-only: refuses an agent / no CONFIRM_PROD / no tty / an unchecked or changed release, then runs the existing `deliver.release.publish --yes`); the upload's app-version guard moved to `pipeline/deliver/release/app_gate.py` (upload.sh calls it); `deliver all --hold-latest` names the release in data/release/CANDIDATE so a failed check leaves LATEST untouched; runbook + build docs updated. Earlier (2026-10-31): STAGE_GRAPH (--graph), --status, --side proof runs, heavy.sh re-entrant (`build-pass-2026-10-31.md`).
the optimizing it (requiring big long builds) can happen together").

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T297 | Build clean-up together with T266 + T295 in one build-focused pass, after the loose ends; one full rebuild proves it | DONE | b1f58097f, ea9cade2a, ae5cb8e6a, 9bc734bfa | T408: one release command, `--prod` human-only re-upload of the same release, app_gate shared with upload.sh, CANDIDATE before LATEST |

## Order
After the loose-end tickets (T287 leftover, tidal leftovers, salmon proposals/apply, T282, T292/T291 tails…), ONE
build-focused pass: T266 (builds read exported decided-data files, never marks.jsonl; the ~6 per-scope apply paths collapse
into one exporter) + T295 (stage timings, input-hash caching, parallel stages) + this clean-up (dead stages/flags, duplicate
helpers, one entry point, clear stage graph, docs). One full rebuild at the end proves it: outputs byte-identical (or every
difference explained) against the release before.

Depends: T266, T295. Size: L. Owner: no.

## T408 re-scope (owner walkthrough 2026-11-05, `handoff/PLAN/data-flow-decisions.md`)
Owner: Claude runs the build batched after curated changes land, and the human runs the SAME command with a
short runbook. `pipeline release --prod <r-id>` re-uploads the same checked bytes (human-only): build once, promote the
same artifact. Run timing leaves deliver (T412).
