# Audit 2026-11-05 (T333–T335): the report

Scope approved by the owner 11-05 (T333). Four finders (Fable, read-only, one per area) → four adversarial verifiers
(Opus, each finding re-checked against the code and git log). Per area: `<area>.md` = the finder's list,
`<area>.verified.md` = the verdict, evidence, severity and fix for every finding.

| area | findings | confirmed | plausible | rejected |
|---|---|---|---|---|
| pipeline | 18 (+1 new) | 15 | 3 | 1 |
| app + workers | 19 | 9 (+1 part) | 6 | 5 (+1 n/a) |
| review app + deploy | 17 | 14 | 2 | 1 |
| tests + docs | 25 | 19 | 3 | 3 |

## Fix tickets, most harmful first (the owner picks the order)
| ticket | what | severity |
|---|---|---|
| T429 | a missing review input can EMPTY a curated file (found_names / slough_calls) on a saved mark | high |
| T430 | crash + problem-report formats have no parity test: one new field and the worker refuses every report | high |
| T431 | the feeds worker's 26 test files run nowhere; its silent catches; skipped suites; REPORT_SALT default | high |
| T432 | the app's golden parity passes green on zero cases (rules→app unproven after each release); no app skip gate | high |
| T433 | staging `upload.sh` can leave staging dark; `sync-inputs.sh` / HYDAT mask failures; check-public | high |
| T434 | review app turns errors into empty answers (incl. T421's refusal swallowed in `reuse.py`) | medium |
| T435 | pipeline: hard-coded data paths, mtime staleness, two release-id formulas, silent vintage skip | medium |
| T437 | docs + tickets drift: 12 status mismatches, AGENTS rules not true, one START-HERE, safe deletes | medium |
| T436 | dead code + duplicate helpers | low |
| T438 | a locked 1.4 GB agent worktree | low |
| T335 | delete archive/ (verified: nothing live reads it; move 4 cited docs first) | — |

## Checked and sound (verifiers agree)
No GitHub schedules; staging scripts name only staging buckets and both prod scripts have CONFIRM guards; no secrets
echoed; heavy builds go through `scripts/heavy.sh`; kills are own PIDs; no `git add -A`; `writes.py` is the one
curated writer of marks; no cascade logic in TS; no clock reads outside `clock.ts`; no DOM/Platform leaks beyond the
T410 allowlist; no external assets beyond the two exceptions; `curated.py`, content-based stage keys and
`emit --check` in CI hold; the pipeline no-silent-skips allowlist counts hold.
