# T1074 — AGENTS rule 28 says map layers come only from `layers.source.json`, but every stream overlay is a runtime layer

Status: OPEN.

Source: the 0.9.1 chat, 2026-11-24 (T1029 ticket and draft PR #112): stream-closed, the issue stripe and T1029's
one-bank cue are added at run time; the source DSL cannot express an offset half-line keyed by feature-state.

## Work
1. List every runtime map layer and why it is not in the source file.
2. Decide one way: extend the DSL so they can live in the source file, or reword rule 28 to name the allowed
   runtime kind (feature-state overlays) and add a guard that lists them, so a new one cannot appear unnoticed.

## Acceptance
Rule 28 and the code agree; a test fails when a runtime layer is added outside the listed set.

Severity: low to medium (a rule the code breaks teaches lanes to ignore rules). Release: 0.9.2. Model: Sonnet for the
list and rule words, Opus if the DSL is extended. Size: S–M. Owner: no.
