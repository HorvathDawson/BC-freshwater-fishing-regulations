# T1186 — Post-stage stop shapes: what can still stop a build after its stage ran

Status: OPEN — a #286 follow-up; scope to confirm

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1186 | Post-stage stop shapes: what can still stop a build after its stage ran | OPEN — a #286 follow-up; scope to confirm | — | a lane: list every way the key or trace step can stop a build after the stage already ran; each says what to do, none loses the result (scope to confirm) |

Source: the planning chat's follow-ups after #286, 2026-11-26. **Scope to confirm:** the sources give this line only. The third review's M1 is the model: an
untraceable module (uv / virtualenv, an entry script outside the repo, an editable or conda install) stopped the build AFTER the stage ran; #286 keyed such a
module by its file's bytes and never stops. The same shape may remain elsewhere (the atlas record, `--ship-stale-atlas`, the tiles preflight).

## Work
1. Walk the code after each stage for a refusal that comes after the work; list the shapes.
2. A stop after the stage keeps the result and says what to run; a test per shape.

## Acceptance
No stop discards a finished stage. Severity: medium (hours of build lost). Model: Opus. Size: S-M.
