# T1055 — A new production crash kind on 0.8.5: `TypeError: Cannot read properties of undefined` (2 reports)

Status: OPEN.

Source: production feeds worker, crash kind `fe6313a53dca22c6`, 2 reports, last 2026-11-23 06:40Z, app 0.8.5 (read by `pipeline reports`, planning chat 2026-11-24). It arrived one second before kind `9e825d06c018b12a` ("The source image could not be decoded", T1005), so it may be the same session.

## Work
1. Read the kind's stack from the worker (`GET /crashes`, read-only) and map it with the 0.8.5 source maps (T1008).
2. Say whether 0.9.0 already fixes it; if not, fix it or class it (`expected` / `network`) with the reason.
3. Write the verdict into T1005's table so the kind is cited.

## Acceptance
the kind is cited by a ticket with a verdict; a fix has a test that fails without it.

Severity: medium (unknown until the stack is read). Release: 0.9.2 (0.9.1 if the stack shows a defect still in 0.9.0). Model: Sonnet. Size: S. Owner: no.
