# T1085 — Ice fishing says "terminal attractor" where the Gear tab says "hook"; the page shows neither word consistently

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1085 | Ice fishing says "terminal attractor" where the Gear tab says "hook" | OPEN — 0.10.1 | | 0.9.1 chat 2026-11-24 |

Source: the 0.9.1 chat, 2026-11-24, PR #130 (T1050): clause `zp:allowable_methods.r2b`.

## Work
1. Say what the printed rule calls it and what the app's Gear tab calls it; pick one plain word for the page, with the book's term in the source line.
2. Apply it wherever the clause is drawn; state the count of waters.

## Acceptance
before/after picture; a wording test.

Severity: low (wording). Release: 0.9.2. Model: Sonnet (Opus if the wording comes from the cascade). Size: S. Owner: no.

## 2026-11-26
Built by the hardening chat on a local branch of the main checkout, not pushed: the pre-push hook refused it because
`pipeline/deliver/answers/gear.py` changed and the mini release was not re-cut. Next: the re-cut under the lock (the
owner's yes below), push, review.

## Owner

- [x] 2026-11-26 — A build chat runs `python -m pipeline.tools.mini_release --write` (the fixture re-cut) once per branch, under the lock → "Yes" (2026-11-26; T1085, T1096, T1168)
