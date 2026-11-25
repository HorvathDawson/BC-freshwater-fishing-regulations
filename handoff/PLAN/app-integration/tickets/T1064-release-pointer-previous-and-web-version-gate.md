# T1064 — T1016 follow-ups: the release pointer names its previous release; the web applies new data only when the app version can read it

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1064 | T1016 follow-ups: the release pointer names its previous release; the web applies new data only when the app version can read it | OPEN —  | | planning chat 2026-11-24 |

Source: the 0.9.1 chat's finds on T1016 (RESTART-091-chat.md, 2026-11-24).

## Work
1. The release pointer has no `previous` field, so a 404 on the kept previous release is classed as "replaced": a too-early prune would be hidden. Add the field and class a 404 on `previous` as a defect. Since PR #125 (the tab refreshes itself once) such a prune is auto-refreshed under an `expected` report and the reader never sees it (PR #125 review).
2. The web has no app-version gate on the in-session apply of a newer release.

## Acceptance
tests for both; a pruned-too-early previous release reports as a defect.

Severity: medium (a masked failure, `no-silent-fallbacks`). Release: 0.9.2. Model: Opus. Size: S–M. Owner: no.
