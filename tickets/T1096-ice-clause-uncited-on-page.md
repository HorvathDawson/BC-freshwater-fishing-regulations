# T1096 — Methods: the page's ice-fishing clause carries no citation (the source is known to the pipeline only)

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1096 | Methods: the page's ice-fishing clause carries no citation (the source is in the pipeline only) | OPEN — 0.10.1 | | 0.9.1 chat 2026-11-24, T1050's leftover |

Source: 0.9.1 chat 2026-11-24 22:55, a "left" line of T1050 (PR #130): the ice clause the page prints is built in
`app/packages/ui/src/methods.ts` (`waysVM`, about line 96) with no Source line; the citation for the ice rule lives
in the pipeline only, so the reader cannot open where the words come from.

## Work
1. The pipeline writes the ice rule's citation into the water's method facts (the same shape other method lines use).
2. `waysVM` shows it as the clause's Source line, and the "All sources" sheet (T1036) lists it.
3. Test: a water with an ice clause has a source that opens; a clause with no citation fails the build check that
   already guards other printed rules (name the check in the PR).

## Acceptance
On Dick Lake the ice line's source opens the printed page it comes from. The test fails without the change.

Depends: T1050. Severity: low (the words are right; their source is not shown). Release: 0.9.2 (needs a data
release). Model: Opus (pipeline + page contract). Size: S. Owner: no.

## 2026-11-26
Built by the hardening chat on a local branch of the main checkout, not pushed: the pre-push hook refused it because
`pipeline/deliver/answers/gear.py` changed and the mini release was not re-cut. Next: the re-cut under the lock (the
owner's yes below), push, review.

## Owner

- [x] 2026-11-26 — A build chat runs `python -m pipeline.tools.mini_release --write` (the fixture re-cut) once per branch, under the lock → "Yes" (2026-11-26; T1085, T1096, T1168)
