# T1111 — The More page refreshes by pulling down, like the other pages

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1111 | The More page refreshes by pulling down, like the other pages | OPEN — 0.9.2 (proposed) | | owner 2026-11-25 |

Source: owner 2026-11-25 ~00:55 (planning chat), verbatim: "more page should have refresh by swipping down standard."

## Work
1. Pull-to-refresh on the More page with the same control and the same action as the other pages (check for a
   newer data release and the feeds; one refresh path, `one-load-path-per-screen`), with the same shown result
   ("Up to date" / what was updated / the failure, reported).
2. List every other scrolling page that lacks it and add it or say why not (a page with nothing to refresh).
3. Component test for the gesture's action on More; emulator check.

## Acceptance
Pulling down on More runs the same refresh as elsewhere and shows its result; the list of pages is in the PR.

Severity: low. Release: 0.9.2 (the planning chat's proposal). Model: Sonnet. Size: S. Owner: no.
