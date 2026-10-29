# T291 — Test problem reports and crash reports are tagged and ignored; staging reports cleaned up

Status: OPEN (owner 2026-10-29: "can we clean up reports too? can add a test tag so that we can ignore test ones").

## Work
1. A `test` tag on problem reports and crash reports: set by every tool that sends one (testbed scenarios report/crash,
   smoke-staging, bad-net, request-walk, any unit/e2e that posts), and by a dev/staging build's hidden switch if useful.
   The feeds worker stores tagged ones apart (e.g. `private/reports-test/`, crash kinds marked test), never counts them in
   the digest/alerts, and purges them after 7 days.
2. The review tool's reports view hides test ones by default (toggle to show).
3. Clean up staging now: list the existing staging reports + crash kinds, delete the ones that are clearly from tests
   (testbed/smoke ids, the forced T242 check), keep anything that looks real; report counts.
4. A real user can never set the tag (worker ignores it from production builds unless the request carries the run token, or
   the tag is only honoured on staging).

## Acceptance
Tool-sent reports land tagged; digest ignores them; review view hides them; staging cleaned (counts before/after).

Depends: T237, T242. Size: S. Owner: no (worker deploy is the owner's command).
