# T993 — Make the release run smoother: find every problem before `pipeline release`, not one per run

Status: POSTPONED — owner 2026-11-21: a future ticket, not 0.8.5.

Source: owner 2026-11-21, during the 0.8.5 staging release, verbatim: "we need to have a ticket to make the review
process smoother we always have so many issues we need to fix...".

## Evidence (0.8.5 staging, 2026-11-21): nine stops, each found only by the run before it
1. Fresh copy + fast-fail: the committed mini release had been cut before the T686 switch flipped.
2. Size gate: `water` was over its gate by 85 KB (T985 #2 mouth place). Found after a 25-minute build.
3. App-version gate: `--min-app-version 0.8.4` was not passed, though T686 always needed it.
4. CI red: the feeds worker test pinned the old band words. `pnpm check` doesn't run the worker suite.
5. The staging site was app 0.8.3, under the release's minimum app.
6. Fast-fail: a step I added handed worker files to the app's vitest.
7. Binding diff: Lost Lagoon +1, expected.
8. Golden pages: 9 changed, all renumbered source indexes.
9. Release-backed tests: 7 water-map tests were pinned to the old release's missing mouth place.

## Work (to scope when picked up)
- **One rehearsal that lists EVERYTHING at once**, run before the real release. `--dry-run --rehearse-follows` exists
  (T680); find out why it was not enough, and make it the step before every release. It should include:
  - the candidate build and its size gates;
  - the app/site version gates, with the minimum app derived from the formats (no flag to remember);
  - CI parity: the worker suite, as fast-fail now does;
  - the binding diff;
  - the fixture re-cut;
  - the release-backed tests.
- **Lanes test against the next release's shape.** A lane that changes a format or adds a column re-cuts the mini
  release and runs the release-backed tests on a candidate, so its tests don't pin the old release (stops 1, 2, 9).
- **Auto-accept what is safe.** A golden re-cut that only renumbers ids/indexes, and a binding diff that only names
  entries the release adds on purpose, pass with a printed note (see T992).
- **Release prep checklist** for the release ticket: flip switches, re-cut the mini release, staging site at the new
  app version, the gate budget for each new column.

Depends: T992 (the explanation) helps. Size: M. Owner: no.
