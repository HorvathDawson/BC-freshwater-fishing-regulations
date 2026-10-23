# T132 — Curation review: a salmon (DFO) pass in the review tool

Status: OPEN — later; after T97–T136 (the first review pass) is running.

Source: owner 2026-10-23, approving the curation review plan: "We could also maybe make it do salmon review. But for now
it seems good." Q36 (default kept): DFO salmon entries, overrides, steelhead waters, cross-references and name variants
are NOT in the first pass.

## Work
- A second queue in the review tool for the DFO salmon entries (`source: "dfo"`, memory `dfo-override-tag`), with the same
  T/R/P/A checklist and marks store (T97); its evidence is the DFO notice text instead of the synopsis row.
- DFO churn (memory `dfo-salmon-churn-asymmetry`): waters never move, rules turn over ~50 %/yr. So the place (P) mark
  should last across re-scrapes while rule (R) marks go stale on each re-scrape — design the fingerprints for that.
- Decide whether overrides.json / steelhead_waters / cross_references get their own pass here too (Q36 follow-up).

## Acceptance
A reviewer can work the salmon queue end to end; a re-scrape that changes only rules stales only R/A marks.

Depends: T97 (marks), T134 (review screen). Size: M. Owner: scope call when it starts.
