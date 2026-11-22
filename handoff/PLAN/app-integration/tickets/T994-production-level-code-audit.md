# T994 — Production-level audit: non-standard practices and data-flow issues across the code

Status: IN PROGRESS (main chat 2026-11-21: read-only audit agent first; findings become numbered lines here or new tickets).

Source: owner 2026-11-21, verbatim: "You could also get an agent to do a basic audit for non standard practices or
issues with the flow of data etc. we want to make our code production level. Also clean up gha workflows and make sure
we just waste less time with bad ci/cd of failed runs." (The GitHub Actions half is in T993's scope.)

## Work
1. Read-only audit of `pipeline/`, `app/`, `curation-review/`, `deploy/`, `scripts/`: non-standard practices
   (error handling, config, typing, logging, dependency use, dead code, copy-pasted logic), and data-flow issues
   (a fact computed twice, a reader of curated files at run time, a hidden fallback, an untyped boundary).
   Start from the 2026-11-16 audit (`handoff/PLAN/audit-2026-11-16/`) and report only what it and the open tickets
   do not already hold.
2. Each finding: file:line, what is wrong, why it matters in production, the fix, its size. Ranked.
3. The owner sees the list; fixes go into this chat's lanes or new tickets.

## Acceptance
A ranked findings list with no item already in an open ticket; each kept finding fixed or ticketed.

Depends: —. Size: M. Owner: reads the ranked list.
