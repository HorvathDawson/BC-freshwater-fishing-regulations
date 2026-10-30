# T290 — Remove the Names and Proposals tabs from the curation review tool

Status: DONE 2026-10-29 (removed: Names/Proposals/Low-cuts tabs + TopNav entries + keys; NamesReview.tsx, ProposalsView.tsx; GET /api/names/{queue,progress,candidates}, GET /api/proposals + reuse.proposals_view; their tests. Kept: variants.py (marks_for backs POST /api/marks scope name), per-entry low-cut panel, all data + pipeline). Was: OPEN (unblocked by owner 2026-10-29 — do it now; owner 2026-10-29: "after this we can remove the name tab from review curation tool"; waits on the T224
close-out in the current rebuild — coverage names accepted, obvious gauge names accepted, held gauge names answered).

## Work
Also the **Low-confidence cuts** tab (owner 2026-10-29). Any open item those tabs held is settled in chat.

Also (owner 2026-10-29: "the review curation tool proposal tab can be removed too"): the read-only Proposals view
(`#proposals`, TopNav entry, its backend endpoint if only the view uses it). The proposals/apply pipeline
(`apply_proposals`) stays — only the view goes.

Remove the Names tab (frontend route `#names/`, its backend endpoints, tests) from curation-review. Keep the data files and
pipeline (mined names, accept/reject records) — only the review UI goes. Any held gauge names are settled in chat, not the tab.

## Acceptance
Review app builds and runs without the tab; no dead endpoints; tests green.

Depends: T224. Size: S. Owner: no.
