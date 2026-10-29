# T290 — Remove the Names tab from the curation review tool

Status: BLOCKED (owner 2026-10-29: "after this we can remove the name tab from review curation tool"; waits on the T224
close-out in the current rebuild — coverage names accepted, obvious gauge names accepted, held gauge names answered).

## Work
Remove the Names tab (frontend route `#names/`, its backend endpoints, tests) from curation-review. Keep the data files and
pipeline (mined names, accept/reject records) — only the review UI goes. Any held gauge names are settled in chat, not the tab.

## Acceptance
Review app builds and runs without the tab; no dead endpoints; tests green.

Depends: T224. Size: S. Owner: no.
