# T525 — The DFO review screen is easy to follow and hard to get wrong

Status: OPEN

Owner 2026-11-09: "make app easier to review too right now the info is really hard to follow and extents already
defined get lost when confirming vs redo. etc etc it is just hard to use right now and feels like i will make bad data".

## What
- Audit the DFO review flow end to end (curation-review/frontend DfoReview, ExtentFixEditor, useMarking; backend dfo.py)
  as the owner uses it: what a place shows, what each answer (confirm / redo / fix / unsure) does to the existing
  curated extent, where the current extent disappears or is silently replaced.
- One clear layout: DFO's words → the CURRENT curated extent (words + map colour) → what this mark will do to it
  (keep / replace / add a piece) → save. Confirming never loses the existing extent; redoing starts FROM it.
- Every save states its effect before it is made; nothing is changed without the owner seeing old vs new.
- Builds on T524 (pieces, tributary default from the title, "replaces extent" banner); lands after it in the same files.
- Proof: a walkthrough with shots of 4 real places (whole water, between two cuts, two-river T524 case, a landed fix).
