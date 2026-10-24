# T184 — The curation review as a shareable, phone-friendly artifact

Status: OPEN. Owner 2026-10-24: "make the curation review into an artifact instead so I can work on it remotely (a good
mobile version too) … I could send it to other people to help."

## Shape
- Export, from the SAME backend code (curation-review/backend), each target's review payload once per vintage:
  `/api/entries/{id}/review`, its book crop (page image + rectangle), map geometry, the angler's-page preview
  (`/api/answer`), vocab/problem lists. Book pages uploaded once as assets; crops are rectangles over them.
- Page: one target per screen on a phone (book crop, map, checks with big y / n / ? buttons + note), the laptop keys
  (t r p a, y n ?, Enter, o, m) kept; per-target timer like the local screen (`seconds`).
- Marks in the artifact's db with the viewer's id (`user` capability) → an importer writes them into the repo's mark
  history in the same shape `POST /api/marks` writes (so `gate pilot-report` and `commit_marks` work unchanged).
- Helpers: shared from the Share menu (claude.ai accounts; guests by email). Each mark records its reviewer; the page
  shows who has a target open/marked so two people do not double up.
- Not possible without the live backend: re-binding / searching another water — those become a wrong/unsure mark.

## Phases
1. The 30 pilot targets (handoff/PLAN/curation-review/PILOT.md, list regenerated for the current vintage) → the pilot
   runs on the artifact. 2. The whole queue, in batches per tier, re-exported per vintage.
Acceptance: the pilot report computed from imported artifact marks; a mark round-trip test (artifact doc → repo mark
→ pilot-report) pinned.
