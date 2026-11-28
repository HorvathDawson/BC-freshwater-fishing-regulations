# T1232 — The review tool shows nothing when an input file is missing

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1232 | The review tool shows nothing when an input file is missing | OPEN | — | a missing input is a 503 naming the file and its command; marks health says "input missing"; what a closed domain keeps is T1233's |

Source: repo clean chat, 2026-11-27 (scoping note of 2026-11-26 and the review of PR #366). The backend swallows a
missing generated input and renders an empty list, e.g. `tools/curation-review/backend/awaiting_water.py:36` and
`dfo.py:128` (`except (OSError, ValueError)` then empty), `cascade.py:176` (`if not src.exists(): return {}`). The 503
handler exists (`app.py:35`, T434) but only for a `FileNotFoundError` that reaches it. And
`pipeline/regs/review/marks.py:1172-1182` (`health`) counts a mark as ORPHANED when its target cannot be read, even
for a closed domain: deleting the closed Q154 domain's sample would have reported all 80 of the owner's Q154 marks as
orphans. The notes also found three more silent sites: `pipeline/regs/review/tidal.py:409-412` (a missing pass-2
file is read as "no pass 2"), `tools/curation-review/backend/preverify.py:41-46` (a missing pre-verify or split-suggestion
file becomes `{}`, no badge) and `backend/dfo.py:1077` (`return None` when the closure file is missing).

## Work
1. A missing input is a 503 naming the file and the command that re-makes it (T434's handler); remove the swallowing sites.
2. The marks health view says "input missing" rather than "orphaned".
3. Decide with the owner what a CLOSED domain's marks need kept on disk.

## Acceptance
Delete one generated input: the screen says which and how to re-make it; no mark is counted orphan for it.

Depends: none. Size: M. Owner: yes (what a closed domain keeps). Default: its marks and the one input they were
checked against stay committed; health reads "input missing" for the rest.

## Owner

- [x] 2026-11-27 — What must a CLOSED review domain (Q154: 80 marks) keep on disk so its marks are never read as
  orphans? Default: the marks plus the one input file they were checked against, committed. → superseded by T1233:
  "A: a "review evidence" folder (Recommended)" (2026-11-27): every file a mark's check reads moves to `data/review/<domain>/`, q154 included.
- [x] 2026-11-27 — Which order: "C: Safe order (Recommended)" = the loud refusal first; then ONLY the DFO closure entries
  (`dfo_closure_entries.json`) and the tidal pre-verify (`tidal/tidal-preverify.json`) leave git; the tidal pass-2
  proposals stay for good (5 tidal marks point at rows that exist only there); the entry split suggestions and the DFO
  pre-verify stay until their domains close or a re-make can run. Consequence: the refusal lands before any file leaves git.
