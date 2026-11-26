# T1156 — One copy of the synopsis book; the region PDFs get a fetch command with checksums; a guard against a second copy

Status: PARTIAL — the guard and the data docs are done; left: `fresh-machine.md` lines 54 and 109 (the repo-move chat's page)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1156 | One copy of the synopsis book; the region PDFs get a fetch command with checksums; a guard against a second copy | PARTIAL — guard + data docs done | 11ea55132, 8635bebd6, 7516341b7 | `fresh-machine.md` lines 54 and 109 still call the region PDFs read by "quota print checks"; the repo-move chat owns that page: say they are not kept and nothing reads them |

Source: the read-only synopsis spot-check of 2026-11-25 (planning chat's night agents): the repo's main checkout
held two copies of the book. `data/source/fishing_synopsis.pdf` (tracked in git; read by the pipeline, the review
tool and the tests) carries the red in-season corrections; `data/source/official/2025-2027_freshwater_fishing_regulations_synopsis.pdf`
(git-ignored, downloaded 2026-09-21, read by nothing) did not, so a check against it shows false differences
(Cowichan, Fraser, Peace, Kootenay). Owner 2026-11-25: "how do we fix this", then, to the three steps below, "you
can do the 3 steps". Also his earlier ask of the same day: "fix this too (data/source/official/ has no fetch
command.)". Related: T1120 (fresh clone), T1157 (the book on gov.bc.ca has changed), the memory note
`synopsis-pdf-not-byte-identical`.

## Work
1. DONE 2026-11-25 21:43 (planning chat, on the owner's word, the one file only): the stale full-book copy in
   `data/source/official/` is deleted from the main checkout. It was git-ignored, so there is no commit.
2. The nine region PDFs in `data/source/official/` (`region_1_vancouver_island.pdf` … `region_8_okanagan.pdf`,
   about 7 MB): a fetch command (in `data/fetch_data.py` or beside it) that downloads them from
   `https://www2.gov.bc.ca/assets/gov/sports-recreation-arts-and-culture/outdoor-recreation/fishing-and-hunting/freshwater-fishing/<name>.pdf`
   and checks each against a small tracked pin file (name, sha256, bytes, date fetched). Measured 2026-11-25 21:44:
   all nine local files are byte-identical to what gov.bc.ca serves that day (sha256 prefixes: R1 74b37288,
   R2 4c2a8b84, R3 ec296644, R4 ac221655, R5 a8d44ca1, R6 ab228eef, R7A fb217c1d, R7B 2b2a6ddf, R8 6bb89989).
   A changed checksum is a shown stop that names the file (the province republished it: T1157's kind of event),
   never a silent re-pin. First say what reads them: `fresh-machine.md` line 57 says the quota print checks do and
   line 141 says nothing does; a grep of the code on 2026-11-25 found no reader. If nothing reads them, the owner
   picks: keep them as pinned reference copies, or delete the folder.
3. A guard (in the data path gate or `check-size.mjs`'s family): fails if a second full synopsis PDF exists
   anywhere under `data/source/` besides `fishing_synopsis.pdf` and the dated archive copies the annual runbook
   makes on purpose (`fishing_synopsis.<edition>.pdf`). `fresh-machine.md` and `data/README` say in one line: the
   book is `data/source/fishing_synopsis.pdf`; validate against nothing else.

## Acceptance
A fresh clone gets the region PDFs with one command and the checksums match; the guard fails when a copy of the
book is dropped into `data/source/official/` (mutation); the two doc lines agree with the code. Severity: low
(a trap for checks, not for users). Model: Sonnet. Size: S. Owner: the pick in step 2 if nothing reads them.

## Owner

- [x] 2026-11-25 — The stale second copy of the book: how to fix → "you can do the 3 steps" (2026-11-25)
- [x] 2026-11-25 — Keep or delete the nine region PDFs (`data/source/official/`)? Evidence: nothing reads them (no code, test, tool or review-app reader; `quota_print` was deleted in 759bdebbd) → "B: delete the folder" (2026-11-26)

## Done (lane 2026-11-26)
- Built first: a fetch command with pinned checksums for the nine (11ea55132, hardened in 8635bebd6; a real fetch matched
  all nine pins). On the owner's pick "B: delete the folder" both were REMOVED (7516341b7): `data/fetch_region_pdfs.py`,
  its pins and its tests. The planning chat deletes `data/source/official/` from the owner's main checkout (it is
  git-ignored: nothing to delete in git). The `source/official/` ignore rule stays, so a leftover folder never reaches a
  `git add`.
- Guard: `python -m pipeline.tools.one_book` (`pipeline/tools/one_book.py`), run in CI by `pipeline/tests/test_one_book.py`.
  A full book = named like it, or 60+ pages at 5 MiB+, or 20+ pages with the synopsis cover; nothing under 1 MiB is
  opened; CI checks the tracked files; passes where `official/` is absent. A region chapter (4-9 pages) is not a book.
- Docs: `data/README.md`, `data/.gitignore` comment, `pipeline/tools/README.md`.
- Left: `fresh-machine.md` lines 54 and 109 (repo-move chat). Also `T1166` line 19 says "the nine region PDFs are checked the
  same way (T1156's pins)": the pins no longer exist, so T1166's planning needs that sentence changed.
