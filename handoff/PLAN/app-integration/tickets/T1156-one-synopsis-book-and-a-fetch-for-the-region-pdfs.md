# T1156 — One copy of the synopsis book; the region PDFs get a fetch command with checksums; a guard against a second copy

Status: IN REVIEW — owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1156 | One copy of the synopsis book; the region PDFs get a fetch command with checksums; a guard against a second copy | IN REVIEW — owner: see ## Owner | 11ea55132, 68d3c7e4a | the owner's pick: keep the nine as pinned reference copies (default) or delete the folder |

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
- [ ] 2026-11-25 — Nothing reads the nine region PDFs (lane 2026-11-26: no code, test, tool or review-app reader; `quota_print` was deleted in 759bdebbd, only a stale `data/.gitignore` comment named it, now fixed). Keep them as pinned reference copies (default, the fetch and pins are built) or delete the folder.

## Done (lane 2026-11-26)
- Fetch: `python -m data.fetch_region_pdfs` (`data/fetch_region_pdfs.py`), pins `data/source/region_pdfs.pins.json`
  (sha256, bytes, date_fetched per file; extra keys kept by a re-pin). Real fetch into a scratch folder: all nine matched.
- Guard: `python -m pipeline.tools.one_book`, run in CI by `pipeline/tests/test_t1156_region_pdfs_and_one_book.py`.
  A full book = named like it (synopsis/regulations) or 20+ pages with the synopsis cover; passes where `official/` is absent.
- Docs: `data/README.md`, `fresh-machine.md` (lines 54 and 109 corrected), `data/.gitignore` comment.
