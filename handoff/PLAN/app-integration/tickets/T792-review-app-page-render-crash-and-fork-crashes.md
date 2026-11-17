# T792 — Two crashes: the review app's page renderer frees PDF objects outside its lock; child processes die after fork where a test process has used the map-projection library

Status: OPEN (review app backend; test runner).

Source: general audit 2026-11-16, guards checker section 5 (`handoff/PLAN/audit-2026-11-16/guards.md`); T777 described only the second, and its proposed fix would not help.

## A. The PDF library called outside its lock (stopped a release attempt; can crash the review app)
`curation-review/backend/synopsis_pages.py` `page_png` and `evidence.py` `_render` hold `PDFIUM_LOCK` around
`doc[n - 1].render(scale=…).to_pil()` and `doc.close()`, but never close the page or the bitmap. Every pypdfium2
object refers to itself, so those temporaries are freed only by the cyclic garbage collector, in whichever thread it
runs, without the lock (crash report `python3.13-2026-11-16-024652.ips`: `gc_collect_main` → `CPDF_Page::~CPDF_Page`
while another thread was in `FPDF_LoadDocument`). The review app serves these routes from a thread pool.
Fix: inside the lock keep `page` and `bitmap` in names, copy the pixels out, then `bitmap.close()`, `page.close()`,
`doc.close()`. Test: `test_review_screen.py::test_pages_render_from_many_threads_at_once` many times over.

## B. A child dies between fork and exec
Six crash reports, one chain: `subprocess_fork_exec` → `fork` → PROJ's after-fork handler closing its cached
`proj.db` handle → Apple's SQLite logging → SIGSEGV. Ordinary `subprocess` calls (git, node, helper processes) in a
test process where an earlier test file used pyproj. "Use spawn" does not help (spawn goes through the same call).
Fix: in `scripts/pytest-chunked.sh` a test file that starts a child process gets a process of its own (34 files
match `subprocess|multiprocessing|Popen`; under a minute). Never group such files by hand with atlas-loading files.
