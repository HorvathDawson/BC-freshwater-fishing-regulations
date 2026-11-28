# T1230 — constraints.txt pins pymupdf but none of the packages it pulls in

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1230 | constraints.txt pins pymupdf but none of the packages it pulls in | OPEN | — | pin each at the installed version; a check that every package pip would install is in constraints.txt |

Source: repo clean chat, 2026-11-27. `constraints.txt:62` pins `pymupdf==2.2` (`requirements.txt:10` lists it bare).
Installing it into a venv added flatbuffers, onnxruntime, psutil, pymupdf4llm, pymupdf_layout and tabulate at whatever
version resolved that day (planning chat, 2026-11-27 06:00). Two machines or two days can resolve different versions,
against the pin-everything rule in `requirements.txt` / `constraints.txt`. The only check today is
`pipeline/tests/test_curation_review_deps_t994.py:55`, which covers the review backend's imports, not the closure.

## Work
1. Pin each of the six at the installed version (read from the installed METADATA).
2. A check that every package pip would install for `requirements.txt` is in `constraints.txt` (a dry-run resolve, or
   `pip install --dry-run --report`).

## Acceptance
A fresh venv from the two files installs only pinned versions; the check fails when a pin is removed.

Depends: none. Size: S. Owner: no.
