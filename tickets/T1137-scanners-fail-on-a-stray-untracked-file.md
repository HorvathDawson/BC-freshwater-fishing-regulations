# T1137 — Two pipeline scanners fail a push from the main checkout on a stray untracked file

Status: OPEN — 0.10.1

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1137 | Two pipeline scanners fail a push from the main checkout on a stray untracked file | OPEN — 0.10.1 | | the scanners read tracked files only, with a test (the stray files are answered and deleted) |

Source: the 0.10.0 chat, 2026-11-25 14:50. Related: T1021 (the hard-coded-path gate), T1127 (the same fault in the
store scripts: a tool reads whatever is in the folder).

A stray file `data/generated/owner1028/one.py` in the main checkout (untracked) makes two pipeline scanners fail
any push from there; the 0.10.0 chat pushed its release commits from a scratch worktree to get round it. Nobody has
touched the file.

Work: (1) the scanners read the files git tracks (or that are staged), never an untracked or ignored one; a test
with a stray file; (2) the owner says whether `owner1028/one.py` is his and still needed (it is in his checkout:
no chat deletes it).

Acceptance: a push from a checkout holding an untracked `.py` under `data/generated/` passes the scanners.
Severity: low. Model: Sonnet. Size: S. Owner: yes (one answer on the file).

## Owner

- [x] 2026-11-25 — `data/generated/owner1028/one.py` sits untracked in the main checkout and makes two scanners fail pushes from there: is it yours and still needed? No chat deletes it. → "Not mine, delete it" (2026-11-25 17:39). Two more there (`rows_ab.py`, `two.py`): "(a) Delete both (Recommended)" (2026-11-27); deleted from the main checkout 2026-11-27, copies kept in the planning chat's night notes.
