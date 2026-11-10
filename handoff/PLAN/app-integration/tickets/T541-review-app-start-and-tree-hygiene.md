# T541 — Review app: run.sh proves its backend started; review outputs stop dirtying the tree

Status: OPEN.

Source: workflow review 2026-11-09 (verified against the code). Rank 9.

## Today
- `curation-review/run.sh:24-27` starts the backend in the background and goes straight on to Vite (`:30-33`). If port
  8787 is held by another copy, the new backend dies with "address already in use" (scrolled away by Vite's output) and
  the owner's new frontend talks to the OLD backend process: its code, its marks store, its write-through setting
  (`frontend/vite.config.ts:8,23-27`). The standing workaround is the paragraph at the top of STATUS.md. No recorded
  incident.
- The tidal / sloughs / names writers leave `data/curated` files uncommitted (`pipeline/regs/review/apply.py:113-127`
  prints "commit it"); release preflight stops on any dirty curated path (`pipeline/release.py:108-115,326-331`). The
  proposal domains commit themselves. Not hit yet (those files have not changed since T324).
- Tree is always dirty: two `tsconfig*.tsbuildinfo` files are tracked although ignored (`.gitignore:55,68`);
  `data/generated/review/` is tracked piecemeal while the same generators also write untracked `.md` summaries,
  `pfma_basin_areas.json` and `write-through.jsonl` there. Exposed to another lane's `git add -A`.
- "N review items wait on a rebuild" is known only to the running app (`backend/app.py:523-528` →
  `reuse.pending_build`); nothing server-less reads it, so a session or `release --dry-run` cannot see it.

## Work
1. `run.sh`: refuse when the backend port is already listening (name the PID and the two options); after start, wait for
   `/api/ready` from its own child and exit if it died. `/api/ready` returns start commit, marks path, write-through
   on/off; the header shows them and a banner when HEAD has moved since start.
2. File domains commit like proposal domains (path-scoped; refuse if the file was already dirty); the save reports the commit.
3. `git rm --cached` the two tsbuildinfo files. Per file in `data/generated/review`: committed review input, or ignored
   run log / summary. Remove stale DFO summary copies under `handoff/PLAN/curation-review/`.
4. `python -m pipeline.regs.review.pending` prints what waits on a build; `release --dry-run` shows the count.

## Acceptance
- Starting a second copy on a taken port stops with the message; `git status` is clean after a review save + app build.

Depends: —. Size: M. Owner: no.
