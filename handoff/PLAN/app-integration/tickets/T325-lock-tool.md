# T325 — Write-through: reviewed marks → one final curated file per domain (was: lock tool)

Status: IN REVIEW (2026-10-30, write-through) — was OPEN (owner 2026-10-30; T324 design approved, plan in handoff/PLAN/data-flow.md §4–5).

`python -m pipeline.regs.review.lock <domain>`: refuses until the domain's review is finished; writes the final file
(no review fields, schema in data-flow.md §4, read through `CURATED`); proves a side build is byte-identical; then CLOSES
the domain. Owner chose option (a): marks.jsonl is never edited — a closed-domains list
(`data/curated/review/closed.json`) that the exporter and review app skip (tab shows "locked"). Reopen = remove the
entry. Tests: refuse-when-open, byte-identical proof, closed domain skipped. Depends: T266. Size: M.

## Owner correction 2026-10-30 → WRITE-THROUGH (supersedes the lock below)
"I don't want to have to individually lock everything. It should just work naturally from marks, and when reviewing
items it should take my reviews and add to the curated items." Done:
- After the backend appends a mark (single or batch — one request), it runs THAT domain's apply
  (`curation-review/backend/write_through.py` → `review.apply.run`): sloughs + names in the request; dfo / entries /
  tidal / splits in the background, debounced 1.5 s (binding check ~20 s cold). `GET /api/review/write-through` says
  per domain and per item: saving / saved / refused / waiting (a cut waiting on its side rebuild), and why.
- Refused apply: the mark stays; reason in the response or on the item; amber "not in the curated file" list
  (`CuratedSaveBanner.tsx`, `CuratedTag` on the verdict bar); logged to `data/generated/review/write-through.jsonl`.
  All writes temp + rename.
- On only over the curated marks store (`CURATION_WRITE_THROUGH=1/0` overrides) — no test writes curated files.
- `lock.py` deleted; "locked" badge/notice → "closed" (ponds only: feeds nothing); a finished queue's badge says
  "done". closed.json keeps ponds only.
- Fixed on the way: `apply tidal` landed nothing — tidal (and new-split) proposals' marks carried no scope, so
  `--scope` dropped them (proposals.py now stamps `scope` on each mark; digests unchanged).
- Tidal: still on `apply_proposals --scope tidal` (decided/tidal.json + splits.json); T326 owes `waters/tidal.json`.
  A tidal cut above the mouth waits for its side rebuild, as before (listed on the item).
- Tests: `pipeline/tests/test_review_write_through.py` (12): per domain (sloughs, names, dfo, entries, tidal),
  refused keeps the mark + reason + log, batch writes once, atomic write, off over a temp store, closed ponds.
  Also run: review_export/loop/marks/proposals/tidal_proposals, slough_fold, build_command, name_sources_t224,
  curation_review_{q154,writes,slough,api,safety} — 258 pass; frontend `tsc` clean.

## Done (2026-10-30, superseded by write-through above: lock removed)
- **One command for every open domain** (owner requirement 2026-10-30): `python -m pipeline.regs.review.apply
  [domain …] [--dry-run]` (`pipeline/regs/review/apply.py`) = export, then the domain's writer — `apply_proposals
  --scope …` for tidal / dfo / entries / splits (new `--scope` filter, every guard kept), `tools.slough_calls`, and
  `tools.mined_names`. Marking is unchanged. The review app prints the command (`/api/mode` `commands.apply`).
  Documented in data-flow.md §4.
- **Closed list** `data/curated/regulations/review/closed.json` (`CURATED.regulations.review.closed`, beside the marks;
  `pipeline/regs/review/closed.py`). marks.jsonl is never edited. The exporter skips a closed domain and removes its
  leftover `decided/` file; `--check` flags a leftover; `load` does not ask for it; ranks are unchanged, so closing one
  domain moves no byte of another. `apply` skips it. Not a build input (`build.NOT_BUILD_INPUTS`).
- **Lock** `python -m pipeline.regs.review.lock <domain>` (`--dry-run`, `--reopen`, `--status`). It refuses on: an
  unsure/wrong mark in force, the review app's unmarked queue count (ponds / sloughs / tidal / dfo; others need
  `--queue-checked`), the domain's decided file behind the marks, or a proposal still waiting. It writes the final file
  through the same writer as `apply`. It PRINTS the heavy proof (`scripts/heavy.sh … sandbox-exec … pipeline build
  --side …` with marks, closed.json and decided/ unreadable, then a `diff -rq` against the live release). Then it
  closes the domain. Tidal is refused until T326.
- **Review app**: `GET /api/review/closed`. The queue, progress and detail endpoints of a closed domain answer 423.
  A mark on its scopes gets 423. The tab and sub-tab show "locked", and the view shows a Locked notice
  (`frontend/src/components/Locked.tsx`).
- Tests (`pipeline/tests/test_review_lock.py`): refuse while open, a closed domain skipped by the exporter, apply and
  app API, reopen restores it, closing moves no other byte, and a new DFO extent mark reaches its DFO file through
  `apply dfo` (temp git repo). Also run: review_export, review_loop, review_proposals, slough_fold, build_command,
  name_sources_t224, curation_review_{q154,writes}, review_marks. All pass; frontend `tsc` clean.
- Not run (heavy, human): the byte-identical side build. The queued batch rebuild can serve as the proof for ponds.
