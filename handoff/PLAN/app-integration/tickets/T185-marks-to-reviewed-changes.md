# T185 — From review marks to catalogue changes: the marks are the decisions, Claude applies them

Status: DONE (e32eb08e) — owner decides the 8 proposals and applies them. Owner 2026-10-24: "we want to avoid making the review curation tool update files directly anyways. must
be a better process to get the manual results in to entries etc."

## The process
1. Review (T184 artifact, anyone): writes MARKS only.
2. Tickets: `python -m pipeline.regs.review.tickets` turns every wrong / unsure mark into a ticket (exists today).
3. Proposals (Claude): per ticket, a concrete edit to the curated entry / split (data/curated/...), applied on a side
   branch; side rebuild → binding diff + card gate → a proposal record: the ticket's note, entry before/after, map
   effect, each rule's bound-length delta.
4. Change review (artifact, like the braid page): one card per proposal, accept / reject / note → read back.
5. Apply: accepted proposals land as one commit per ticket (`git commit -- <paths>`), then the normal rebuild; the
   changed target re-enters the queue as a "verify" check (its fingerprint moved, T97).
- The local curation tool's FIX MODE loses its writers (entry/split edit, rename, /api/check-driven writes): the screen
  becomes read-only everywhere; a pin test that no curation-review route writes under data/curated.

Depends: T184 (marks from the artifact). Size: M (proposal builder + change-review page + retiring the writers).

## Outcome — the writers part (T184 lane, 2026-10-24)

DONE: "The local curation tool's FIX MODE loses its writers". There is no fix mode any more (not a hidden flag):
the entry / split edit, add, move, delete and rename routes and functions, the `/api/check` draft check, the attach
flow and every editor component are removed (backend `d959855b`, screen `23dfc3ca`); `run.sh` has no `--fix`. The app
serves reads and ONE write, `POST /api/marks`. Pins: `test_curation_review_safety` — exactly that one write route
whatever `CURATION_FIX` says, and no backend module but `writes.py` writes a file (its one caller is `post_mark`'s
commit of the marks store), each with a mutation; `test_curation_review_screen` calls every route and proves nothing
under `data/curated` changed. A ticket's work line now reads "a reviewed change to the curated files (T185)". Still
open here: steps 3–5 (the proposal builder, the change-review page, apply).

## Outcome — steps 3–5 (2026-10-27, e32eb08e)

Marks -> proposals -> owner decides (a mark) -> owner applies, one commit each. Nothing but the owner's
`apply_proposals` writes a curated file; the one-write pins are unchanged and green.

1. **Proposals** — `python -m pipeline.regs.review.proposals build [--check]` (`pipeline/regs/review/proposals.py`)
   reads the marks in force: a mark's `extent_fix` (T191) and `tributaries` calls (T189) are ONE proposal (catalogue
   entry / rule, or the DFO location's `binding`); each new split (T164) is ONE proposal per distinct cut across all
   marks (`marks.proposals`, T210), and a mark naming it `depends_on` it; `lake_split_later` is listed with no edit.
   Each holds structured edits (file, entry / rule / location, field, before, after) and a digest of them; a file is
   re-spelled only where it changes (splits.json's one-line anchors kept). Output:
   `handoff/PLAN/curation-review/proposals/index.json` + one `.patch` each. A new split's anchor comes from the
   registry (confluence by the tributary's WSC, lake inlet/outlet by wbk + edge), else the stamped point.
2. **Cheap check** (`--check`): `reuse.binding_change` / `dfo_binding_change` bind every rule in memory before and
   after (the T191 preview's `build_reach` path): sections and km, gained / lost, outcome; severity from the queue. A
   new split, or a proposal naming one, says "side rebuild". Each proposal prints its side chain:
   `proposals side <id> --out <side>` (patched copies; refuses data/curated) -> side atlas when splits change ->
   reach on the side copies -> `binding_diff` live vs side.
3. **Proposals view** (review app, `#proposals`, `ProposalsView.tsx`; `GET /api/proposals`): per proposal the edits,
   the marks behind it, the check, the side commands; accept / reject with a note. A decision is a mark (`marks.py`
   additive: scope `proposal`, P y accept / P n reject, held against the proposal's digest; `marks.decisions`)
   through `POST /api/marks`; never a ticket; stale once the proposal changes.
4. **Apply** — `python -m pipeline.regs.review.apply_proposals [--dry-run]` (the owner runs it): rebuilds the
   proposals from the files NOW and lands each accepted one whose digest still holds, new splits first, as its own
   `git commit -- <paths>`; refuses a stale decision, a waiting dependency, or a file with uncommitted changes (files
   put back if the commit fails).

First run on the owner's marks (13 lines): 8 proposals — 2 new splits (Quinsam confluence, shared by two Campbell
locations; Cowichan Sandy Pool sign), Hatzic r2/r3 tributaries in (in memory: 4 -> 92 sections, 8 -> 124 km each),
4 DFO extents (Campbell Maple St -> cement block binds 1 section; 3 wait on a new split), Dragon Lake listed.

Checks: `test_review_proposals.py` (one patch per mark / per cut, only the marked lines change, building writes no
curated byte, a decision is a mark and never a ticket, stale / waiting decisions refused, apply in a temp repo = one
commit each, the app reads and records a decision) plus the safety / marks / queue / gate / DFO suites; frontend
`tsc`. `test_curation_review_screen.py` needs a built atlas (none in the worktree): run it at merge.

## Outcome — the marks ARE the decisions (owner 2026-10-27, decisions.md "after the scheduled run")

The accept / reject step is gone. Steps 4–5 above are replaced:

- **No `proposal` mark.** `marks.Scope` no longer has it; `marks.decisions` is removed; `load` skips a retired
  `proposal` line (`RETIRED_SCOPES` — the store is append-only, so the 7 old decision lines stay as history);
  `POST /api/marks` refuses scope `proposal` (422, "the marks behind it are the decision"). Gate / tickets lost
  their `proposal` branches. The one-write pins are unchanged.
- **Proposals tab** (`ProposalsView.tsx`, `GET /api/proposals`) is a read-only "waiting to apply" list: edits, the
  marks behind each, confidence, the cheap check, side commands, and where it stands (waiting / after a new split /
  not applicable). To change a proposal, change the mark.
- **`apply_proposals` (Claude runs it)** lands every applicable proposal of the marks in force, one
  `git commit -- <paths>` each, new splits first: the in-memory binding check runs first (printed and carried in
  the commit message; a failed check refuses it). A proposal that adds or moves a cut — a split edit, a new split
  it waits on, or a split it names that landed in this run — needs its SIDE REBUILD first: the side atlas at
  `proposals.side_dir` (named by id + digest; `side_commands` print the chain), or `--side-checked ID`. `--dry-run`
  and `--only ID` kept / added.
- **Confidence on every proposed split**: `NewSplit.confidence` (the DFO new-split picker sets it; default medium
  for a hand-placed point, high for a registry anchor — `marks.default_confidence`); owner overrides in
  `data/curated/regulations/review/proposal_overrides.json` (`CURATED.regulations.review.proposal_overrides`,
  matched by `same_anchor`), since marks.jsonl is never edited. It shows in `marks.proposals`, the proposal row and
  its split record (`confidence`, absent = high), the Proposals list, the DFO "already proposed" list, DFO
  suggestions (each suggested new split carries its point's grade) and the tickets. **Sandy Pool = medium** via
  that override file.

Checks: `test_review_proposals.py` rewritten (confidence + override + Sandy Pool, retired line skipped and refused,
side-less / waiting proposals held, apply in a temp repo = one commit each with the check in the body, dry run
writes nothing, the app lists read-only and refuses a decision); frontend `tsc`.
