# After chat 10: start here

Written 2026-11-10. It replaces `NEXT-SESSION-after-chat9.md`.

## Paste this to start the chat
> Read `handoff/PLAN/START-HERE.md`, the restart page it names, and the STATUS.md top entries. Work the ticket list on
> that page in order, one agent at a time. Start ticket work without asking; ask me only for decisions. Keep replies
> short and STATUS.md current.

## What this chat is for
Two workflow reviews (2026-11-09) filed tickets T533–T567; the owner's phone look added T568–T570. The owner asked for
this page on 2026-11-10 so a new chat works through them. That is the go for the ticket lanes.

## State
- **Branch `redesign/stream-sections`, nothing pushed** since 2d539b988's parents; `main` untouched. LATEST =
  r-a178299f6005 on staging (min app 0.7.0). Build 22 (0.7.0) on Play internal.
- **Review app (T532) is the owner's own job right now**: he is labelling DFO extents in it. Chat 10 did nine rounds of
  fixes from his use (commits d740b4ff2 … 3da87d77f, plus round 9 if it landed: check `git log -5` and
  `git status -- curation-review pipeline/regs/review` before touching those paths; an agent from chat 10 may have left
  uncommitted work there if that chat was closed early).
- **Not ours, leave uncommitted unless committing marks as-is:** `data/curated/review/marks.jsonl` (owner's saves),
  `data/generated/review/dfo-suggestions.json`.
- **Known failing test:** `test_review_queue.py::test_no_second_copy_of_the_order…` (two `def on_disk(` in
  `pipeline/regs/review/pending.py`). Item 4 below.

## The list, in order
One agent at a time; batch related tickets into one agent when that saves credits. Model by task (Opus data/pipeline,
Sonnet UI/mechanical, Haiku lookups). Heavy jobs through `scripts/heavy.sh`, one at a time. Each ticket file under
`handoff/PLAN/app-integration/tickets/` is the brief; the README ledger there is the status of record.

0. **The review app is NOT this chat's work** (owner 2026-11-10: "review app will be this chat only", meaning chat 10).
   Do not edit `curation-review/`, `pipeline/regs/review/` or `data/curated/review/`, and do not start a copy on ports
   8787/5173 or 8799/5199. If the owner reports a review app fault here, point him to chat 10. Commit with explicit
   paths; chat 10 commits to the same branch, so `git pull`-style surprises in `git log` are expected.
1. **T568** (gauge map jumps above/below the chart on Flow/Level) **+ T567 open parts** (phone and web differ at Fraser
   at Mission; the forecast day chart keeps today in view): one emulator session.
2. **T569** Report a problem from any screen.
3. **T570** Indigenous land names only at a close zoom (owner picks the zoom from two shots; confirm it means the app map).
4. (Chat 10 keeps these, skip: the failing `test_review_queue`, **T562**, the rest of **T545**, T571, T572.)
5. Data correctness, by rank: **T546** reach run drops nothing silently → **T547** atlas records its curated inputs →
   **T548** in-season notices in force and fresh → **T549** (L) DFO feed and release share one location set →
   **T550** (L) gauge readings age/datum/day → **T551** today rolls over on one BC clock → **T552** run timing and
   forecast floors → **T553** base rules pinned to print.
6. **T554–T561** by rank (gauge alerts + HYDAT vintage, update service, Python/TS parity, one gauge publisher, worker
   state, privacy retention, one home for helpers, static layers + doc drift), then the owner-look tickets
   **T563–T566** (percentile alert follows the day, gauge forecast + chart, in-season notice at top, map switching).
7. Process tickets by rank: **T533–T540, T542–T544** (T541 is done).
8. One APK to Play internal after the app tickets land (patched MapLibre, T530 rides along).

## Waiting on the owner (do not decide for him)
- **T571** Sheldens Creek "Spur 19": no such road in BC road data on Moresby Island; a low-confidence proposal sits at
  the road crossing 130 m above the mouth. He can place the cut himself in the editor.
- **T572** Thompson three year-round closed areas: a four-stretch proposal with five new cuts waits. Juniper upper signs:
  4 km above DFO's coordinate (used) or 1.5 km below Juniper Beach Park. Open faults: the first stretch does not draw
  in the preview (Kamloops Lake cut is mid-lake); closed areas are not shown as closed year-round.
- The Forks (Morice/Bulkley): he saved it with tributaries on at place level; the proposal said off. Told, not changed.
- Show the 19 old wordings on waters no longer live?
- T531 (species under their calendar) still waits on his OK. Prod release waits on DFO labelling (owner-run).

## Review app leftovers (chat 10's, listed for the record only)
- Take-out / add-back of a piece never ran through the real write-through.
- "Save for all" covers only older wordings.
- Allco fence cut needs a "withdrawn" option.
- Pickers do not offer another water's cuts first.
- DFO's heading bytes are rebuilt for display from the `tributaries` flag; verbatim needs a scrape change.
- Places cache staleness was checked by reading; no test.

## Rules learned in chat 10
- **A message that names a button must be shown only where that button exists.** Check the state, not the wording.
- **A cut carried across braids has several resolved rows;** always take the row on the cut's own water (29 cuts drew
  off their river).
- **"The preview is wrong" may be the map fit, not the data:** look at the numbers before changing a proposal.
- **A two-step save needs the second step on screen;** the owner lost a problem report to a box below the fold.
- **Measure speed on the owner's start command,** not the agent's copy with warm-up off.
