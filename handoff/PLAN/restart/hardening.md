# Restart: the hardening build chat

Fixes that need only unit tests. Orchestrator on Fable; at most 3 lanes (Sonnet; Opus where a rule or a date is read
from text); lanes spawn no sub-agents; one independent review per PR; merge queue with auto-merge; PR titles 93
characters at most; each fix has a test that fails without it. Work in scratchpad worktrees of `origin/main`.

## Never

- No data build, atlas, tiles, emulator, phone build, heavy job or parser run. `scripts/before_pr.sh` is two heavy
  jobs: not run (planning chat agreed 2026-11-26); a lane runs its own test files one at a time, typecheck, knip,
  platform, boundaries, check-tickets; the pre-push hook runs at push; the PR body says CI is the wide run.
- Not `.github/workflows`, `pipeline/release*`, `scripts/heavy.sh`, the hooks, the build key code, `app/packages/map`.
- Number no ticket (the planning chat does). Close no ticket with an open `## Owner` line.
- Pushes one at a time, about five minutes apart, on the planning chat's word when the Mac is loaded. Ask it before
  queueing a PR that touches the docs folder beyond ticket files. A visible change is parked (no auto-merge) until
  the owner has seen its picture. After the folder rename lands: `python3 scripts/rename_handoff.py --commit` on each
  open branch, then merge main.

## State when the chat was closed (2026-11-26 03:05; check `gh pr list` and each ticket's row first)

Every branch below is also a local branch of the main checkout's repository (lane worktrees share its `.git`), pushed or not.

| Ticket | Branch | State |
|---|---|---|
| T1073, T1163, T1140 test leak, T1156 | merged: #285, #289, #294, #298 | T1073 PARTIAL (TestFlight at run time; his Play Internal check); T1163 open for the 12 steps; T1156 PARTIAL for two setup-page lines (repo-move chat) |
| T1164 item 1 | `fix/T1164-error-boundary` | #302, auto-merge on; ticket PARTIAL (items 2-5 and the finds in it) |
| T1167 | `feat/T1167-inseason-banner-window` | reviewed, fixed, his three picks built; NOT PUSHED: the pre-push hook refused it on four guards the lane never ran (`tools/no-silent-skips`, `one-button` literal radius, `one-day-table` spelled once, `regs-tokens`); fix those, then a PR WITHOUT auto-merge until he has seen `T1167-shots/inside-*.png` |
| T1166 | `feat/T1166-synopsis-change-feed` | review "fix first"; the lane was stopped part-way through the seven fixes: read its last commits before queueing |
| T1168 item 1 | `feat/T1168-licence-wording-fwid-wild` | his picks A, A, A built and tested (a doubled full stop in the licensing composer fixed on the way); not reviewed; needs the mini release re-cut; pictures made |
| T1085, T1096 | `fix/T1085-ice-gear-hook-word`, `fix/T1096-ice-clause-cited` | built, pictures made, not reviewed; need the mini release re-cut; both and T1168 change `answers/gear.py`: land one after another |
| T1092, T1093 | `fix/T1092-web-db-deleted`, `fix/T1093-location-settings` | review "fix first"; the fixes are committed but their tests were NEVER RUN (T1093's are unproven, its ticket still says "no new words"): run each test file first |
| T1077, T1078 | `fix/T1077-bait-source-line`, `fix/T1078-all-year-closers` | built, pictures made, not reviewed; both visible: parked |
| T1105 | `fix/T1105-yielded-closure-detail` | findings in the ticket only: a data build, or the app-only route written there |

Not started: T1172 (setup fixes: on the planning chat's word), T1123 + T1124, T1164's other app items, T1111, T1100
(the inventory path and the owner's two answers are with the planning chat), T1169 item 3, the OpenStreetMap client
name (`pipeline/atlas/waters/added_streams/fetch_osm.py:30`). Left on T1163: a comment in `legal.ts` still says a
wording change re-asks (his pick: no re-ask). For T1164's "left" list: `gear.count_nouns` / `statics.countNouns` is
read by nothing in core; Kamloops Lake's ice-fishing way in the trout fixture release prints "Lines: 1.".

## Waiting on the owner

- His look: the T1167 amber pictures (in its `T1167-shots/`); T1163's four, T1077, T1078, T1085, T1096 before / after
  (sent in chat or made by each lane's opt-in shots test; not in the repo).
- T1078: cite every overlapping closer on a water closed all year (499 waters; cuts T1052's one block)? Default no.
- May this chat run `python -m pipeline.tools.mini_release --write` (a small sample release on invented data that CI
  rebuilds on every run; not timed, never run here) once per branch under the lock? Recommended yes. Until then
  T1085, T1096 and T1168 fail CI's "committed mini release matches a rebuild".
- Answered 2026-11-26 (relayed by the planning chat; in each ticket's `## Owner` block): licence wording A, A, A;
  banner amber with a mark, grey before, never date a range starting after its effective day; first-run re-ask no;
  region PDFs: delete the folder.

## For the planning chat

Sent 2026-11-26: the TestFlight case; the no-throw gate's blind spot; report context missing a boundary name; the
held water pane; `locate.web.ts` code 2; six undatable in-season notices after the next worker deploy; T1105's
app-only route.
