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

## State when this page was written (2026-11-26 02:25; check `gh pr list` and each ticket's row first)

| Ticket | Branch | State |
|---|---|---|
| T1073 | `fix/T1073-update-notice-installable` | MERGED (#285); PARTIAL: TestFlight at run time, his Play Internal check |
| T1163 | `fix/T1163-iphone-audit-leftovers` | #289 open, auto-merge; left: a comment in `legal.ts` still says a wording change re-asks (his pick: no re-ask) |
| T1140 | `test/T1140-order-dependent-test` | reviewed "merge as is"; pushed first; PR + auto-merge; tell the planning chat its number |
| T1164 item 1 | `fix/T1164-error-boundary` | reviewed, fixed; push, PR, auto-merge |
| T1167 | `feat/T1167-inseason-banner-window` | reviewed, fixed, his three picks built; PR WITHOUT auto-merge until he has seen the amber pictures (sent) |
| T1156 | `feat/T1156-region-pdf-fetch-and-synopsis-guard` | reviewed; rework in progress on his pick "delete the folder": fetch + pins out, guard stays |
| T1166 | `feat/T1166-synopsis-change-feed` | review "fix first", NOT yet applied: a missing result is a note not an error; a failed or `--only` run keeps the last finding; refuse a partial re-pin after a library change |
| T1168 item 1 | `feat/T1168-licence-wording-fwid-wild` | lane building his three picks (A, A, A); then one review; visible: parked |
| T1092 | `fix/T1092-web-db-deleted` | review "fix first", NOT applied: listen for the connection's `close`; say what was lost, once; two tests cannot fail |
| T1093 | `fix/T1093-location-settings` (on T1163's branch) | review "fix first", NOT applied: its own line "Location is off on this phone", not the app-permission line; the error code name is the old one |
| T1077 | `fix/T1077-bait-source-line` | built, not reviewed; visible (the "All sources" order): parked |
| T1078 | `fix/T1078-all-year-closers` | built, not reviewed; owner pick open in the ticket (cite every overlapping closer? default no) |
| T1105 | `fix/T1105-yielded-closure-detail` | findings in the ticket only: needs a data build, or the app-only route written there |
| T1085, T1096 | lane running | ice-fishing wording and its citation |

Not started: T1172 (setup fixes: after the planning chat's word), T1123 + T1124, T1164's other app items, T1111,
T1100 (inventory path and the owner's two answers are with the planning chat), T1169 item 3, the OpenStreetMap
client name (`pipeline/atlas/waters/added_streams/fetch_osm.py:30`).

## The owner's answers (2026-11-26, relayed by the planning chat; each goes into its ticket's `## Owner` block)

Licence wording A, A, A (T1168). Banner inside the window: amber with a mark; before: grey; a range starting after
its effective day: never dated (T1167, built). First-run re-ask: no (T1163). Region PDFs: delete the folder (T1156).
Waiting for his look: the T1167 amber pictures; T1163's four pictures; T1077 and T1078 before / after; T1168's.
Pictures live in each ticket's `Tnnn-shots/` where committed (T1167); the others were sent in chat and are remade
by the lane's opt-in shots test.

## For the planning chat

Sent 2026-11-26: the TestFlight case; the no-throw gate's blind spot; report context missing a boundary name; the
held water pane; `locate.web.ts` code 2; six undatable in-season notices after the next worker deploy; T1105's
app-only route.
