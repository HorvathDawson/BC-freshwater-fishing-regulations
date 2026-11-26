# T1145 — The game-fish status bar and its "Open" word take the same open colour as the salmon bar

Status: OPEN — 0.10.0 (the release is held anyway); app-only, small

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1145 | The game-fish status bar and its "Open" word take the same open colour as the salmon bar | OPEN — 0.10.0 (the release is held anyway); app-only, small | | owner 2026-11-25; T1028 leftover; Capilano before/after picture |

Source: owner, 2026-11-25, in the 0.10.0 chat while looking at the T1140 pictures, verbatim: "also the calendar bar
colors still do not match for open". T1028 (owner 2026-11-23: "the schedule bar for trout should have same open
color as salmon") changed only the Trout and char YEAR bar and was closed. The GAME FISH status bar at the top of
a water's page is still blue while the SALMON bar is green (the Capilano picture), and its "✓ Open" word too.
T1028 stays closed; this ticket is its leftover.

Work: one open colour token for every "open" bar and word on the page, whatever the species group; find every
other place a second open colour is defined (a search for the blue token) and list them in the PR. Web and phone
(rule 34a pairs). No data change.

Acceptance: a test that fails when two species groups resolve "open" to different colours (mutation-proven);
Capilano before/after pictures to the owner. Severity: low. Model: Sonnet. Size: S. Owner: sees the pictures.
