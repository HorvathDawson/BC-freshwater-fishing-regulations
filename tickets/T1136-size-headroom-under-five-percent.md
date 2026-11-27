# T1136 — Size budgets with under 5% headroom: web conditions_open, web first_tap, water

Status: OPEN — 0.10.1

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1136 | Size budgets with under 5% headroom: web conditions_open, web first_tap, water | OPEN — 0.10.1 | | 0.10.0 chat 2026-11-25 |

Source: the 0.10.0 chat, 2026-11-25 14:50 (the 0.10.0 release's size gates). Related: T905, T914, T1027.

Headroom left under the budgets on the 0.10.0 candidate: web `conditions_open` 2.8%, web `first_tap` 2.8%, water
3.4%. The next change of any size stops a release on a size gate.

Work: for each of the three, say what grew since 0.9.0 and by how much; then either cut it back (preferred: T914
is about the same bytes) or raise the budget with the reason written beside it. Nothing is raised silently.

Acceptance: each of the three has at least 10% headroom or a written reason. Severity: low (a release stop waiting
to happen). Model: Opus (with T914's lane). Size: S. Owner: no (a raised budget is shown to him).
