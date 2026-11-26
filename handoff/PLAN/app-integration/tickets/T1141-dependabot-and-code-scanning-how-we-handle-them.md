# T1141 — Dependabot and code scanning are on: how their PRs and alerts are handled

Status: OPEN — the hardening release (tooling); the first Dependabot PR (#250) is held until 0.10.0 is out

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1141 | Dependabot and code scanning are on: how their PRs and alerts are handled | OPEN — the hardening release (tooling); the first Dependabot PR (#250) is held until 0.10.0 is out | | repo-move chat 2026-11-25; owner: "I turned them on, keep" |

Source: the repo-move chat's snapshot compare after the transfer, 2026-11-25 15:25. The owner turned on Dependabot
security updates, and GitHub's CodeQL and Dependency Graph workflows now exist on the new repo; his words: "I turned
them on, keep". Dependabot has already opened PR #250 (vite 5.4.21 to 8.3.8, a three-major jump, no auto-merge).
Related: T1041 (the move), T1081 and the hardening chat (they will meet more of these), T1100 (open-source
notices: the dependency list changes), `no-github-schedules` (these two are GitHub's own scheduled services, kept
by the owner's word: the rule's one exception, written here so nobody removes them or adds others on its strength).

## Work
1. A short policy in AGENTS.md: who looks at a Dependabot PR and when (proposed: the chat that owns the next
   release, once per release, never during a release in flight); security updates within a week; a major-version
   bump is never merged as it comes: it becomes a ticket with its own lane and the full app tier; nothing from a
   bot is set to auto-merge; a `dependabot.yml` that groups minor and patch bumps and names the ecosystems (pnpm
   workspace, pip, GitHub Actions, Gradle).
2. PR #250 (vite 8): held. Decide: close it with a note and ticket the upgrade properly, or keep it open as the
   reminder. The web bundle's size budgets (T1136) and the store-shots export depend on vite.
3. CodeQL: read its first results; alerts become tickets by severity; say which languages it scans and what its
   runs cost in wall time on a PR (it must not slow the merge queue: not a required check unless the owner says).
4. The first Dependency Graph alerts: list them; a real security alert in a shipped dependency goes into the next
   release.

Acceptance: the policy is in AGENTS.md, `dependabot.yml` is on main, #250 has a decision, and the first CodeQL and
dependency alerts are each a ticket or closed with a reason. Severity: low (medium if a first alert is real).
Model: Sonnet (Opus for a major upgrade's lane). Size: S. Owner: no (he sees the policy).

## 2026-11-25 15:45: paused until 0.10.0 is out; how to turn them back on
Owner (relayed by the repo-move chat): "Pause both until 0.10.0 is out". Dependabot security updates: off, by
him (the repo switch). CodeQL default setup was forced by the ORG security configuration
`canifishthis-org-config-1` (id 303766), not a repo switch; with his "you can change it" the repo-move chat set
its `code_scanning_default_setup` to disabled at 15:43. Back on after 0.10.0: that one field to enabled, and the
repo switch for Dependabot security updates (an owner item, 2026-11-25 afternoon). PRs #250, #252 and #254 were closed.

## Owner

- [x] 2026-11-25 — Dependabot security updates and CodeQL → "I turned them on, keep" (2026-11-25, repo-move chat); Dependabot PR #250 (vite 5 to 8) is held until 0.10.0 is out.
