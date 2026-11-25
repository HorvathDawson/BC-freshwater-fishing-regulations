# T1079 — The Playwright MCP writes its console logs into the main checkout, whatever worktree a lane works in

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1079 | The Playwright MCP writes its console logs into the main checkout, whatever worktree a lane works in | OPEN — none (tooling; with the 0.9.2 chat) | | 0.9.1 chat 2026-11-24 |

Source: the 0.9.1 chat, 2026-11-24: logs land in the main checkout's git-ignored `.playwright-mcp/` directory.

## Work
1. Point the MCP's output at the session's scratchpad (its config or an environment variable), or say in the lane rules that it cannot be moved.
2. Clear what has piled up (owner rule: cleanup freely).

## Acceptance
a lane in a worktree leaves the main checkout untouched; the lane rules say where the logs go.

Severity: low (hygiene; lanes are told never to touch the main checkout). Release: none (tooling; with the 0.9.2 chat). Model: Sonnet. Size: S. Owner: no.
