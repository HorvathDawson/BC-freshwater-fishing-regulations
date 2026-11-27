# T1069 — A Kildala-style polygon route is written in 10 pieces

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1069 | A Kildala-style polygon route is written in 10 pieces | OPEN —  | | planning chat 2026-11-24 |

Source: the 0.9.1 chat's find on T1024 (docs/agents/RESTART-091-chat.md, 2026-11-24). Also noted there: a selection rule inside a layer filter splits the stream bucket (`lab.test.ts` already guards it).

## Work
1. Say what the 10 pieces cost (tile size, outline joins, selection) and how many waters have it.
2. If it shows on screen or costs size: write one route per polygon reach.

## Acceptance
the count of affected waters; before/after shots if anything changes on screen.

Severity: low. Release: 0.10 (with a tile build). Model: Opus. Size: S–M. Owner: no.
