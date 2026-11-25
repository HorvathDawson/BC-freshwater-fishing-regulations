# T1066 — Two hidden failures in the feeds code: the HYDAT release check and the DFO cache readers

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1066 | Two hidden failures in the feeds code: the HYDAT release check and the DFO cache readers | OPEN —  | | planning chat 2026-11-24 |

Source: the 0.9.1 chat's finds on T1021 (RESTART-091-chat.md, 2026-11-24). Owner rule `no-silent-fallbacks`.

## Work
1. `pipeline/gauges/feed/publish.py` hides a failed HYDAT release check (`except Exception: latest = None`): report it.
2. The DFO cache readers never refuse a missing directory: refuse with the path.

## Acceptance
a test per case that fails with the swallow put back.

Severity: medium. Release: 0.9.2. Model: Opus. Size: S. Owner: no.
