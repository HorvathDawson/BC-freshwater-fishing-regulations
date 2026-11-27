# T1068 — T1027 leftovers: scheduled notifications across an update, the `offline` bench step, a cold-start stall

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1068 | T1027 leftovers: scheduled notifications across an update, the `offline` bench step, a cold-start stall | OPEN —  | | planning chat 2026-11-24 |

Source: the 0.9.1 chat's finds on T1027 (RESTART-091-chat.md, 2026-11-24).

## Work
1. A notification SCHEDULED before an update would not load after it (trigger classes have no serialVersionUID). The app schedules none today (AGENTS rule 34b says so): fix before any feature schedules one (T234).
2. The `offline` bench step opens a water on its Map tab and fails.
3. One 315 / 544 ms cold-start stall seen under load: measure again on a quiet machine.
4. expo-modules-core and react-native-svg keep rules cannot be narrowed without a pnpm patch: decide if it is worth it.

## Acceptance
the bench step passes; item 1 has a test or stays guarded by rule 34b with this ticket named.

Severity: low. Release: 0.10 (item 1 before T234's scheduled alerts). Model: Opus. Size: S–M. Owner: no.
