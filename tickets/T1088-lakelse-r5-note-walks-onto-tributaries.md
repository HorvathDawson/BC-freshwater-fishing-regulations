# T1088 — Lakelse r5, a "Note: the mouth … signs" rule, walks onto every tributary

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1088 | Lakelse r5, a "Note: the mouth … signs" rule, walks onto every tributary | OPEN — 0.10.1 | | 0.9.1 chat 2026-11-24 |

Source: the 0.9.1 chat's T1053 lane, 2026-11-24. Evidence: the T1053 lane's before/after list, `docs/app-integration/T1053-shots/head-lakes-before-after.md` on branch feat/T1053-walk-climbs-head-lakes (not pushed when reported).

## Work
1. A note about where the mouth is marked belongs to the mainstem's mouth, not its tributaries: find why it walks (a `tributaries` flag on a note row?) and how many other note rows do the same.
2. Stop notes of this kind from walking, by the row's kind, not by a hand list.

## Acceptance
the count of note rows that walked before and after; Lakelse's tributaries no longer quote the note.

Severity: low (a note, not a restriction). Release: 0.9.2. Model: Opus. Size: S. Owner: no.
