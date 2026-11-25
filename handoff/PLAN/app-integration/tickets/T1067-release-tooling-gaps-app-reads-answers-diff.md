# T1067 — Release tooling gaps: `app_reads --check` on a format change, and `answers_diff`'s licence keys and count

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1067 | Release tooling gaps: `app_reads --check` on a format change, and `answers_diff`'s licence keys and count | OPEN —  | | planning chat 2026-11-24 |

Source: the 0.9.1 chat's finds on T1048, T1030 and T1020 (RESTART-091-chat.md, 2026-11-24).

## Work
1. On main a format change fails `app_reads --check` for the old app version with no clean way out: give it one (a recorded, reviewed step).
2. `LIC_KEYS` in `answers_diff` does not list the licensing refs `alt`, `displaced_by`, `displaced`, `also_printed`.
3. `answers_diff` treats a licence `when` change as wording only.
4. "N waters changed" prints no thousands separator, and the count is against the immediate predecessor only (a phone that skipped a release sees too few).

## Acceptance
tests per item; the count question in item 4 goes with the owner's T1030 answer.

Severity: low to medium (item 1 can stop a cut). Release: 0.9.2 (item 1 before the first format change). Model: Opus. Size: S–M. Owner: no.
