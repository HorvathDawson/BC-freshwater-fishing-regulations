# T1059 — `scripts/ci_status.py --strict` says "no finished run" when the green run is on an ancestor more than 30 runs back

Status: OPEN.

Source: the 0.9.0 chat, 2026-11-24 (also in RESTART-091-chat.md): `strict_judge` looks up an ancestor's runs with gh's default `--limit 30` across all branches, so on a busy night docs-ci's green ancestor (`15a257ca9`) fell out of range and `build-apk.sh --dry-run` stopped. The 0.9.0 production build used `CI_CHECK_SKIP`.

## Work
1. Query runs per commit (`gh run list --commit <sha>`, or the API by head_sha) instead of the newest 30 of all branches.
2. A test with more than 30 newer runs on other branches.

## Acceptance
the 0.9.1 build's CI check passes without `CI_CHECK_SKIP` on a busy day; mutation: the old query fails the new test.

Severity: medium (teaches the release run to skip its CI gate). Release: 0.9.1 (before its build). Model: Sonnet. Size: S. Owner: no.
