# T1059 — `scripts/ci_status.py --strict` says "no finished run" when the green run is on an ancestor more than 30 runs back

Status: DONE — 87e1783a3, 34c35758a (review: "no run" only when every run GitHub counts was read). The ancestor search pages through every run of the workflow (`gh api …/runs`, 100 a page, at most
10 pages, then CANNOT TELL). Live, read-only: `ci_status.py --strict --sha defb62b4c` (the 0.9.0 build commit) said
"STOP docs-ci.yml: no finished run" before and "ok … nothing in its area changed since its green run on 15a257ca9a78"
after (that run was number 109 of 375). Tests replay the recorded pages (`pipeline/tests/fixtures/ci_status_docs_ci_runs_2026-11-24.json`);
mutations (the old newest-30 query, one page only, the cap answering "no run", counting a shifted run twice) each fail them.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1059 | `scripts/ci_status.py --strict` says "no finished run" when the green run is on an ancestor more than 30 runs back | DONE | 87e1783a3, 34c35758a | planning chat 2026-11-24 |

Source: the 0.9.0 chat, 2026-11-24 (also in RESTART-091-chat.md): `strict_judge` looks up an ancestor's runs with gh's default `--limit 30` across all branches, so on a busy night docs-ci's green ancestor (`15a257ca9`) fell out of range and `build-apk.sh --dry-run` stopped. The 0.9.0 production build used `CI_CHECK_SKIP`.

## Work
1. Query runs per commit (`gh run list --commit <sha>`, or the API by head_sha) instead of the newest 30 of all branches.
2. A test with more than 30 newer runs on other branches.

## Acceptance
the 0.9.1 build's CI check passes without `CI_CHECK_SKIP` on a busy day; mutation: the old query fails the new test.

Severity: medium (teaches the release run to skip its CI gate). Release: 0.9.1 (before its build). Model: Sonnet. Size: S. Owner: no.
