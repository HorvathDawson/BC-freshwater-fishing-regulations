# T1006 — One read-only command lists every new report from every source; every chat runs it first

Status: OPEN. Until it exists, `handoff/PLAN/START-HERE.md` step 0 lists the commands by hand.

Source: owner 2026-11-22, verbatim: "Also get next chat hand off to always look for more reports on Apple external
test, feed worker ,.etc."

## Today (2026-11-22)
- Worker reports + crashes: `GET /reports?since=` and `GET /crashes?since=` (Bearer RUN_TOKEN) on BOTH workers —
  production `https://feeds.canifishthis.ca` (`app/workers/feeds/.run-token.prod`) and staging (`.run-token`). The
  review app's Reports tab reads staging only (`curation-review/backend/reports.py` DEFAULT_URL).
- TestFlight: `node deploy/staging/testflight-feedback.mjs --days N` (tester comments + screenshots + crash
  submissions) and `node deploy/staging/testflight-distribute.mjs --dry-run --max-wait-min 1` (external review state;
  GET only).
- Google Play: **no reader.** Store reviews (Play Developer API `reviews.list`, the service account `play-upload.py`
  uses) and closed-testing feedback (Play Console only) are not read by anything.
- Nothing says which report is already in a ticket: reports are cited by their words, crashes not at all.

## Work
1. `python -m pipeline reports [--since DATE]` (or a script beside the others): reads every source above, read-only,
   and prints per source the items since the last check, each marked with the ticket that cites it (by report id,
   crash hash or TestFlight id; tickets record those ids from now on) or NEW.
2. Add the Play reviews reader (`reviews.list`, read-only scope).
3. Remembers the last check's time locally (git-ignored), so "new" means new since the last chat.
4. START-HERE step 0 becomes that one command. Tests with every HTTP call mocked.

Depends: none. Size: M. Owner: no (Play's read scope may need a tick in the Play Console: ask then).
