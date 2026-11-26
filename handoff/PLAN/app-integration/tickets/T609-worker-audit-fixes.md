# T609 — Feeds worker: fixes from the outside-in audit

Status: PARTIAL — f19e26a3b (W7), 592144f2e (W1), 7f0273448 (W4), dbef7f480 (W5; the ranked-share check not added), ccd044950 (W15), ed4872785 (W14), 9423258fa (W12), d9a13521e (W10 worker side), a09928919 (W11), c09d3c180 (W19 by cap). left (Claude): W17 (upstream size caps, ECCC range checks), W20 (never-told kinds re-read), W16 (read-only token, prod rotate script, curl -H @file), W18 + W10's tab (review app: curation-review/backend/reports.py still defaults to staging).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T609 | Feeds worker: fixes from the outside-in audit (rate limits on IPv6 /64 + daily total, in-season URL pinned, no rank without the envelope, token file ignored, list truncation, upstream caps) | PARTIAL — left: W16, W17, W20; W18 + W10 tab are the review app's | f19e26a3b, 592144f2e, 7f0273448, dbef7f480, ccd044950, ed4872785, 9423258fa, d9a13521e, a09928919, c09d3c180 | T608C findings W1, W4, W5, W7, W10–W20; with T558, T559 |

Source: T608 part C, `handoff/PLAN/app-integration/T608C-findings.md` (20 findings W1–W20, each with file:line,
evidence and fix). W3, W8, W9, W13 belong to T558; W2, W6 to T559. This ticket takes the rest.

Done (commits in Status): W1, W4, W5 (the ranked-share check not added), W7, W10 worker side, W11, W12, W14, W15, W19 by cap.

## Work (left)
1. **W17:** size caps and no blind redirects on upstream reads, ECCC values range-checked.
2. **W20:** crash kinds never told are not re-read every run.
3. **W16 (worker + review app):** a second, read-only token for GET /reports and /crashes, so the review app no longer
   holds the token that runs jobs and sends mail; a production twin of `deploy/staging/rotate-run-token.sh`; scripts pass
   the token with `curl -H @file`, not on the command line.
4. **Review app** (chat 10's `curation-review/`; list the change for it, do not edit it here):
   - W18: the Reports tab reads staging by default (`backend/reports.py` DEFAULT_URL). Needs a staging/production switch
     (or both listed), each with its own token file (`.run-token`, `.run-token.prod`); add it to T404's switch list.
   - W10: GET /reports and /crashes now answer `truncated` and `skipped` beside the list (d9a13521e); the tab should show
     "list cut at 500" / "N unreadable" when they are set.
Anything not done is listed in the report with its W number.

## Acceptance
- Each W item above has a test or is closed with a reason; worker tests green; nothing deployed without the owner's
  usual staging deploy.

Depends: T558, T559 (same files; one agent does all three in order). Size: M–L. Owner: no.
