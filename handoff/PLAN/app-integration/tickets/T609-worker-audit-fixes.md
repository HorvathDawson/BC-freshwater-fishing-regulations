# T609 — Feeds worker: fixes from the outside-in audit

Status: PARTIAL — f19e26a3b (W7), 592144f2e (W1), 7f0273448 (W4), dbef7f480 (W5; the ranked-share check not added), ccd044950 (W15), ed4872785 (W14), 9423258fa (W12), d9a13521e (W10 worker side), a09928919 (W11), c09d3c180 (W19 by cap). Left: W17 (upstream size caps, ECCC range checks), W20 (never-told kinds re-read), W16 (read-only token, prod rotate script, curl -H @file), W18 + W10's tab (review app, chat 10).

Source: T608 part C, `handoff/PLAN/app-integration/T608C-findings.md` (20 findings W1–W20, each with file:line,
evidence and fix). W3, W8, W9, W13 belong to T558; W2, W6 to T559. This ticket takes the rest.

## Work (by severity)
1. **W1 (high):** rate limits keyed on the full address: one home IPv6 line can write without limit to `/report` and
   `/crash`; no daily total; the digest reads only the first 1,000 keys a day. Key IPv6 on its /64, add a daily total
   per route with a plain refusal, and make the digest say when it stopped counting.
2. **W4:** in-season fetch follows redirects and reads any matching tables: pin the final URL and check the page is the
   in-season page before reading rows; a mismatch is a failed run, last good kept.
3. **W5:** gauges publish with no envelope when `clim.json` is missing and record the run ok: fail the rank step
   loudly, keep the last good ranks, say so in status and digest.
4. **W7:** `.run-token.prev` is not git-ignored; ignore it, remove it on every exit path of the rotate script.
5. **W14, W15:** a missing `matches.json` is a failed in-season run, not "all unmatched"; `?limit=` never rewrites the
   public index.
6. **W10, W11, W12, W17, W19, W20:** list answers say `truncated` / `skipped`; body size checked in bytes while
   reading; status page headers and no stack traces to anonymous callers; size caps and no blind redirects on
   upstream reads, ECCC values range-checked; the staging `test` flag needs the token or a build-time key; crash kinds
   never told are not re-read every run.
7. **W16, W18:** separate read-only token for the review app's Reports tab and a staging/production switch there
   (the review app is chat 10's: list the change for it, do not edit it); production rotation script.
Anything not done is listed in the report with its W number.

## Acceptance
- Each W item above has a test or is closed with a reason; worker tests green; nothing deployed without the owner's
  usual staging deploy.

Depends: T558, T559 (same files; one agent does all three in order). Size: M–L. Owner: no.

## Left for the review app (chat 10's `curation-review/`; not edited here)
- W18: the Reports tab reads staging by default (`backend/reports.py` DEFAULT_URL). Needs a staging/production switch
  (or both listed), each with its own token file (`.run-token`, `.run-token.prod`); add it to T404's switch list.
- W10: GET /reports and /crashes now answer `truncated` and `skipped` beside the list (d9a13521e); the tab should show
  "list cut at 500" / "N unreadable" when they are set.
- W16 (worker + review app): a second, read-only token for GET /reports and /crashes, so the review app no longer holds
  the token that runs jobs and sends mail; a production twin of `deploy/staging/rotate-run-token.sh`; scripts pass the
  token with `curl -H @file`, not on the command line.
