# T609 — Feeds worker: fixes from the outside-in audit

Status: OPEN

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
