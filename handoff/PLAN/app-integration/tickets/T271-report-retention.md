# T271 — Problem reports deleted automatically after 180 days

Status: OPEN (owner 2026-10-29: "yes this sounds good"; privacy@canifishthis.ca mailbox is set up by the owner).

## Work
1. Feeds worker (app/workers/feeds) daily cron deletes report objects older than 180 days (by stored timestamp), staging bucket
   for staging, prod config ready; logs the count in /status; a failure goes to the daily digest.
2. Privacy page: "reports are deleted 180 days after they are received"; contact line = privacy@canifishthis.ca (no
   placeholder). Remove the OWNER-TODO items for the mailbox + retention.

## Acceptance
Unit test: 181-day-old deleted, 179-day-old kept; staging run shows the count; page wording matches.

Depends: none. Size: S. Owner: no (prod worker deploy is the owner's command).
