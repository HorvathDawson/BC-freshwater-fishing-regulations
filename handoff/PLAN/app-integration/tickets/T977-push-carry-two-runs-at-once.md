# T977 — Push wake carry-over: two feed runs at once both read it

Status: POSTPONED — decided 2026-11-22 (lane L5, 0.9.0 batch): a DOCUMENTED ACCEPT, no lease now. Revisit when the
staging or production `/status` shows a carried wake ("part N of a wake begun …"), i.e. past 1,500 registered phones.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T977 | Push wake carry-over: two feed runs at once both read it (duplicate silent pushes, or a wake's circle overwritten); no small fix, a lease when worth it | POSTPONED (documented accept 2026-11-22, lane L5: needs > 1,500 phones; lease built when /status first shows a carried wake) | | lane LF 2026-11-20, T739 review |

Source: the combined review of T739 (lane LA), 2026-11-20; lane LF checked whether a small fix exists.

## The problem
A wake too big for one run keeps its place in `private/push/carry.json` (`push.PUSH_CARRY_KEY`, T739 L3). `sendWake`
reads the carry, tells the next `PUSH_SENDS_PER_RUN` phones, then writes the carry (or deletes it when the circle is
done). Two feed runs that wake at the same time both read the same carry: both tell the same next batch (up to 1,500
duplicate silent pushes, harmless), and one carry write overwrites the other (a new wake's circle can be put back or
deleted early; those phones are told only by the next wake). Runs overlap only when one outlasts the 5-minute cron gap
(`wrangler.toml`) or a hand run (`POST /run/<job>`) meets a cron.

## Why there is no small fix (checked 2026-11-20, lane LF)
A conditional write (`put` with `onlyIf: { etagMatches }`) only helps if the batch is CLAIMED before it is sent (today the
carry is written after the sends); R2 `delete` takes no condition; `R2BucketLike.put` here takes only `etagMatches` (no
"only if absent").

## A fix, when it is worth it
A lease: `private/push/lock.json` written with "only if absent" (or `etagMatches` on a lease record that always
exists), holding the run's id and an expiry, taken before `sendWake` reads the carry and released after the carry
write. A run that does not get the lease leaves the wake to the run that holds it (its reason is told on the next
run: the carry stays). Needs: a test double that honours the conditions, a stale-lease rule (expiry over the longest
run), and the subrequest count (`PUSH_FIXED_SUBREQUESTS` +2).

Owner 2026-11-21 12:46: asked for the 0.8.5 batch: "No, next chat".

## Decision (2026-11-22, lane L5; documented accept, PR #39, head 3c186213a)
- The race needs a carry, and a carry needs more than `PUSH_SENDS_PER_RUN` (1,500) registered phones; closed testing has
  well under 100, staging 0, production has no APNs secrets yet (`push.missing.ios`, read 2026-11-22).
- The harm is bounded (duplicate silent pushes, or a circle cut short until the next wake; the phone's own periodic check
  still runs); a gauge run takes about a minute (53.5 s at the 2026-11-22 staging deploy).
- **Trigger to build the lease** (the design above stands): the first time `/status` shows "part N of a wake begun …" on
  either worker, or a duplicate-wake report. The code says the same at `PUSH_CARRY_KEY` (`app/workers/feeds/src/push.ts`).
