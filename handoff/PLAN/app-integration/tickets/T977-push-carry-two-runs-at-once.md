# T977 — Push wake carry-over: two feed runs at once both read it

Status: POSTPONED — decided 2026-11-22 (lane L5, 0.9.0 batch): a DOCUMENTED ACCEPT, no lease now. Revisit when the
staging or production `/status` shows a carried wake ("part N of a wake begun …"), i.e. past 1,500 registered phones.

Source: the combined review of T739 (lane LA), 2026-11-20; lane LF checked whether a small fix exists.

## The problem
A wake too big for one run keeps its place in `private/push/carry.json` (`push.PUSH_CARRY_KEY`, T739 L3). `sendWake`
reads the carry, tells the next `PUSH_SENDS_PER_RUN` phones, then writes the carry (or deletes it when the circle is
done). Two feed runs that wake at the same time both read the same carry:
- both tell the same next batch (up to 1,500 duplicate silent pushes; a phone checks the feeds twice, harmless);
- both write the carry: one overwrites the other. Where one of them started a NEW wake (`reason`) from the carried
  cursor, the other's write can put back the old circle, or delete the carry while the new circle still has phones
  left. Those phones are then told only by the next wake.

## When it can happen
The worker's crons are at :05/:35 (gauges), :15 (DFO), :45 (in-season), :50 (DFO retry, only after a failed DFO run;
`wrangler.toml`), 5 minutes apart at the closest. Two runs overlap only when a run lasts longer than the gap to the
next one, or when a run is started by hand (`POST /run/<job>`) during another.

## Why there is no small fix (checked 2026-11-20, lane LF)
- A conditional write on the carry (`put` with `onlyIf: { etagMatches }`, as `io.writeMerged` does for the status)
  only helps if the batch is CLAIMED before it is sent: write the advanced carry first, send only if the write won.
  Today the carry is written after the sends, and a failed write throws (the phones left would never be told).
- R2 `delete` takes no condition, so "the circle is done, delete the carry" cannot be made safe this way.
- Creating the first carry would need "only if absent". `R2BucketLike.put` here takes only `etagMatches`
  (`io.putIfUnchanged` with no etag writes unconditionally).
- A wake with no carry at all (it fits in one run) has nothing to condition on: two runs with reasons each tell every
  phone once. That is two wakes for two reasons, which is fine.

## A fix, when it is worth it
A lease: `private/push/lock.json` written with "only if absent" (or `etagMatches` on a lease record that always
exists), holding the run's id and an expiry, taken before `sendWake` reads the carry and released after the carry
write. A run that does not get the lease leaves the wake to the run that holds it (its reason is told on the next
run: the carry stays). Needs: a test double that honours the conditions, a stale-lease rule (expiry over the longest
run), and the subrequest count (`PUSH_FIXED_SUBREQUESTS` +2).

## Owner 2026-11-21 12:46
Asked for the 0.8.5 batch: "No, next chat".

## Decision (2026-11-22, lane L5): accept and document; the lease waits for its trigger
- **The race needs a carry, and a carry needs more than `PUSH_SENDS_PER_RUN` (1,500) registered phones.** Below that
  every wake fits in one run and no carry is written: two runs at once each tell every phone once (two wakes for two
  reasons, fine). Closed testing has well under 100 phones; staging has 0 registered today, production has no APNs
  secrets yet (`push.missing.ios` = all three, `GET https://feeds.canifishthis.ca/status.json`, read 2026-11-22).
- **Even with a carry, the harm is bounded.** Duplicate silent pushes (a phone checks twice; iOS throttles background
  pushes anyway), or one wake's circle cut short: those phones are told by the next wake, and the gauge run wakes every
  30-60 min by day whenever a station changed. No alert content is lost: the phone's own periodic check still runs.
- **The overlap itself is rare.** The crons are 5 minutes apart at the closest (`wrangler.toml` `[triggers]`); a gauge
  run takes about a minute (53.5 s measured at the 2026-11-22 staging deploy); only a hand run (`POST /run/<job>`)
  during a cron, or a run slower than 5 minutes, overlaps.
- **A lease costs more than it saves today:** a new always-existing record with `etagMatches`, a stale-lease rule (an
  expiry longer than the longest run, or a crashed run blocks every wake until it lapses), a test double that honours
  conditions, and 2 more fixed subrequests per wake. Each is a new way for wakes to stop.
- **Trigger to build it** (the design above stands): the first time `/status` shows "part N of a wake begun …" on
  either worker, or a duplicate-wake report. The code says the same at `PUSH_CARRY_KEY` (`app/workers/feeds/src/push.ts`).

## State 2026-11-22 (stopped for credits)
Done: documented accept (PR #39, head 3c186213a). Left: the lease, when /status first shows a carried wake.
