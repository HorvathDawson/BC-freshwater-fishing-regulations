# T977 — Push wake carry-over: two feed runs at once both read it

Status: OPEN — not built: no small, clean fix (the analysis below). Low risk today: the crons are at least 5 minutes apart.

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
