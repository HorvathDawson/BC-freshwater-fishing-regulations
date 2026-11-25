# T1124 — Push token while permitted (T1116): the leftovers

Status: OPEN.

Source: the 0.9.1 chat's T1116 lane and review (PR #194), sent to the planning chat 2026-11-25 02:12. Not checked by
the planning chat. Related: T1116, T996 (production push proof), T1098 (the privacy page's row 10), T1110.

1. An untrue line, ships in 0.9.1: with a notification turned on in the app but blocked in the phone's settings, the
   push line reads "Server wake-ups: off (no notification is on); ...". The lane suggests "(notifications are
   blocked in the phone's settings)". The words are the owner's: a proposal until he picks.
2. The worker's daily token sweep has no cap and shares the backup's subrequest budget: cap it, or give it its own
   run, and report when the cap is hit (`no-silent-fallbacks`).
3. A failed unregister is retried at the next cold start only. Decide: retry on resume too, or say why not.
4. 0.9.0 and earlier still register after "Don't Allow". The privacy page says "From version 0.9.1"; nothing to
   build, but the 30-day sweep is what removes those tokens: check after 30 days that it did.
5. Not verified: no simulator or device walk of the three paths (allow, deny, allow then turn off in settings), and
   `notify.native.ts` `onResume` has no test. Do the walk on both platforms with the worker's token list read before
   and after; add the test.

Acceptance: items 2, 3 and 5 with tests that fail without the change; item 1 after the owner's pick; item 4 a dated
check. Severity: medium (item 1 is an untrue line; item 5 is an unproved privacy promise). Release: 0.9.2. Model: Opus
(worker + native lifecycle). Size: S–M. Owner: yes (the wording of item 1).
