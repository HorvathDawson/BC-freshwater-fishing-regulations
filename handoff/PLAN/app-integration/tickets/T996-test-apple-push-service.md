# T996 — Test the Apple push service end to end (next release)

Status: OPEN (owner 2026-11-21: next release, the next chat; not this one).

Source: owner 2026-11-21, verbatim: "Add ticket to test apple push service in next release. (The next chat)."

## Context
- T739 / T822: iPhone push "wake-ups" from the feeds worker (APNs, iPhone only); both still wait on a TestFlight build
  and the owner phone test.
- T994 lane F1 (`lane/t994-f1-worker`, finding 23): the worker now keeps its APNs auth token and reuses it for 50
  minutes instead of minting one per wake. Not verified against Apple's live service (Apple refuses a token older than
  an hour and throttles one refreshed more often than every 20 minutes). Finding 25 caps new push tokens at 1,000 a day.

## Work
1. With the next release's TestFlight build: the phone registers with the worker (staging, then production).
2. A wake-up push reaches the phone; the app checks and shows the alert/notice on time.
3. Across more than one hour of worker runs: the token is reused, refreshed before expiry, and Apple returns no
   `TooManyProviderTokenUpdates` / `ExpiredProviderToken`; the worker log says which token it used.
4. A refused token (bad device token, 410 Unregistered) is dropped and said in the log.

## Acceptance
Owner phone receives a wake-up from staging and from production; worker logs over 1 h+ show no token errors.

Depends: T994 F1 merged; a TestFlight build. Size: S. Owner: yes (the phone test; Claude runs the rest).
