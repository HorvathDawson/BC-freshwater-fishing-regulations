# T308 — Your location dot keeps moving with you (not stuck at the first fix)

Status: OPEN (owner 2026-10-31, on 0.3.0: "the location doesn't change and update with where the user is. It locks to the
first location it seems").

## Work
While "My location" is on (shown, not only following), keep a foreground location watch running and move the dot with
every fix (native: locate.native.ts watch → me.fix → our own dot from T303 d51ca02c8; web: watchPosition). Following
additionally moves the camera. Stop the watch when location is turned off or the app goes to the background; resume on
return. Battery-sane interval (e.g. 2–5 s / 5–10 m). Emulator: a series of mock fixes moves the dot without following.

Depends: T303. Size: S. Owner: no.
