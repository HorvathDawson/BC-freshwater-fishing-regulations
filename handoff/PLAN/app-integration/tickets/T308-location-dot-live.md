# T308 — Your location dot keeps moving with you (not stuck at the first fix)

Status: DONE (d5e3fcf8e; emulator-checked 2026-11-06) (owner 2026-10-31, on 0.3.0: "the location doesn't change and update with where the user is. It locks to the
first location it seems").

## Work
While "My location" is on (shown, not only following), keep a foreground location watch running and move the dot with
every fix (native: locate.native.ts watch → me.fix → our own dot from T303 d51ca02c8; web: watchPosition). Following
additionally moves the camera. Stop the watch when location is turned off or the app goes to the background; resume on
return. Battery-sane interval (e.g. 2–5 s / 5–10 m). Emulator: a series of mock fixes moves the dot without following.

Depends: T303. Size: S. Owner: no.

## Done (d5e3fcf8e)
`useMyLocation` starts the foreground watch once the dot is shown (after the first fix) and keeps it for the screen's life; follow only sets `following` (the camera), a hand pan / un-follow no longer stops the watch. The native watch already pauses in background and resumes on return (T303). Unit test: pushed fixes move `fix` without following. Emulator: `locate` scenario gained a 3-fix series step (dot leaves the middle, camera still) — NOT yet run (batch 2026-10-30 hit Maestro flow hangs, see T307).

## Emulator check (2026-11-06, test bed `locate`, mock GPS via `adb emu geo fix`)
PASS (all 6 steps). Granted + tapped: blue dot centred (701 blue px). Without following, 3 successive fixes (~300 m NE each): dot left the middle (0 blue px at the centre after each), dot present on the map after the last, camera still, button stayed "Follow my location". Following: moved fix moved the map (30 % changed, dot re-centred); a pan stops it. Shots: app/tools/testbed/out/t308/ (T303-series-0..3.png, local, gitignored).
