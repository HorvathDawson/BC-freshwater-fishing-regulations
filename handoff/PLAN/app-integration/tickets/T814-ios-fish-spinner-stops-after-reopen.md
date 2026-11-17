# T814 — iOS: after the app is closed and reopened mid-download, the fish spinner stops moving

Status: OPEN (app, iOS; app chat)

Source: owner 2026-11-16 ~21:22, testing iPhone TestFlight build 36 (app chat; pictures in the chat). Verbatim: "a-d worked good except after closing app and opening again the fish icon stopped moving....".
The first-install checks (a) lock, (b) switch apps, (c) swipe away and reopen, (d) a steady bar all passed; only the
swimming fish (`fish-sprite.generated.ts`, the download screen's spinner) stood still after the reopen.

## Look at
The spinner's animation start on a cold start that resumes a download (the bar already part-way): an animation started
before the view is laid out, a JS-driven loop paused by the app state, or the T746 "applying" screen's swap. Reproduce on
the iOS simulator (`deploy/staging/build-ios-sim.sh`): start the first download, kill the app, reopen.

## Owner, 2026-11-16 ~23:40 (app chat), verbatim
"the spinner on a second phone jusst never spinned on initial download". So on a second iPhone the fish never moved on the
first download at all, not only after a reopen. Simulator (app chat, iOS worker): the fish moves on first download, after a
mid-download kill and after a background launch, so not reproduced there. Lead: `FishSpinner.native.tsx` stands still by
design under Reduce Motion (`AccessibilityInfo.isReduceMotionEnabled`), and a still fish with no other progress sign reads as
hung. Asked the owner whether Reduce Motion or Low Power Mode is on on that phone.

## Owner, 2026-11-16 ~23:50 (app chat), verbatim
"loading icons seem like they can exist still maybe instead we make a second loading icon i will make it then send it here".
So a still loading icon is fine; the owner designs a second, still icon and sends it in the app chat. The app keeps one slot
for it (used where the fish cannot move, e.g. Reduce Motion); today's still fish frame stands in until it arrives. The
restart of the animation on app-active and first layout goes ahead.

## Owner, 2026-11-17 about 00:10 to 00:14 (app chat), verbatim
"it had reduce motion on." — so the second iPhone's still fish was the Reduce Motion design, not a bug in the loop.
"we dont want a full still item for reduce motion on.. i am making a new loader icon that will replace the loading
circle on the splash page or curtain (it will still be used for all other loading." Then he sent the loader: a Python
generator (`fish_loader.py`, Pillow; a linocut spawning coho, 80 frames, 480×200, 8×10 sprite sheet, 20 fps, 4 s loop,
ink #1f2e3b + coho red #cf5a3c, same linocut style as the app icon). Then: "we will ignore reduced motion for this".

So:
- The still-icon slot (`stillIcon.ts`, cdbdfed16) goes; no still image anywhere.
- The new coho loader replaces the circling fish on the splash / first-download / curtain screens only; the circling
  fish stays for every other loading place.
- Both loaders ignore Reduce Motion: they always move. (The generator's still frame and its reduced-motion CSS are not
  used.)
- The restart fix (loops start again on app-active, first layout, a dropped loop) stays.
- The generator is committed under `app/tools/` and run to make the sheet, like `pnpm spinner`.

## Owner, 2026-11-17 ~08:45 (app chat), the dark-mode look, verbatim pick
Shown: light as built, dark as it was (ink lost on the dark screen), A (paper-coloured ink #f2e8d5 on the dark screen,
red kept) and B (his colours on a paper tile). Picked "A: paper-coloured ink". So dark.json `color.ui.coho.ink`
#F2E8D5, ground `@color.ui.card`; light unchanged.
