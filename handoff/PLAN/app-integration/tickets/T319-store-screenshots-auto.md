# T319 — Store screenshots that sell the app, generated automatically (for the real launch)

Status: IN PROGRESS — owner 2026-11-18 ~23:35 delegated the shots and titles ("I want you to add the screenshots to the
stores you can give them titles"). Set 3 picked and generated, both uploads prepared as dry runs (nothing sent). Left
(Claude): when the 0.8.2 UI is final, regenerate from it and upload (commands below). Before: OPEN — owner 2026-11-11: back in, part of the closed-testing goal (T495 step 5). Where it stood: IN PROGRESS 2026-11-05 — the generator is built (one command, every size); the shot list + titles wait for the owner; the saved-spot shot needs the phone build. Was: POSTPONED (owner 2026-10-31: "make it so the screenshots for promotion on app store are better … optimal pictures of
the app and how it is used. Making it look good? Then we can have a way to auto generate it? This is a ticket for later
don't start it now add to stuff needed for real deploy").

## Work (later, part of the production launch — T240)
1. A shot list that tells the story: find a water → today's answer → what you can keep → conditions → salmon → save a spot →
   offline. Real, attractive places (Lower Mainland / interior / north), good light/dark, no dev overlays, a clean status bar.
2. Framed store images: phone frame, short caption per shot in the app's fonts/colours, Play (phone + 7"/10" tablet) and App
   Store sizes (6.7"/6.5"/5.5" iPhone, iPad), feature graphic.
3. One command regenerates them all from the current app + release (emulator/simulator via the test bed, or the web app at
   device viewports), so each release can refresh the store listing.

Depends: T240, T244, T313. Size: M. Owner: picks the shot list/captions.

## Built 2026-11-05
- **One command:** `node app/tools/store-shots.mjs [--theme light|dark] [--only <targets>] [--no-shoot]` — runs
  `play-shots.mjs` on staging for every raw size the targets need, then frames each shot at the store's exact pixels:
  title + one line in the app's faces (`tools/seo/fonts`) and theme colours, the screen in a square card with the hard
  shadow. Refuses a missing raw shot, a missing font or token.
- **Targets** (`app/tools/store-shots.json`): Play phone 1080×1920, 7" 1200×1920, 10" 1600×2560; App Store 6.7"
  1290×2796, 6.5" 1242×2688, 5.5" 1242×2208, iPad 13" 2048×2732. `play-shots.mjs` gained `iphone67`, `iphone55`,
  `ipad13`, a `09-offline` scene, and seeds the mode-hint tours as seen ("Where you are" covered the bottom bar).
- **Shot list (PROPOSAL, owner picks):** map → search → today's rules → conditions → salmon → runs → offline, with titles
  in the json. 49 light images made 2026-11-05 in `store/graphics/store/light/` (gitignored, regenerate).
- **Left:** the saved-spot shot (the web Spots tab is a "get the app" notice → the emulator test bed, heavy); dark set;
  the feature graphic's text; the owner's pick of shots and titles.

## Set 3, 2026-11-18 (owner delegated order and titles; Claude's pick) — REPLACED by set 4 below
Order and titles (`app/tools/store-shots.json`; big title, then the small line):
1. `01-map` — **Can I fish here today?** Every B.C. lake and stream, coloured by today's rules
2. `18-regs-trout` — **Know what you can keep, on every B.C. water** How many, what size, and until when, from the rule book
3. `04-salmon` — **Salmon openings, on the map** DFO's freshwater salmon notices, river by river
4. `11-cond-chart` — **River flow, and where it is heading** The gauge against past years, with the forecast drawn on it
5. `05-runs` — **When the runs come** Run timing by species and river
6. `07-saved-spot` (device) — **Keep your spots** Your notes, with the flow and weather from that day
7. `20-help` (device) — **Works with no signal** Rules, the map and your spots stay on your phone. The map key is one tap away
8. `06-search` — **Find any water** Search every named lake and stream by name, or a town near you

Dropped from set 2: the river-split composite (on the current UI the Chilliwack's map half is hard to read and its rules
half was a closed ecological reserve) and the Change/Forecast composite.
**Light theme**: the basemap's labels and the status colours read best on it at store-thumbnail size, the earlier
approved sets were light, and the dark highlight look is still open (T735).

Sizes (targets): Play phone 1080x1920, 7" 1200x1920, 10" 1600x2560 (Play lists the app on tablets: the tablet sets are
optional but shown there); App Store 6.9" 1320x2868 (`APP_IPHONE_67`, the required iPhone slot) and 6.5" 1242x2688
(`APP_IPHONE_65`). Dropped: 5.5" (not required once 6.9" is given) and iPad (`supportsTablet: false`).

Spots and Help are phone-only on the web (a "get the app" notice; the web's Help says it needs a signal), so they come
from the iOS simulator: `app/tools/store-shots/device-ios.mjs` (status bar pinned 9:41, light, one seeded spot) writes
`device-<scene>.png`; the generator crops its status bar and fits it to each size's shape. Web scenes are shot from a
local web export of the commit under test (`STORE_SHOTS_SITE`), not whatever staging serves.

Regenerate (from the commit to ship):
```bash
deploy/staging/build-ios-sim.sh                                     # heavy (lane), simulator app of HEAD
TESTBED_SIM="iPhone 17 Pro Max" scripts/heavy.sh store-ios 30 -- bash app/tools/testbed/session-ios.sh --fresh -- \
  node app/tools/store-shots/device-ios.mjs handoff/PLAN/app-integration/store/graphics/raw
# web export of HEAD served locally (or staging once it carries HEAD), then every size:
STORE_SHOTS_SITE=<site> node app/tools/store-shots.mjs --theme light
```
Upload (Claude, when 0.8.2's UI is final; both scripts default to sending nothing):
```bash
.venv/bin/python deploy/staging/play-listing-push.py --plan --shots <store>/light/play-phone --tablet7 <store>/light/play-tablet7 --tablet10 <store>/light/play-tablet10
.venv/bin/python deploy/staging/play-listing-push.py --commit (same folders)
.venv/bin/python deploy/staging/appstore-screenshots-push.py            # plan; --check reads ASC; --commit uploads
```
`<store>` = `handoff/PLAN/app-integration/store/graphics/store` (gitignored).

Made 2026-11-19 00:10 from 6e16c087f (origin head: 0.8.1 + the 0.8.2 work landed so far): web scenes from a local web
export of it (clock pinned noon 11-19, live staging feeds), device scenes from its iOS simulator build. 40 images, both
plans printed clean (8 per set; nothing sent). Copy with the contact sheets and the two plan outputs:
`~/bcfish-session-backup-2026-11-12/store-t319-set3/`. Nits to look at on the final regen: the spot shows "Some days of
the week before were not available yet ... Refresh" (the seeded visit is 4 days old; it fills in later), and on the
tablets the device shots are phone pages on a tablet canvas (Play's tablet sets are optional; drop them if they look off). `appstore-screenshots-push.py` is new
(App Store Connect API, appScreenshotSets / appScreenshots; locale en-CA, the app's primary language; never run beyond
its plan yet). `play-listing-push.py` gained `--plan` (its old dry run uploads into an edit and deletes it) and the
tablet folders.

## Set 4, 2026-11-19 (owner: split screens back, more of them, conditions early, the video too)
Owner, verbatim (relayed by the coordinator; times off the clock): ~00:15 "We lost the split screen shots"; ~00:17
"Couldn't we combine more together?" (more features paired into split screens so every feature fits in Play's 8);
~00:19 "The first ones should include one about conditions also the preview we added for video ?"; ~00:28 "did you
upload the google play photos and description etc for me?"; ~00:30 "i think the screen shots you take with splits etc are
fine?" (so the split set goes up without another look, once 0.8.2 is on staging).

Seven shots, the same for Play (phone, 7", 10") and the App Store (6.9", 6.5"); a split = two real captures stacked, each
cropped to its useful part (per raw size, `at` in store-shots.json) under a dark caption bar, the 11-13 style:
1. **Can I fish here today?** — the map
2. **Know what you can keep** — the fish table
3. **What the rivers are doing** — Flow against past years | Change, next 10 days
4. **Rules for each stretch** — The river's sections on the map | One section's rules
5. **Salmon openings and run timing** — Salmon openings on the map | When the runs come
6. **Your spots, even with no signal** — A saved spot | Works with no signal (both from the iOS simulator)
7. **Find any water** — search
The optional 8th (the forecast map by day, `13-cond-forecast`) is out for now: on the current UI the scene finds no day
buttons. Recheck it on 0.8.2 and add it only if it reads at thumbnail size.

Layout rendered on the current staging for the owner (not the final set): `~/bcfish-session-backup-2026-11-12/store-t319-set4-layout/`
(`T319-set4-phone.png`, `T319-set4-all.png`, every framed image, `play-plan.txt`, `asc-plan.txt`). Nits for the final
crops: the spot's notes are cut at the pane's foot; the 6.9" change map is mostly the grey south of the border.

Preview video (T709): `~/bcfish-session-backup-2026-11-12/store-video/preview.mp4`, retaken on 0.8.2 with the shots
(`node app/tools/store-video/run.mjs`). Play: the listing already holds a video (youtube Qq_aptl3wJ8, seen on Play
2026-11-19); a new clip goes to YouTube by the owner (unlisted is fine), then `play-listing-push.py --video-url <link>`.
App Store: `appstore-screenshots-push.py --preview <clip>` checks it; today's clip misses Apple's rules (864x1920, the
6.9" slot takes 886x1920; its silent AAC track is 2 kbps, the spec says 256 kbps) and it is an Android recording (the
owner's reading of Apple's page is footage from the iOS app). No preview is in the ASC plan.

Play listing text (coordinator relayed the go, 2026-11-19): `store-description.md` checked: "we recommend" -> "I
recommend" (owner 2026-11-18 "yes change all"); every feature it names is in the 0.8.2 code. `play-listing-push.py
--no-shots` (new: text, icon, feature graphic only) validated on Play: en-US only, the change is the full description;
icon and feature graphic unchanged. The `--commit` was refused by this session's permission check (an agent's relay is
not the owner's go); not pushed. Owner command, from the repo: `.venv/bin/python deploy/staging/play-listing-push.py --no-shots --commit`.

Later (after 0.8.2 is on staging): render set 4, upload to Play and App Store Connect, retake the clip, send the contact
sheet of what went up.
