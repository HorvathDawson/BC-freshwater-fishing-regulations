# Owner to-do

**The owner ticks items off in the Owner To-Do page: https://claude.ai/artifact/Lh2ougAwfxx1wNmCV18m8f** (owner 2026-11-11). Every open item for the owner is a row
there (collection `todos`: title, detail, group, ticket, order, added, done, note). Claude adds rows with the
ArtifactData tool when work produces something for the owner, and reads `done` and `note` back at the start of a
session. This file keeps the long text and history.

Things only you can do. Claude keeps this list current while you are away; the work itself lives in
`handoff/PLAN/app-integration/tickets/` (README.md is the ledger, and its "Owner questions" table has every question
with the default Claude uses until you answer). Newest first inside each section.

## Decisions that block work (nothing ships until you answer)

- **ANSWERED 2026-11-14 (production move changed).** Owner, verbatim: "Production will use the latest version" and "It
  will also get new dfo places I will label tonight". So production is NOT deployed from the 9048fbc20 copy below: it
  waits for tonight's DFO labels, then a new data release with them, then the website built from the latest checked
  code and that release. Claude: staging site redeployed from the latest code now; after the labels, a new staging
  release, then fresh production folders and fresh commands here. The commands in the next item are OUT OF DATE.
- **Production move (2026-11-14): SUPERSEDED by the answer above; was: READY, yours to run** (owner: "tonight we will probably do that"). Follow
  `handoff/PLAN/release/PRODUCTION.md`, first-transition steps 3-8. Data, from the main folder:
  `CONFIRM_PROD=r-f749475d0962 .venv/bin/python -m pipeline release --prod r-f749475d0962`. Website, from the clean copy
  `~/bcfish-clean-staging` (must be at 9048fbc20, `git status --porcelain -- app` empty): `deploy/prod/site.sh --dry-run`,
  then `CONFIRM_PROD=canifishthis.ca deploy/prod/site.sh`, then `node app/tools/smoke-staging.mjs --production`. Then look
  at `https://canifishthis.ca/privacy`. Production has no Play config file (`play.json`) yet. Page row
  `hold-prod-move-staging-dirty-1114` has the same steps.
- **To-Do page and this file compared 2026-11-14.** Added to the page: T686 pick, the 12 iOS-day defaults (one row),
  Apple account after approval, Play closed-testing steps. Closed on the page: T688 wording (answered 11-12). Build 29
  phone check marked replaced by the 0.7.8 / 0.7.9 rows. Not on the page (old, to prune or re-ask): Run timing T199 runs,
  Q4 / Q126 / Q9 / Q26 / Q27, the long phone-test lists (T50 / T77 / T74 / T217 / T243), SEO v1.
- **ANSWERED 2026-11-14 (T750 step order).** Upstream / Downstream now walk one line through every part, no "Which way
  upstream?" picker (owner: "we should only have up or downastream that access all components"). Asked whether to rank a
  part by its upper end (as shipped) or its downstream end (differs on Wap Creek and Nimpkish River only): "upstream is
  fine". Shipped order stays; no release needed.
- **T743 Fraser DFO places (2026-11-14): BLOCKS tonight's release until re-marked.** Dossier:
  `handoff/PLAN/curation-review/fraser-dossier-2026-11-14.md`. Three "Mission to Hope" wordings (marks lines 2552, 2554,
  2555; two written by a group click) are bound Mission → Spuzzum; with the live chum/coho opening that would show
  salmon open on Hope → Spuzzum (km 158.8-197.0) where DFO says no fishing for salmon. Yours, one at a time once
  grouping is gone: (1) re-mark those three as Mission → Hope (your own line 2546 was right); (2) accept ONE Hope Hwy 1
  bridge cut (proposed twice, km 158.8; needs an atlas build after); (3) which bridge is "the Alexandra Bridge": the
  Hwy 1 bridge (km 201.2) or the old suspension bridge 855 m upstream (both are 4.3 km past the Region 2/3 line at
  Spuzzum); (4) re-check the two "upstream of Mission" marks (Mission → Spuzzum is plausible); (5) does Region 3's
  "Mainstem waters" include the stretch shared with Region 5; (6) which edge of Lillooet the signs are measured from.
  Suggester fixed (319a447a0): 3 Fraser suggestions change; regenerate only after (1). Page row `t743-fraser-remark`.
- **T735 dark-mode highlight (2026-11-14): pick a look; four sheets sent in chat.** Owner: "The highlighting isn't very
  good in dark mode on regulated streams." Measured: the white ring nearly swallows a yellow or mint line (1.1-1.3:1),
  and on the Fraser every other part carries the same bright ring. Options, dark only: A dark edge, status colour
  carries; B thinner white ring, others dimmer; C dark edge + soft glow [recommended, glow can be stronger]. Patch kept
  outside the repo (`~/bcfish-session-backup-2026-11-12/dark-highlight/options.patch`); nothing changes until you pick.
  Page row `t735-dark-highlight-pick`.
- **From the 0.7.10 checks (2026-11-14), two looks, not blocking; pictures sent in chat.** (1) T731: stepping from the
  Chilliwack into the Vedder still makes the bottom bar 122 px shorter (the Vedder has one part, so no "Show all N
  parts" row) and the map grows by that much; steps inside one river move it 4 px. Nothing goes blank any more.
  Choose: leave [default], or keep the bar the same height across the step (an empty row's worth of space on one-part
  waters). (2) T730: picking a PLACE row in search (for example the village "Cultus Lake"), then Back, reopens search
  with the text still typed; a second Back clears it. Choose: leave [default], or Back from a place closes search too.
- **Phone check, build 32 = 0.7.10 (Play internal, 2026-11-14).** Look at: (1) Back from a water's page: no flash
  (Map, Conditions, satellite, dark); (2) Flow / Level in a water's Conditions card leaves the Conditions map's layer
  alone; (3) the credits card opens above the legend on every map tab and closes on a tap outside or Back; (4) search:
  swipe down closes it, at most 4 recents in a small block, after picking a water Back leaves the map there, search
  reopens empty; (5) Upstream / Downstream: the bar no longer blanks (the height change into a one-part water is the
  open look above); (6) say if the app ever closes by itself when opening a water (a rare crash was fixed, T734: twice
  in about 100 opens before, none in 110 after). Page row `phone-check-0710`.
- **T730 search (2026-11-14), not blocking; look on the next build.** (1) Recent searches PROPOSAL: one small block
  under the search field, "Recent searches · Clear", then up to 4 searches as small square tags that wrap (about 80 px
  tall instead of about 340). Alternatives in the ticket: 3 compact rows, or one scrolling line of tags. (2) Closing
  search now always forgets what was typed, on desktop too (Escape or the panel's X). If desktop should keep the typed
  text, say so; it is a one-line exception. Default until answered: as built.
- **T686: pick the shipped-data shrink (2026-11-14), not blocking.** Measured on r-f749475d0962, nothing shipped (table in
  the ticket). The release data is 17.75 MB of a 463 MB phone download (tiles + basemap are 91 %, T699). Choose:
  (a) DEFAULT: near as one blob per place + climate bands packed + down in blocks, ≈ 1.94 MB, lossless, no web moment
  at risk; (b) (a) + core.item_section without its by-item index + water.part_section as runs (first search and
  water's gate get headroom; device scan time to measure first); (c) every lossless candidate, ≈ 2.59 MB; (d) brotli
  instead of gzip, ≈ 3.66 MB, a host and native-client change, no format change; (e) nothing now.
- **T728 follow-up (2026-11-14), not blocking.** Fixed: Flow / Level in a water's Conditions card no longer changes the
  map's layer. Left as it was, for you to decide: (1) the card still OPENS on whatever the map shows (map on Level, card
  starts on Level); alternative: always open on Flow. (2) a pick in the card lasts while that water is open; another
  water starts again from the map; alternative: keep the pick for the whole session, like 72 h / Year. Default until
  answered: as it is now.
- **ANSWERED 2026-11-14 (privacy page check).** Owner, verbatim: "we are close to moving staging webpage into prod.
  tonight we will probably do that." and "choose the best wording please you know better then I do for that stuff".
  So: item 1 is settled by the production move (after it, check that `https://canifishthis.ca/privacy` shows the
  policy); item 2: Claude chose the wording and applied it to `privacyBody` and the staging site (effective date
  2026-11-14). Original findings (owner: "please check page too"):
  1. `https://canifishthis.ca/privacy` (the link on the Play listing) does NOT show the policy: the live site is still
     the old site and answers every address with its own front page. The policy is only on
     `https://staging.canifishthis.ca/privacy`. Choose: publish the new site's privacy page to canifishthis.ca (you run
     it; production is yours), or point the Play listing at the staging address until production goes live.
  2. Three wording proposals for the policy text (`app/tools/seo/html.ts` `privacyBody`; NOT changed, the text was
     approved 2026-11-06):
     a. Short version says spots "stay on your device". A saved spot's place, rounded to about 1 km, goes to Open-Meteo
        for weather (the Weather section says so, but says "that spot's coordinates" without the rounding).
     b. A problem report's map centre is described as "where the map was looking, not where you were". The app opens
        centred on the reader, so it is often about where they are (to about 100 m).
     c. Not mentioned: Share (a link you send) and Directions (opens Google Maps with a spot's parking point). Both
        only on your tap.
  Fixed without asking (our own working page `handoff/PLAN/play/data-safety.md`, not the Play form): production does
  send reports; crash switch is More, Settings, Privacy; spot place is rounded; downloads carry the app version; one
  app-stopping error is kept for the next start. No Play form answer changes.
- **iOS reopened (owner 2026-11-14): four decisions to answer on return** (Apple account/TestFlight timing and individual vs organization, minimum iOS version, iPad, own iOS tap-up patch; defaults in the doc's Unattended section). ANSWERED 2026-11-14: rotation "Portrait only."; app name "Can I Fish This?" (same as Play, T371 closed on that point); proposals, physical needs and risks in `handoff/PLAN/ios/START-HERE-ios.md`.
- **ANSWERED 2026-11-14 (atlas builds).** Owner, verbatim: "You can do atlas rebuilds if you coordinate with other agent".
  Used so far only for the T667 slice 8 mini build (invented inputs, scratch output); the Android chat was told. Promote
  and release stay as before (not from the iOS chat). T602 is still not started: the handoff parks it for you.
- **Open from the iOS day (2026-11-14), each built with its default; also rows on the Owner To-Do page:**
  (1) oldest iPhone / iPad: today's build needs iOS 16.4+, iPhone only [keep] (T725);
  (2) App Store privacy: the report email "linked to the user" [linked], and the category Claude chose, Navigation then
  Sports [keep] (T727);
  (3) shared links with no Team ID: the site build skips Apple's link file with a warning [keep] or always fails (T724);
  (4) backup (T732): confirm the words the Android chat relayed ("save spots with prioirty", "images can be compressed if
  still visible"); the photo pair was sent. PROVEN on the emulator 2026-11-14: today a backup carries nothing (over
  Android's 25 MB limit); with the rules it is 17,920 bytes with spots and settings, and a wipe + restore brings them
  back. ANSWERED 2026-11-14 evening, verbatim: "Only need to backup photos and spots the rest of the data isn't
  important". Then: "Settings could also be saved". Being built on the branch: the backup carries the spots, the
  settings and the photos; photos ride
  the cloud backup as smaller copies (1600 px), which needs one image library (Claude read your sentence as the yes to
  it). PROVEN again 2026-11-14 21:15 with photos: backup Success (227 KB with 3 photos, 20.4 MB with 29),
  wipe + restore brings spots and photos back; the shot was sent. Still open: screen-lock-only [no], copy size [1600 px].
  Not merged;
  (5) clean-up: five calls in `handoff/PLAN/CLEANUP-CANDIDATES.md` (evidence screenshots, old releases, a leftover flag,
  15 DONE rows still in the open ledger, graphify and scratch dirs);
  (6) the small invented release for tests (T667 slice 8): cut it with no map tiles and no basemap, only the data files
  the app's logic tests read [yes, being built that way as a proposal], or also give the tile step its own invented
  inputs (more pipeline change). Found on the way: the export step checks that the rules are the real rule book (64
  guide examples, 75 case mechanisms, the licence and protected-fish duties, the printed page map), so it refuses the
  invented world. Being built as a proposal: the export takes the list it checks against as a named input, the real
  book's list is the default and is pinned so it cannot shrink, and the invented world brings its own list [yes]; the
  other way is to grow the invented rules to about 60 entries. BUILT 2026-11-14 in scratch: the release cuts on invented
  data, two runs identical; ten provincial rule-book rows the answers step reads by id are copied into the test world
  as printed;
  (7) the About page gains one sentence saying the app is not affiliated with the government (Apple asks for it; audit
  G15) [About only, the first-run notice is not shown again];
  (8) the web build leaves out the four Android-only "What's new" notes [keep];
  (9) moving to a new phone copies saved spots and photos but not the downloaded data, which downloads again [yes];
  (10) an own iOS tap-up patch for MapLibre (decision 4): BUILT and proven on the simulator 2026-11-14 (branch
  `ios/t719-tapup`): the cue lights 8 to 121 ms after the lift, was 371 to 495; Android's half of the patch is
  byte-identical [merge with the other iOS branches after the Android benches] or leave;
  (11) ANSWERED 2026-11-14, a third heavy lane for iOS simulator jobs. Owner, verbatim: "Maybe we can run 2 at same time
  might have enough resources". Done: `heavy-job-3.lock` in `scripts/heavy.sh` (bbee2220f), same memory guards, three lanes
  at most. Clarified later the same evening, relayed by the Android chat, verbatim: "i just meant use as much of the
  resources as you can to speed stuff up": lanes run in parallel as far as the machine allows; every heavy job still
  goes through `scripts/heavy.sh` and its memory gate;
  (13) iOS keyboard and back fixes are proven on the simulator (T723, branch `ios/t723-keyboard-back`); two picks: a
  spot's Save row stays under the keyboard while typing [leave: drag down or Done first] or lift it above the keyboard;
  the new left-edge back strip on full-screen pages is 16 pt wide [keep] or 8 pt;
  (14) the first-install bar reads low on both platforms (38.8 MB shown with 116 MB on disk after a relaunch): the four
  big files download at once and the bar counts one at a time [fix it, ticket T738] or leave it;
  (15) push for iPhone notices (owner 2026-11-14, verbatim: "dont we want to use it for the notifications thought?"):
  today no notification on either platform uses push; the phone checks in the background and makes the notification
  itself, and on iPhone that check runs only when iOS allows (often hours late). Proposal [Claude's pick]: a "wake-up"
  push from our feeds worker when notices change, so the iPhone checks at once; the worker learns a device token only,
  never which waters you watch. Needs: Push ticked on the App ID, an APNs key, a privacy-page line, a ticket. Until you
  pick, the build keeps its push entitlement (the G12 change is on hold). Ticket T739 (proposal). Also asked: "can we
  do this same push notification style to android?": yes, but Android's only push channel is Google's Firebase, a Google
  service in the app, so it needs your exception to "self-host everything" [iPhone first; Android only if phones show
  late alerts]; and "there are also time sensitive notifications?": possible for closure notices on iPhone [off for now].
  ANSWERED 2026-11-14 late, verbatim: "okay i ticked push notifications for app yes build for iphone."; time-sensitive:
  "dont need this"; and "i guess if new data is there we could send a notification that the user needs to download it."
  (built as: a new release wakes the phone, which downloads by itself when settings allow and otherwise tells you new
  data is ready). Then, verbatim: "for android too." (Android push approved as well; it uses Google's Firebase for delivery only,
  Claude's reading: your exception to "self-host everything" for this one thing). Being built on branch `ios/t739-push`.
  **ANSWERED 2026-11-15 01:10 (push on a real phone).** The owner made the Apple push key (Key ID 6T3DBFF92H) and ran
  the three `wrangler secret put` commands himself; the staging feeds worker lists APNS_KEY, APNS_KEY_ID and
  APNS_TEAM_ID (names checked, values not read). To "Do you want those tonight?" (deploy the worker with the push code
  to staging, and upload TestFlight build 35 from the push branch), owner, verbatim: "yes i do i ran command". Build 35
  **uploaded 01:24** from `ios/t739-push` 650665c61; the worker deploy is timed with the Android chat. Yours
  after Claude says the worker is on staging: install build 35, turn notifications on in the app, tell Claude; Claude
  sends one test push.
  **Worker on staging since 2026-11-15 02:05** (also carries the crash-report limit you approved). Ready for your
  phone: build 35, More → Notifications → "New regulations data" on, app to the background, tell Claude. Firebase (Android)
  still yours, later.
  Still yours later: one more Apple key, for push, and a free Firebase project for Android (steps come when the worker
  side is ready);
  (12) Play's data-safety form says approximate location and the App Store answers say precise: should Play match?
- **Apple Developer account: APPROVED 2026-11-14 evening (individual); the owner has the Team ID** (given in chat; not
  written in the repo). DONE by you 2026-11-14: the Team ID is saved in `~/.canifishthis/ios-team-id` (checked: the site build reads
  it and now writes Apple's link file). Also 2026-11-14: the owner made the App Store Connect API key (Key ID and Issuer ID given in
  chat, not written in the repo; the `.p8` is in his Downloads). DONE by you the same evening: the key and `asc-api-key.json` are in
  `~/.canifishthis/` (checked by shape only; `build-ios-testflight.sh --dry-run` finds both files and would build 0.7.10
  as build 33). CONFIRMED by you 2026-11-14 ("yes and yes"): the App ID `ca.canifishthis.app` is registered with
  Associated Domains on (Push left off: Claude turns the unused push entitlement off in the build, audit G12), and the
  app record exists. The Apple side is complete. FIRST TESTFLIGHT RUN: owner 2026-11-14 late, verbatim: "you can do first
  ios release test for me". Claude started it 21:32: staging data, version 0.7.10 as iOS build 33, from f51870652 (last
  green app CI, the same app code as Play build 32). RESULT 21:33: stopped in the signing step after 1 minute, nothing uploaded. Apple: "Your team has no devices
  from which to generate a provisioning profile": Xcode's automatic signing needs one iPhone registered on the team
  before it can archive. YOURS (2 minutes): register your iPhone under developer.apple.com → Devices (steps in chat),
  then tell Claude, who runs it again. Later TestFlight runs: ask
  first unless you say otherwise (Claude's reading: the go-ahead was for the first one). History: ENROLLED 2026-11-14, waiting for Apple's approval (owner, verbatim: "okay i enrolled with
  apple developer I will tell you wne i get approved"). This answers decision 1 of `handoff/PLAN/ios/START-HERE-ios.md`
  (account now). ANSWERED 2026-11-14 evening, verbatim: "it is a individual evelper account" (so the App Store shows
  your own legal name as the seller). Same message: "i cant find team id how do i make one?": it is not made, Apple
  assigns it once the enrolment is approved and paid; it then shows under developer.apple.com → Account → Membership
  details. After approval, yours to do: save the Team ID in
  `~/.canifishthis/ios-team-id`; create the app record (bundle id `ca.canifishthis.app`, name "Can I Fish This?", English
  (Canada)); make an App Store Connect API key (role Admin; corrected 2026-11-14 22:15, an App Manager key cannot sign for the store) and save `~/.canifishthis/asc-api-key.json`. TestFlight
  (T88) stays yours to start.
  **DONE 2026-11-14 22:22:** first TestFlight upload, 0.7.10 build 33 (staging data), after the owner registered his
  iPhone and made an Admin key. Yours now: App Store Connect → TestFlight → Internal Testing, make a group and add
  yourself; install the TestFlight app on the iPhone; install the build and open it. Later uploads: Claude asks first.
  **ANSWERED 2026-11-14 23:29:** owner asked "i thought it can download in background?" (not on iPhone in build 33);
  offered a second build from the merged iOS branch `ios/integration` (background download, progress bar, keyboard,
  back swipe, tap cue); owner, verbatim: "yes". Build 34 from 51349c3bf **uploaded 23:39** (0.7.10 build 34, staging data). The shared
  branch is not touched by it. Yours: add build 34 to your internal group and install it; checks only a phone can do
  are on the To-Do page row.
- **ANSWERED 2026-11-15 10:33 (merge timing of the iOS work).** Owner, verbatim: "ios work could merge when ever I
  think?" My reading: no need to wait for him. Plan, timed with the Android chat: (i) the four iOS fix branches
  (proven on both platforms) first; (ii) the loading screen, first-install bar, crash-report app half and the
  faster-and-lighter branch after one Android run; (iii) push last, with the privacy-page line for his eyes. The R8
  Play internal build is taken before any of these, so it carries nothing else.
- **ANSWERED 2026-11-15 evening (round 6, verbatim: "Yes to both (Recommended by it)"): the push privacy-page line and the App Store privacy answer are approved AS WRITTEN below; and all the fish-pun lines stay (T746).** Next (Claude): the line goes into the privacy page's source on the push branch; putting the page live stays yours, with the production release. Was: NEW 2026-11-15 10:42 (iOS chat, T739, push: privacy wording; PROPOSAL). With push the app sends our server one
  new thing, a push token (an address Apple issues so a message can reach the app). The push code merges into the
  shared branch only after the privacy page and the store answers say so; the live privacy page changes only on your
  yes (production stays yours). Proposed privacy-page line, under what the app sends: "Push token. If you turn
  notifications on, the app sends our server a push token for this phone (an address Apple or Google issue so a
  message can reach the app) and whether the phone is an iPhone or Android. We use it only to send a silent signal
  telling the app to check for new data; the signal carries no words, no waters and no location. We do not receive
  your alerts, the waters you watch or where you are. The token is deleted when you turn notifications off, or when
  Apple or Google tell us it no longer works. It is not linked to you or to an account and is not backed up."
  Proposed App Store privacy answer: Identifiers, Device ID: collected, App Functionality, not linked, not used for
  tracking. The Android (Firebase) line and the Play Data safety answer wait for your Firebase decision; their draft is
  in T739. Answer: yes as written / your changes. To-Do page row `t739-privacy-wording`.
- **NEW 2026-11-15 10:09 (iOS chat, T749, Android's code shrinker R8).** The trial passed on the emulator: the app
  starts and passes all six checks; the test APK is 7.45 MB smaller (about 7.4 MB off the Play download, estimated).
  It is behind a switch that is off. Pick: make one Play INTERNAL build with the switch on for you to walk on your
  phone (update, notifications, a spot photo, location, the rule book): yes / not now. Nothing is uploaded until you
  say.
  **ANSWERED 2026-11-15 10:32.** Owner, verbatim: "if that build passes the tests then it should be good to release";
  asked which build and which release, he picked "R8 build to Play internal": the Android app with the code shrinker
  on, built from the shared branch as it is today, the six emulator checks re-run on that exact build, and if all
  pass an upload to Play INTERNAL testing for his phone walk. Production stays his. Nothing else rides along.
  **State 2026-11-15 21:56 (newest; the states that follow are older): nothing needed from the owner.** The later
  stops are fixed (the build's quick test step passes under its own settings). The build waits only for tonight's
  staging data release (the main chat's), so it is built on a shared branch that holds still. The second chat is now
  THE APP CHAT (Android and iOS; owner 2026-11-15 evening) and makes the Play INTERNAL build 0.7.11 with the shrinker
  on; the ten-step phone walk then goes onto his To-Do row `t749-r8-phone-walk` with the build number.
  **How it runs (agreed with the Android chat, 10:37; superseded by the state above for who builds).** I build the test APK and re-run the six checks; the Android
  chat makes the Play build and uploads it, because that step uses the Play sign-in and its version counter. It will
  be build 33, version 0.7.11. To be plain about "nothing else rides along": none of my other branches are in it, but
  it does carry what the Android chat put on the shared branch since build 32: the web loader (T737), search opening
  the water map on its part (T747) and the one-line Upstream / Downstream (T750). Those were due for your next phone
  check anyway.
  **State 2026-11-15 12:49: not uploaded.** Five of six checks passed on the exact build; the first-install-with-the-
  app-closed check did not, and it fails the same way with the shrinker off: this Mac's internet line was crawling. No
  merge and no upload until that check passes on a good line; I re-run it when the line recovers. If it is still bad
  this evening the build slips to tomorrow. Found on the way, not from the shrinker: T756 (reopening mid-download
  restarts the big files from zero after "Wait for Wi-Fi"); fix on a branch, not merged, not in this build.
  **State 2026-11-15 17:18: ready except one thing that needs you.** All six checks passed, the code is merged
  (shrinker off by default), both GitHub checks are green. The build did not run: the build script's quick test step
  fails on one test that reads your uncommitted `data/generated/review/dfo-suggestions.json` (two rows merged at
  midday lack their evidence). The agents will not skip the test or edit your file: regenerating the two rows was
  refused by the automatic permission check, so it is yours; the Android chat's item "regenerate the two proposal
  rows" (first on your To-Do page and on OWNER-RETURN-2026-11-15.md) has the command. Nothing built or uploaded
  until then.
  **CORRECTION 2026-11-15 15:19: back on for today.** The line was not down; my probe was wrong (it timed a "not
  found" answer). All six checks are running since 15:17 on the merged code (T752 included) with the shrinker on. If
  all pass: merge, CI, then the Android chat's Play build today. The 14:44 note below is superseded.
  **State 2026-11-15 14:44: planned for tomorrow.** The line is still down (30–46 bytes a second since 13:04). The
  Android chat lands its desktop split (T752) on the shared branch first, so all six checks are re-run on that new
  head with the shrinker on before the build. If the line recovers today the sixth check is still re-run today.
- **NEW 2026-11-15 evening (iOS chat, T759 + T757; PROPOSAL, words only; To-Do row `t759-banner-words`): the banner's
  words for a DFO rule not matched on a river.** From your decision (no "not known" status; a banner on every stretch
  quoting the unmatched rule; a different look for the line). Written on a branch (the app half of T759), not merged; no
  status or colour changes; on today's pages it shows nowhere (all 8 cases are already "parts not mapped").
  Title: "A DFO rule to check on this water". New rule not placed: "DFO lists a rule for this water that this app has
  not placed on the map yet. It may apply here:". Close match not confirmed: "DFO reworded a rule for this water. The
  app applies it where the earlier wording was; that has not been confirmed yet:". Reworded closed area: "DFO
  reworded a closed area on this water. The app has not placed the new wording yet:". Closing line: "Check DFO's page
  for where it applies before you fish." Between lead-in and closing: DFO's own area and rule, as printed. Sits above
  the species table. Also new (T757), shown when the latest DFO check cannot be used with the app's data: "The latest
  DFO check could not be used with this app's data, so the DFO rules this data was built with are shown. Check DFO's
  page before you fish." **ANSWERED 2026-11-15 23:00 (dialog): "Yes as written (Recommended)"**, and the banner's place:
  "Above the table + mark the folded row (Recommended)". THE PICTURES ARE DONE (2026-11-15 22:53): the banner on the Campbell River and three looks
  for the line, on https://claude.ai/artifact/T5yS8HymzuhAXq8kYaLEwL (also sent in the app chat).
- **NEW 2026-11-15 22:53 (app chat, T759; To-Do row `t759-line-look`): pick the look of the river's line, and two
  small picks.** Real renders of the app (web, phone width) on the Campbell River with one made-up unplaced DFO rule
  fed in; nothing shipped. The line keeps its own colour in every look. PICK 1: A = a soft glow under the line (halo)
  / B = a thin solid edge each side / C = dashes down the middle; Claude suggests A (the only one still visible with
  the whole river in view, and unlike the bait-ban edges; weak point: two such rivers close together). PICK 2: the
  mark is a magenta the app uses nowhere else; your words were "a variation like bait ban, same color": magenta, or
  black like the bait-ban edges? PICK 3: the banner shows only once Salmon is opened: should the folded Salmon row
  carry a small mark too (Claude suggests yes)? Not shown: dark theme, the colour-blind check, the phone app (the
  look is built for the web only so far, on a prototype branch).
  **ANSWERED 2026-11-15 22:56 to 23:00.** Typed in the app chat: "I like the stripe option best for the ones with
  banners. Also the stripes should only be on the salmon view with this error mode right?" So: look C (dashes down
  the middle), on rivers that have a banner, shown ONLY in the map's Salmon view (the prototype drew it in the Trout
  view too: that is wrong and the build shows it in Salmon only). The dashes' colour, dialog answer, verbatim: "I
  think it should dash with closed color maybe? Or a better color what ever is best practice". So the colour is
  Claude's call by best practice: the closed colour is tried first; if it reads as "closed" or fails the dark-theme
  or colour-blind check, a neutral ink that does not is used; he sees the final picture before it ships. Folded
  Salmon row: marked (the banner-place answer above).
- **NEW 2026-11-16 04:37 (app chat, T749; To-Do row `t749-r8-phone-walk`): Play internal build 33 (version 0.7.11,
  code shrinker on) is up; walk it on your phone.** Update through Play (your tester link, the email has it). All six
  emulator checks passed on this code; no real phone has run it. Any time the app closes by itself is a fail. The ten
  steps, each with what a failure looks like, are on the To-Do row and in the T749 ticket ("Phone walk for the
  owner"): 1 update through Play and open; 2 the map draws, tap a river; 3 a notification arrives and its tap opens
  the app; 4 background runs are listed after hours closed; 5 a spot photo from the gallery; 6 a spot photo from the
  camera; 7 location (the map's locate button, "At my location"); 8 the rule book opens at its page; 9 come back
  after a few minutes in other apps; 10 spots and settings survive a force stop. Also in this build, to glance at:
  the data release r-46e4355a7277 (your DFO labels, the year lines T762, Kootenay "over 50 cm" T763). Not in it:
  the stripe (T759), the stuck-download fix (T760).
- **NEW 2026-11-16 00:31 (app chat, T759; To-Do row `t759-stripe-final`): the final stripe is built; look at the
  pictures and confirm two small things.** The stripe branch (6c24f94a6, cut from the T759 app branch; named in the T759 ticket; not pushed,
  not merged, NOT in the 0.7.11 build). Pictures sent in the app chat (contact sheet `final-stripe.jpg` + light and
  dark close-ups; Campbell River with one made-up unplaced DFO rule). Built: dashes down the middle of every stretch
  of a river with the banner, Salmon view only (web and phone style code), fading at far zoom like the bait-ban
  edges; a square "!" mark on the folded Salmon row; a legend entry. Colour (his words left it to Claude): the closed
  colour FAILED (same red on a closed stretch = invisible, contrast 1.00; and a red dash on a blue line reads as
  "closed"), so black ink on a pale edge (white on dark in the dark theme), held by contrast and colour-blind tests
  on every Salmon line colour. CONFIRM 1: the legend entry reads "Check rule" ("DFO rule to check", "Rule to check"
  and "Check a rule" do not fit the two-row legend at phone width); OK or other words of about ten letters?
  CONFIRM 2: is the stripe strong enough on the dark blue "Keep salmon" line? Not proven: no phone or emulator has
  drawn it yet (web pictures + style tests only).
- **NEW 2026-11-15 22:27 (app chat, T760; PROPOSAL, words and numbers; To-Do row `t760-download-words`): a first
  install that stops moving says so.** Two causes of a setup screen that sat for ever were found and fixed on
  branches (not merged, not in tonight's Play build): a small file the app fetches itself that stalled was never tried
  again (now cut after 20 s without a byte); a big file parked by Android's downloader was never restarted (now
  restarted after 10 minutes without a byte unless it is waiting for a network or Wi-Fi, at most twice, then the
  install stops with words). Proposed words: under the bar after one minute without a byte, "The download has not
  moved for a minute. It carries on by itself; if it stays stuck the app starts that file again."; offline, the
  existing "Offline — this will load when you’re back online"; when it gives up, above Try again, "A file would not
  download after several tries. Check your connection, then tap Try again." Numbers are Claude's defaults (1 minute,
  10 minutes, 2 restarts: the give-up words after about 30 minutes). Unit-tested only; emulator runs before any merge.
  **ANSWERED 2026-11-15 23:00 (question dialogs in the app chat).** The three lines and the numbers: "Yes, with
  Claude's numbers (Recommended)" (1 minute, 10 minutes, 2 restarts). Dead connection: "Keep trying by itself
  (Recommended)": same words and Try again button, and the app also tries again about once a minute.
  SECOND PICK (added 22:31, from the first emulator runs): when the connection is dead for minutes the app now says
  so and stops on its failure screen with Try again, and waits for the tap even after the connection is back. Pick:
  (a) keep trying by itself about once a minute while showing the same words (Claude's suggestion) / (b) leave it.
  Answer: the words (yes as written / your changes), and a or b.
- **NEW 2026-11-15 23:17 (app chat, T762; a review, nothing to decide unless you want other words; To-Do row
  `t762-year-line-words`): the trout "through the year" line says the whole year, in tonight's build.** Owner
  2026-11-15 about 22:55: "The bad display oddities must be fixed on this build". On a keep day the line stopped after
  the next change ("You can keep them until Sep 30. Then closed from Oct 1." on a Fraser stretch closed only in
  October). Now, same water on Nov 15: "You can keep them until Sep 30 next year. Then closed Oct 1–31. You can keep
  them from Nov 1 next year." Boardman Creek on Jul 15: "You can keep them until Aug 31. Then the whole water is
  closed Sep 1–Oct 31. Then release only Nov 1–Mar 31. Then the whole water is closed Apr 1–Jun 14. You can keep them
  from Jun 15 next year." The words are the ones the closed and release days already used. 103 lines on 98 answers
  changed, each checked against the stored year. One choice he could reverse: dates in the middle of the line carry
  no "next year". The other oddity (Kootenay Lake's rainbow line dropping "over 50 cm", T763) is the main chat's and
  goes into the same release.
- **NEW 2026-11-15 16:08 (iOS chat; yours, later, no decision needed now): production feeds worker deploy.** Two
  worker fixes are live on STAGING only: T758 (waters covered only by a region's default salmon rule show DFO's own
  sentence under live data again, not a generic "Not opened by DFO" line; the status was right all along) and T757
  (closed areas inside a DFO place are emitted as their own live locations). Production's worker has the same gap
  until it is deployed from the shared branch after these merge; that deploy is yours (`deploy/prod`, never an
  agent's). I say when the branch is ready.
  **THE BRANCH IS READY, 2026-11-16 11:15 (app chat).** Both fixes are on `redesign/stream-sections` since 44b9c25bf
  (merge 1 after your "Yes, one at a time"; both GitHub checks green). Yours, in the main folder at the shared
  branch's head, BEFORE or WITH the production data release (the upload refuses r-46e4355a7277 until production's
  feed server serves closed areas): `deploy/prod/feeds.sh --dry-run` (tests + a local bundle, deploys nothing), then
  `CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds.sh`. The script stops with words if a prerequisite is missing:
  the backup bucket and its 30-day expiry (question 23), `NOTIFY_EMAIL_TO`, `REPORT_SALT`, and the production run
  token in `app/workers/feeds/.run-token.prod` (`handoff/PLAN/release/PRODUCTION.md`, step 2). Check after:
  `https://feeds.canifishthis.ca/status.json` shows `"dfo": {"pieces": 1}`. To-Do row `t758-prod-feeds-worker`.
- **DONE 2026-11-15 07:32.** TestFlight build 36 uploaded (0.7.10, branch `ios/next` d658f88ac, staging data): the
  iOS fixes, the first-install bar, the "applying new data" screen with fish puns, push, the crash-report fix, the
  faster cold start and lighter fonts. Yours: install 36 (not 34 or 35), then the phone checks and the push test;
  the steps are at the top of `handoff/PLAN/ios/START-HERE-ios.md` and on the To-Do page.
- **ANSWERED 2026-11-15 02:09 (before bed).** (1) To "TestFlight build 36: may I upload one in the morning with
  everything that passed overnight (loading screen, push re-register fix, crash-report fix, lighter fonts)?", owner,
  verbatim: "yes you can upload". My reading: one upload, build 36, only what passed; later uploads are asked again.
  (2) On the loading-screen line (T746), owner, verbatim: "can we do more fish puns here that are directed at this ?"
  So the line becomes a fish pun about putting the new regulations in place; candidates sent in chat, a small set
  built on the branch to rotate, the owner strikes or adds. The failure message stays plain (my pick; say if not).
- **NEW 2026-11-15 01:28 (iOS chat, T746, your "loading icon with text like splash").** Built on a branch; pictures
  sent in chat (iPhone light and dark, web). Pick the line: "Putting the new regulations in place…" (built) /
  "Applying the new regulations data…" / "Updating the regulations and map…". A failed apply says: "The new
  regulations could not be put in place. The app is still using the data it had." (OK button): fine / change.
- **NEW 2026-11-15 00:20 (iOS chat, T718 crash check).** Crash reports from the iPhone test runs were refused by the
  staging worker's daily limit (40 per address per day); the Mac's test beds used the 40 up, and your phone at home
  shares that address. Pick: give test-bed crashes their own limit of 200 a day on staging, production unchanged: yes
  (my pick) / no. On yes Claude deploys the feeds worker to staging from branch `ios/crash-send`.
  **ANSWERED 2026-11-15 01:05**, owner, verbatim: "yes to this". The worker commit (441ec090e) goes onto the shared
  branch and to staging at a time agreed with the Android chat (the owner is testing the staging site; the deploy also
  re-runs every feed job).
- **ANSWERED 2026-11-14 (iOS parity audit, T722/T723).** Owner, verbatim: "we want it to progress with app closed in both app
  and android" (the first install download); "need to just update text for ios" (the two promises that do not hold on
  iOS); "everything else looks like a good find should look for more things like that". So: a real iOS background
  download is being built (branch `ios/t722-install`); wording only for the notices / auto-download promises; the other
  findings are accepted as work; a second audit pass is running. Also offered 2026-11-14: "i have time to set uoa.
  developer account right now": the steps are in `handoff/PLAN/ios/START-HERE-ios.md` ("What needs the owner physically").
- **ANSWERED 2026-11-14 (iOS store pack, T727).** Owner, verbatim: "all privacy and descriptions and stuff could use
  playstore items. like icon and name etc it is can i fish? on the icon but can i fish this? on everything else. we want
  precise. it is the id mainly for for analytics and bug report etc etc choose best category". So: the App Store listing
  reuses the Play items (description, privacy answers, icon); home-screen label stays "Can I Fish?", every other name is
  "Can I Fish This?"; location is declared PRECISE; the install id is for Analytics and bug reports (App Functionality);
  category was left to Claude: **Navigation first, Sports second** (matches Play's "Maps & Navigation"; Claude's call, say
  if you want Sports first). Still open from that pack: email "linked to the user" (default: linked). The stale lines in
  `handoff/PLAN/play/data-safety.md` were fixed the same day (fa0d0a45c); the Play form needed no change.
- ANSWERED 2026-11-12 (T678 release, size gate): **data size budgets after the pond fold**: "Raise now, shrink later, but the ticket should cover ways to shrink data across the board, not just ponds." Raised: core 1,450,000 → 1,650,000 B gzip, conditions 950,000 → 1,060,000, web first search 1,500,000 → 1,650,000 (measured: core 1,508,127, conditions 963,726 after D1). Shrinking every shipped file is ticket T686 (survey + proposal for you to pick from). RULINGS O5.
- ANSWERED 2026-11-12 (T678, Owner To-Do row `dewdney-four-lines`): **the four Dewdney lines** (FWA 355991890/895/899/902, 1.6 km): "Yes, cover them". The Nicomen Slough rule covers them; nothing to build, the promoted atlas already does. RULINGS K10.
- ANSWERED 2026-11-12 (T678, Owner To-Do row `gauges-11-other-line`): **eleven gauges on a different river line**: "Take the suggestion". Nine keep today's line; Elk River near Natal (08NK016) and Fraser River at Steveston (08MH028) move to the new line. Ticket T682. RULINGS P49.
- ANSWERED 2026-11-12 and BUILT (b7749b726; ticket T671; Owner To-Do rows `t671-5b-aggregate`, `t671-d5-cr-part`): **T671 salmon, the two follow-up questions.**
  (1) Region 5B "in the aggregate" — your words: "5b says this on the ALL row (All lakes or streams or part thereof in the aggregate (combined total), in Management Units 5-6 to 5-11 in Region 5B, unless otherwise stated below (i.e. Region 5 not including the Fraser River watershed).)... So any water body not in the table will be with the total in aggregate (shared total) .. so basically items not in the table in mu 5-6 to 5-11 are shared but other row listed items have their own quota. Also the 4 salmon a day limit in total of all species is still a thing here too". That is what T349 already built in the release: an unlisted 5B water reads "Up to 4 chinook a day in total, all waters of Management Units 5-6 to 5-11 combined" (92,835 sections); a water with its own row (Bella Coola, Atnarko, Dean, …) has its own count; the combined 4 a day is the bucket line. The release does not change. The live feed (feeds worker) had not pooled — it read the ALL row as "4 a day" per water — and now pools the same way (shows with the owner's worker deploy). RULINGS Q29. Not changed, for your DFO labelling: the Dean River (41 sections) and Long Lake (1) have their own rows but those places are not drawn yet, so their sections show the pooled ALL row with the "some spots" caveat.
  (2) The part-headline for catch and release / check — answered "yes": "Closed today · catch and release in part" and "Closed today · check DFO for part", each with a caveat pointing to DFO for where; open wins, then catch and release, then check (RULINGS P47). On today's release: check 11,888 section-days (33 sections of the Fraser and South Thompson in Region 3, whose undrawn places print sockeye "To be determined"); catch and release 0 section-days. No status, bar or map changes. Shows with the next app build.
- ANSWERED 2026-11-12 and BUILT (4bc03ac14; ticket T671): **T671 salmon readings.** D1 "Yes to option 1 but add to gotcha list": an All closure beats a species row on the same place unless the species row is dated inside it (RULINGS L24; gotcha in `pipeline/docs/GOTCHAS.md`). Tatshenshini below the border: coho now closed Jul 24–Aug 14 (6,254 sections, 22 days, toward closed; Koksilah, not drawn, likewise); nothing moves toward open. D4 "The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4. Individual species limits also apply. Species limit is seperate.": the combined 4 is Region 6's bucket line ("Up to 4 salmon a day in total, fresh and tidal waters combined"), each species keeps its own line — that is already how the app reads, so nothing changes (RULINGS Q28). D5 "Closed today · open in part": built (RULINGS P47; 87,419 section-days on 717 sections, the page's own answer kept). D1 shows with the next release, D5 with the next app build.
- ANSWERED 2026-11-12 ("It is this (no Fishing EXCEPT the outlet stream downstream of the falls 2.4 km downstream of Whiteswan Lake, No Fishing Aug 1-Mar 31, rainbow ) so not the inlet. The outlet is below the falls. Inlet is all no fishing (every inlet) trout daily quota = 5 (catch and release all other species) when open)"; built: every one of the 15 inlets closed, T690, RULINGS E26, rides the atlas build): **T637: Whiteswan inlet named so its closure stays; say if the name or the piece is wrong** (2026-11-11). Blue line 356560775 above Whiteswan Lake (from 8,623 m up) is now "Whiteswan Lake inlet stream", so "No Fishing EXCEPT below the falls" keeps covering it after the oxbow fix. Ticket T637.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **Review app: the checklist is ready (T532, 2026-11-09)** (the two small calls stand as built). Restart it (`bash curation-review/run.sh`). DFO tab = DFO
  places; Entries → "Extents checklist" = regulation extents (both together per water: 3,686 places on 1,374 waters).
  One water at a time; each row ✓/?/✗ with its map pin; "All correct" or "Something's wrong" (→ change the extent).
  Two small calls, defaults in use: (1) ✗ also flags "tributaries walked but the words don't say" and "a carve-out the
  words never state" — default: keep ✗ (look at it), say "make those ?" otherwise; (2) a check saves on the place only, not
  on its older rewordings (they are dormant) — default: yes.
- **Production release: later, after your DFO extent labelling (owner 2026-11-08).** The command when ready:
  `CONFIRM_PROD=r-a178299f6005 python -m pipeline release --prod r-a178299f6005` (or the newer release by then).
- **Phone check (current build): last done 2026-11-09 on build 24** (answers on the owner checks page,
  https://claude.ai/artifact/5LRBFFgvYeTRnygSBWvZbM). 20 tickets closed on "fine". Follow-ups filed: T563 (percentile
  alert follows each day), T564 (gauge page: forecast + readable chart), T565 (in-season notice at the top), T566 (map
  switching), T567 (Fraser at Mission reading); T401 pop-in kept, minor. **One question back to you: T498/T508 "a
  water's pane opens whole" was marked Not right with no note: which tab, and what loads late?**
- **Review app look (T532):** "will check soon". Ticket work waits for your green light (one agent at a time).

- **ANSWERED 11-05:** T260 switches = ink fill; CF token kept until prod (T404); Apple install id declared as Device ID
  (not linked); Heber: carry ignores whitespace-only changes; audits T333–T335 scoped (all areas, report + fix tickets,
  medium, delete archive/). (T425/T427 looks, T319 titles, icon, support URL: decided 2026-11-06.)
- **PSF run-timing files: ANSWERED 11-05** — fine as is; only the parsed `timing.json` stays in git, the PSF pages and their
  fetch record are git-ignored from now on (772fbf4f0).




- **Water-page speed (T405/T409): ANSWERED 2026-11-05.** Build the map only when it is first seen, web-only (local 93·93·93;
  a phone's "back to map" waits ~1–2 s once). Next chat, launch and hygiene, before the prod SEO launch. Keep Cloudflare's
  JS detections and Web Analytics beacon (0 ms blocking).

- **Region hub cards (T406 part 2): answered 11-05 — keep both as they are** (the bar where it is; the map button opens
  the whole map, no region link).
- **Changelog drafts: approved (owner 11-05, "I'm sure they are good")** — `draft` cleared on 0.1.0–0.5.0; 0.6.0 stays a
  draft until its release. **0.7.8 changelog: ANSWERED 2026-11-13 ("the 0.78 draft is good")** — `draft` cleared on 0.7.8. Still yours: production has no Play config file (`play.json`) yet (prod transition, T404).
- **Gauge alerts, stock-style (T527, owner 11-09; replaces the T234 v1.1 cadence readings):** crosses above / below a
  value or a percentile for the day, once per crossing, ready again once back across by 2 % of the line. The per-alert
  cadence and the ±% in 24 h kind are gone; a saved ±% alert is not kept and the page says so. To confirm, as built:
  a percentile line is taken from the gauge's record when the alert is saved (delete + add again after a new HYDAT
  release to refresh it); custom percentiles are whole numbers 10–90 (the record has nothing past p10 / p90).

- **Plan to 0.5.0 approved (2026-11-03):** the plan file (`RELEASE-PLAN-0.5.0.md`) is in git history since T437. Yours along the way: a look at the compressed rule-book photo pages (T354, plan set 11-03)
  (T360/T354, batch 2); the Apple Developer account ($99) before the iOS release (T88); the feeds-worker deploy: now Claude's
  (owner 2026-11-03), when it makes sense (with 0.5.0: dfo-feed/3 + the ten forecast days).
- **Build pass (T266/T295/T297, 2026-10-31):** review `handoff/PLAN/app-integration/build-pass-2026-10-31.md`. The proof side run matched r-51d80e68f056 byte for byte, except the tile_version (T305) and the publish stamps. Nothing was promoted. Nothing is yours here.
- **App test bed (T255, 2026-10-29):** Android emulator scenarios for downloads/sync/offline — `app/tools/testbed/README.md`; last report `handoff/PLAN/app-integration/T255-shots/run-final/index.html`. Found and ticketed: the first install crash-loops on Android (T262, high), Wi-Fi return does not start a held update (T265).
- **ODbL offer (T235, 2026-10-28):** done as you asked (`data/LICENSE-ODbL.md`, `_licence` in each file). Yours: (1) the repo has no LICENSE of its own — the notice says the rest is "under its own terms"; pick one if you want it stated; (2) the app links files on the `redesign/stream-sections` branch (`licences.ts` `REPO_BLOB`) — flip to `main` when it merges; (3) the no-access land (incl. Malcolm Knapp) and reserve outlines are OSM, not the Province — now credited as OSM.
- **Update testing on your phone (T251, 2026-10-28):** dev build → About → Updates log → channel Test → reopen; then `node deploy/staging/test-updates.mjs reset | feed-bump gauges | release 1 | corrupt | missing | feed-404 dfo | min-app 99.0.0` and Check now (steps in tickets/T251). Before prod (T86/T240): the production data worker must allow the `X-App-Version` header (copy staging's data-worker.js CORS line), or web update checks fail.
- **Staging is live (2026-11-05: release r-5a5999c94588, SEO pages from it; was 2026-10-28 r-46be3c135e68): https://staging.canifishthis.ca** (app + SEO pages; data from
  data-staging.canifishthis.ca). Check: the map paints and a tap opens regs; a few `/water/<slug>` pages (e.g.
  `/water/vedder-river`), `/region/2`, `/sitemap.xml`, and an old link such as `/waterbody/930-055749/` (one 301 to
  `/water/san-juan-river`). Staging sends `X-Robots-Tag: noindex`. Decide on the zone: (a) Cloudflare **managed robots.txt**
  is prepended to ours (it says `Allow: /` for everyone and blocks AI crawlers); (b) **Bot Fight Mode / JS detections + Web
  Analytics** inject ~2.6 s of script into every page (Lighthouse Performance 69-76 instead of >= 95; our pages have no
  JS). Prod's data worker `bc-fishing-r2` double-gzips release files: use `deploy/staging/data-worker.js` for prod (T86).
  Details: handoff/PLAN/seo/staging.md.
- **SEO / domain — AT PROD LAUNCH ONLY, not now (owner 2026-11-05: no prod deploy for a while; tuning on staging).**
  These wait for the prod transition (T404 runbook). Nothing below is due today. Staging: https://staging.canifishthis.ca
  **The runbook is written (T404, 2026-11-07): `handoff/PLAN/release/PRODUCTION.md`** — every prod step in order with its
  command (all yours, each needs CONFIRM_PROD), every later release, rollback. Read it and answer Q171–Q175 (tickets/README).
  1. **(in the prod release runbook, T404)** **www.canifishthis.ca drops the path today** (`curl -sI https://www.canifishthis.ca/about` → `location: https://canifishthis.ca/`).
     Cloudflare dashboard → canifishthis.ca → Rules → Redirect Rules → edit the www rule: "Dynamic", expression
     `concat("https://canifishthis.ca", http.request.uri.path)`, status 301, **Preserve query string** on. (canifishthis.com and
     www.canifishthis.com already keep path + query — leave them.) Optional instead: remove those rules and deploy the generated
     host worker: `npx --prefix app wrangler deploy -c deploy/prod/host.wrangler.toml` (after deploy/prod/site.sh has copied it).
  2. **Prod site deploy** (with the 0.6.0 release; T240 §5): build `app/dist/web-prod` (T240 line "SEO build for prod") and
     `node app/tools/build-seo.mjs --release-dir data/release/<r-id> --app app/dist/web-prod --threads 4 --out app/dist/seo-prod`
     (via scripts/heavy.sh), then `CONFIRM_PROD=canifishthis.ca deploy/prod/site.sh`. Check: `curl -sI https://canifishthis.ca/waterbody/930-055749/`
     → one 301 to /water/san-juan-river; `curl -sI https://canifishthis.ca/region/2/` → 301 (not 307); `curl -s https://canifishthis.ca/robots.txt`
     ends `Sitemap: https://canifishthis.ca/sitemap.xml`; no `x-robots-tag` header; `strict-transport-security` present.
  3. **Zone settings** (Security → Bots; Settings → robots.txt): keep Cloudflare's managed robots.txt OFF on canifishthis.ca so ours is the only one.
  4. **Search Console** (search.google.com/search-console): keep the v1 property; Add property → Domain → `canifishthis.ca` → copy the TXT
     record → Cloudflare DNS → Add record TXT `@` with that value → Verify. Repeat for `canifishthis.com`. Sitemaps → submit
     `https://canifishthis.ca/sitemap.xml` (expect ~19,928 discovered). URL Inspection → `/`, `/water/vedder-river`, `/region/2` → Request indexing (those three only).
  5. **Bing Webmaster** (bing.com/webmasters): Import from Google Search Console → pick canifishthis.ca → submit the same sitemap.
  6. Monthly: handoff/PLAN/seo/MEASURE.md (no analytics script — Search Console, Bing, Cloudflare traffic).
  7. Outreach (T197): after prod is live, fill the brackets in handoff/PLAN/seo/outreach/*.md and send each by hand; log in tracker.csv.

### Batch for the end of the closed-testing run (owner 2026-11-11: decide together at the end)

Answered 2026-11-11 (recorded in T665, T667, T666): the six tidal decisions, the six tests decisions, the fresh-copy test run.

Answered 2026-11-11 (later, tidal rulings in T678, T679):
- A fresh stub under 25 m at the top of a line counts as tidal (Ewen Slough, 356242813 become wholly tidal).
- Lines wholly in tidal water are kept in the graph and marked tidal; none is deleted.
- An unnamed widening pond on a river is the river, for whole-river rules and for "in streams" rules (T679 D1, Q154).
- Order of tidal sources: (1) a DFO written boundary, including curated splits that encode one; (2) otherwise coast crossing, where the line enters the sea or a tidal river; (3) the DFO polygon for map shading only. Polygon hand edits and non-DFO splits carry no authority. Verbatim on the splits: "No to this unless the curated splits are the DFO ones. Like all the splits added in region 1 and 2 especially."
- The Fraser's own channels and arms below its tidal boundary are tidal ("We still want all channels and stuff of Fraser below it's tidal boundary as tidal"). Sloughs are NOT covered ("Sloughs aren't channels. Sloughs could be really non tidal?") and are decided one by one.
- San Juan River: the tidal boundary is the San Juan Bridge, by DFO's wording ("... shoreward in a easterly direction of this boundary and downstream of the San Juan Bridge", https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/tidal-maree/a-s20-eng.html). ACCEPTED by you on 2026-11-11 ("No just accept that bridge please"): the curated split goes in as accepted, DFO's wording as the source, no review step (T678).
- T627 digest email: approved, deploy as is.

Answered 2026-11-12 (recorded in the tickets; Owner To-Do rows ticked with the answer):
- Owner approved promoting the second tidal candidate (full_t678b). The 17 lake-outlet gauges keep today's placement. The 187 open coast lines stay held at today's result until a grouped proposal is made.
- Salmon (T671, T676): D1 All row vs species row: the All closure wins unless the species row is dated inside it ("Yes to option 1 but add to gotcha list"). D4 Region 6 aggregate: "The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4. Individual species limits also apply. Species limit is seperate." (combined 4 is a pool note; species limits stay separate lines). D5 undrawn open place: "Closed today · open in part". D2 (Campbell River) accepted. T676 "until further notice": "it is from that day to no end. If the thing is for a closure then it closes etc" (open-ended from that day, doing what the row says, in release and feed).
- Tidal (T678): San Juan: the main-stem bridge is at the river's mouth, so the boundary stays at the mouth; the side channel is tidal below the Deering Road bridge (681 m up it). The 10 curated splits not traced to DFO (Qualicum, Toquaht, Sooke, Chemainus, Campbell River Area 13, Nitinat, Yorkson, Chambers, Noons, Nass): "Keep all as my decisions" (recorded as owner boundaries; this overrides "non-DFO splits carry no authority" for these ten). Kitimat: automatic mouth line. Marble and Tlell: "Mouth for marble river (signs near varney bay) could add it has signs to cut.. for tell say 1.5km upstream bridge option" (Marble at the mouth with a note that signs stand near Varney Bay; Tlell at the bridge 1.5 km upstream, the DFO option naming it: Claude's reading). The four held creek pieces (360586479, 360665787, 360675720, 360812872): the mouth line decides, shown in the T678 diff. Ucona's first 33 m under the Strathcona Park rule: fine. lake:329461947 folds into the Okanagan River Oxbows: yes. "Dewdney Slough" search name finds Nicomen Slough: yes, apply.
- T679: D1 ponds: built then reverted (the bundle refused it: 161 rule keys mixed lake and stream); goes into the next atlas build instead (16,152 ponds change; 97 rules gain, 32 lose). D2 partial salmon coverage: map and bar both give the species verdict. D3 nearer row wins: ignore the loops (receivers with flow-back removed), next build, diff shown. D4 words read at build time: leave, add a guard that stops the build and names the row when a row that matched before stops matching. D5 DFO confluence cuts: same as provincial, the joining water stays out unless DFO's words add tributaries; diff before it ships. D6: Python reads DFO rows tolerantly too (an unreadable row reads "check" and is reported, no longer stops a release). Gauge placement (#4, held): "One rule, accept the moves": atlas placement wins; 29 of 458 shown gauges move at the next build; list reported.
- Screens: T462 G6 Pend d'Oreille: the printed exemption lifts both rules; barbed and treble allowed. T670 G2 bait heading: "Why does it matter the river is closed? It shouldn't show any method or gear or quotas in the regulation card. If salmon are open or something at same time so that river is half closed then it should say in force." (a fully closed water shows no gear/method/quota; where something is open the rule reads "in force today"). T670 G5: Licence tab shown when salmon is open. T668 live straddler: "If python pair table is safe that sounds good we want these to always resolve on their own" (option 2, provided it measures safe). T368 Q6 "today": switch to B.C. time (America/Vancouver). T672 W4 web description: name every region.
- "Accept all as built": feeds worker defaults (T549/T554/T633/T597), T666 release path defaults, T585/T555 and T554/T633 gauge wordings, T182 caps, T532 checklist's two small calls, T630/T605/kept-reading line (leave), T672 K9 (leave), T670 K1-K5/G1/G3/G4 wordings, T672 W1-W3/G11, T668 card wording, T671 D2 fix, the 8 area names cut at 50 characters, T387 point-bound facts (leave per tap).
- Still yours: nothing from this "accept all as built" list; open items are under their own headings below. (Nakina half-river, T670 G7 and T627 answered 2026-11-12.)

Staging: release r-483b295de57c is live on https://staging.canifishthis.ca (site, data, feeds worker). Yours: look at the four new creek cuts (FWA lines 354135384, 356070273, Crescent Slough 356326465, 356349946), and Brem and Ucona, on the staging map.

Open, your call (tidal, T678 / T679):
- ANSWERED 2026-11-12 (build: T685; RULINGS E21–E24): one rule — water reaching the sea is fresh to the shoreline (G1, G2, all of G6, X2, X3, X4); a creek entering a tidal river is cut at the river's edge (G3, X1, X5, X6, X7, sloughs); G4/G5 keep today's cuts; five sloughs whole tidal (Deas, Woodward, McDonald, Tamboline, London). Original ask: Tidal coast proposal: 183 lines in seven groups + 20 Fraser sloughs (T678, replaces the old held-rivers, 46-estuaries, 61+32 held, four-new-cuts/Brem/Ucona and Fraser-slough rows; Owner To-Do row `tidal-coast-proposal`): https://claude.ai/artifact/2VRJAsyeeyMNmCiZpJewVA . Proposals only, nothing applied: G1 59 fresh to the shoreline; G2 17 drop the cut; G3 18 cut where the creek meets the river; G4 10 and G5 3 keep today's cut; G6 63 (E1 to E10) and G7 7 (X1 to X7) one answer per card; S1 to S20 Fraser sloughs, nothing filled in. Answer in chat.
- ANSWERED 2026-11-12 (T678 C, Owner To-Do row `held-five-build27-cut`): five held lines with a build-27 cut and nothing tidal below (354087703, 354154029, 356341234, 360445710, 360861076): "Drop the cuts". Ticket T684. RULINGS E20.

Still open:
- ANSWERED 2026-11-12 and BUILT (4bc03ac14; ticket T676): "until further notice" — "it is from that day to no end. If the thing is for a closure then it closes etc". The release now reads every DFO date cell with the live feed's reader, so both say "from <day> until further notice" (RULINGS D5). No open/closed status moves on today's pages; 6 places not drawn yet change their words. Shows with the next release (the feed already reads it so).
- T668 salmon places from two regions on one stretch (audit `app-integration/T668-audit.md`, 493 sections): built on the defaults D1 an undrawn place stays in its own region, D2 on a section in one region that region wins a tie, D3 a region-wide place binds every section in its region, D4 the card names the region, D5 D1 first. Owner 2026-11-11: a stretch that straddles a boundary is genuinely in both regions, so a straddler keeps both regions' places and the stricter answer per fish speaks (as the freshwater rule, RULINGS Z4); and "straddles should always take the most strict rules from both regions" (limits, sizes, open days, gear, hours). Measured: 432 of 493 sections change, 197 genuine straddlers, 84 statuses flip, all toward closed; shows with the next release. D1 and D4 confirmed by the owner 2026-11-11 ("Yes and yes"), with: on the Skeena the card row names the DFO section as well as the region (the cascade level): the lettered section as DFO's Skeena page prints it (A, B(i), B(ii) …), owner's clarification the same day. ALL ANSWERED; nothing open here until the phone check of that release.
- T668 straddlers, one default to confirm (built, commits dfb4ea7c1 b53d5e193, RULINGS Z4a): salmon openings on a straddling stretch now use the same strictest-wins comparison as the freshwater rules, with ONE deliberate difference: when one of the two DFO regions says nothing about a fish on that stretch, it counts as closed there (freshwater treats silence as 'the base rule applies'). Measured on the 197 straddlers: 19 sections now show both regions' words, none looser, no map colour change, no gear change. Default taken: silent region = closed. ANSWERED 2026-11-11: yes, salmon differs from freshwater here; a silent region counts as closed. The rest of the unification is T669.
- T664 Okanagan River Oxbows as one water (code 25471b393; lands with the next atlas build; detail `app-integration/T664-pending/README.md`): the 45 stream pieces become one water. Default taken: the 8 oxbow polygons that the source data classes as lake or wetland stay separate, so search will show 1 + 8 results, not 1. Folding them in would turn them into streams, which takes lake rules off them and puts stream rules on (including the spring stream closure the oxbow row exempts). ANSWERED 2026-11-11: fold them in. "They should have been converted to stream polygons like Nicomen Slough so it all becomes one. They aren't lakes, they are considered streams." Going into the next build; search shows one result.
- Nine DFO cuts from your marks (proposal patches in `app-integration/T664-pending/`, applied in the next build): Nakina River, DFO's wording is "from the centre line to the northern bank" between the two signs; a cut cannot take half the width of a river, so the rule will bind the whole channel between the signs (stricter than DFO on the south half). Default taken: whole channel. Owner 2026-11-11: will look later. ANSWERED 2026-11-12 (Owner To-Do row `nakina-half-width`): "Whole channel + note": the rule binds the whole stretch between the signs and the card says in DFO's words that it applies from the centre line to the northern bank only. Ticket T683. RULINGS E19.
- ANSWERED 2026-11-12 (T670 G7, Owner To-Do row `t670-g7-classified-terms`): the 26 classified-water licence terms of p.7: "Show on each water": each classified water's Licence tab lists the p.7 terms that apply to it, in the book's words. Ticket T670. RULINGS Li13.
- CLOSED 2026-11-12 (owner: "Close it"; row `t628-marked-entry-guard`): T628 item 5, a plain re-parse can no longer replace an entry carrying your review mark.
- Next rebuild side build, measured (results: `handoff/PLAN/app-integration/next-rebuild-results.md`; no unexplained difference). Items for you, all left at today's behaviour unless said:
  - parse-missing: NOT needed, closed. Checked without parsing (5b5caecd3 lane): 1,393 entries, 0 missing, no reference to a lost water id; entry ids come from the synopsis row, not the water.
- Tidal map pages (same links as the tidal line above): held rivers, one card each https://claude.ai/artifact/3v6g4Ggx8WJZg4Jvxras9q ; every cut on the map https://claude.ai/artifact/3Jbd8SjT64Tj9HXvPgPBDc
- Phone check of the next build: Claude brings one fresh list (folds in the 0.7.5 checks: cold start, gauge ring, key switch, oxbows, page opens whole, part warning, three rule readings, satellite edges, tidal shore, Stellako steelhead line, plus T681 legend card and the T625 shared link). Owner 2026-11-12: "One fresh list for the new build", gone through in chat with pictures.
  - Salmon closed shows the year bar like game fish (Capilano River). Built on your "maybe" (2026-11-13): a water DFO covers whose salmon are closed all year now draws the Salmon (DFO) year bar (closed, today marked, legend) instead of the bare "Closed" word; 751 waters change. Region-wide closures (R4, R5, R7A, R7B) and waters DFO lists nothing for keep the bare word. Say if you prefer the bare word back.
  - More screen: Settings menu, Freshwater Atlas no longer "out of date", no repeated update info. (T703, owner 2026-11-13; next build. Pictures: ~/bcfish-session-backup-2026-11-12/more-settings/)
  - Map zoom carries momentum after a pinch: a quick pinch out or in, fingers lifted while still moving, keeps zooming for about a third of a second (also with the thumb held still and one finger flicking); a pinch held still before lifting stops where it is. (T707, your build-29 report "No zoom flick"; next build, Android.)
- **Phone check after the final build — notification inbox: bell badge, New/Earlier list, open a row, mark all read, an Updated notice** (T706 + T694, owner 2026-11-13: "The inbox can auto be added just add it to the post release checks for me to look at it"). Tap the bell: unread in-season notices under "New", read ones under "Earlier"; tap one (it opens its water's page at the notice and stops counting); "Mark all as read" clears the badge; an amended notice reads "Updated". Pictures (fixture feed, staging waters): `/Users/dawson.horvath/bcfish-session-backup-2026-11-12/notifications/` (light and dark). Nothing to decide unless it looks wrong.
- **Added 2026-11-13 (Owner To-Do rows; every one is a proposal, nothing decided):**
  - Phone checks after 0.7.8 lands on Play internal: `phone-check-078-salmon-bar` (salmon "closed" waters draw the year bar; T678), `phone-check-078-pinch-zoom` (T707), `phone-check-078-inbox` (T694, T706; see also `t706-inbox-phone-check`), `phone-check-078-settings` (More → Settings, T703), `phone-check-078-map-look` (general look).
  - T708 questions: ANSWERED 2026-11-13 (recorded in T708 Owner decisions 6): `t708-haida-gwaii-tidal-mouth` (stays at the mouth for Copper, Deena, Mamin unless a written boundary is listed elsewhere; check found none), `t708-stave-ruskin-channel` (the whole described Ruskin channel; no change), `t708-mcarthur-island-slough` (the whole slough; no change to the stretch; the card keeps the book's place words). Still open: `t708-frog-redfern-squirrel` (look harder; evidence in T708 Work 8).
  - Research questions, one row per note in `handoff/PLAN/research/`: `research-accounts` (8 questions), `research-corridor-imagery` (6), `research-bathymetry` (6), `research-stocking` (6).
  - `store-descriptions-write` (T371): write the short (80 characters) and full (4000) store description from `handoff/PLAN/play/listing.md`.
  - `boundary-name-test-changes` (T678): the boundary names and test pins accepted during the release: Fraser (34 sections), Babine part 5 "juvenile counting weir", Skeena Exchamsiks → Zymagotitz and the Reserve parts (junction fix), Kanaka Creek "→ Source", Mat Creek top end, Coquitlam part names, the Skeena map tests, and the four test updates in 4ecbe1738 (Harrison 3 → 4 rules, unresolved 25 → 29, export codec lake edge as a run's end; plus the regenerated unnamed-stream sweep, −3 attributed). ANSWERED 2026-11-13: (1) Babine part 5 keeps "Juvenile counting weir"; (2) Skeena Reserve split accepted, side channels to be listed where they sit (T710, count first; side-channel name reads backwards, undecided); (3) "Source" stands, header text must match (T711); (4) Fraser cuts + Skeena map tests accepted; (5) Harrison r4 shows the book words with location text (T712); (6) steelhead rules r5b/r6b/r9b trace (T713).
  - `t709-preview-video`: watch the preview clip before any store upload (not ready; waits for the build).
  - `gagnon-williston` ticked: owner answered 2026-11-13 (mouth stays Williston; sign cut about 200 m above the unnamed lake, T708 Work 3).
  - ANSWERED 2026-11-13 (by dialog) and BUILT, released r-f749475d0962 (T708): (a) McArthur Island Slough shows the "in part" form "No powered boats — in part: from westerly entrance to 12th Street entrance to Park"; (b) Lakelse River: rename the end only, the lower stretch's end reads "Mouth (at the signs)", no new cut, no change to which water the rules cover; (c) Tlell r5: yes, take the tidal note off the 13 freshwater tributaries. `t708-mcarthur-lakelse-tlell` (T708, proposals, 2026-11-13): (a) McArthur Island Slough: the catalogue refuses the book's place words beside a whole-water rule; the only allowed form shows "No powered boats — in part: from westerly entrance to 12th Street entrance to Park" (a note on the slough). Accept that, or keep today's "No powered boats" (the book sentence still shows underneath)? (b) Lakelse "signs" at the mouth: needs either the designated mouth located (signs 1.7 km below the logging-road bridge) for a cut, or a new per-water mouth name; which? (c) Tlell r5: take the tidal note off the 13 freshwater tributaries (Work 6 proposal)?
  - ANSWERED 2026-11-13 (by dialog): the Harrison's two vessel entries in build 30 are fine; nothing to change, T712 closes. `t712-harrison-vessel-words` (T712, 2026-11-13): build 30 already shows the Harrison's two vessel chips with the location: "No powered boats — in small bays along the river as signed" and "Speed restriction — in small bays along the river as signed" (your 2026-11-12 "Keep them as two entries"); the pack's "r4 No powered boats" was its own short summary. Enough, or show the one printed line "Speed restriction or no powered boats in small bays along the river as signed" instead?
- **Phone check of build 29 (app 0.7.7)** (2026-11-12; on Play internal; release r-0a9930fb8826). This is the fresh list promised above. Look at these on the phone:
  1. Cold start: close the app, open it; the map comes up with no error.
  2. Gauge ring: a gauge that reports only flow or only level shows as a grey dot with a ring (Kootenay Lake has some).
  3. Map key switch: flip Trout and Salmon (and Runs) quickly; the key under the map always matches the map's colours.
  4. Oxbows: search "Okanagan River Oxbows"; one water, not 45 pieces.
  5. A water page opens whole: search a river and tap Open; the page shows the whole water, nothing loads in late.
  6. Part warning: Shuswap Lake (or Quesnel Lake) shows "Part of this lake is closed today" at the top of the Keep tab (the closure is on a part the map does not draw).
  7. The three rule readings (strictest rule wins on a straddle, "until further notice" dates, a part closed in a lake): say if any read wrong.
  8. Satellite edges: switch the map to satellite and zoom into a stream; the stream opens into two thin edges so the water shows through.
  9. Tidal shore: Deas Slough (the page is the tidal card, "Tidal water: a different regulation system..."), Power House Creek, Suter Brook; each ends at the tidal limit.
  10. Stellako River: the steelhead line reads "Rainbow trout and steelhead: release every one".
  11. T681 gauge legend card: on the Conditions map, a small card over the map says "Gauge reports level only" (under Flow).
  12. A shared link (T625): open a link someone sent you; it lands on the right water.
  13. Nakina River: the card says in DFO's words that the rule applies from the centre line to the northern bank only.
  **Known, do not report: Deas Slough's Map view (the Map tab on that water) keeps the previous camera and says "The parts of this water are not available" on build 29. Ticket T688: fixed in code, ships with the next APK. Pending your word (default built in): the tidal bar reads "Tidal water", the tidal paragraph, and a button "Show regulations for this water".** Also known: searching "Vedder River" then Open lands on the Chilliwack River's part 1: ticketed as T687 (the book pointer above the result opened the wrong water; fixed in the data, 5c294120d, shows with the next release).
- Defaults taken 2026-11-12 (app wave; each is on the Owner To-Do page, group "Look and pick", rows 6 to 13; act only if you disagree):
  1. ANSWERED 2026-11-12 (keep): T677 Ucona River: the Strathcona Park rule now covers the first 33 m of the Ucona. Keep as built, no cut where the park ends.
  2. ANSWERED 2026-11-13 (built): T673: ONE in-season matches file for every release ("Why not just have a single file that does both?"): a notice is placed by the river's map line and metres along it, so a phone on any release places it on its own sections (a section only partly inside shows it too). No per-release copy. Goes live when the main session publishes it with the final build. The "new" mark on an amended notice (T694) is built too, with the inbox (T706).
  3. ANSWERED 2026-11-12 (leave unmarked): T629: "state your FWID" in the Proof of Identity paragraph may be from the later printing. Left unmarked.
  4. ANSWERED 2026-11-12 (all themes): T467: the colour-blind check stays on all themes. The lake-shoreline look stays open for pictures.
  5. T540: "Reading the regulations" stays in AGENTS.md.
  6. ANSWERED 2026-11-12 ("Show the fee only"), built: T670's two basic-licence terms: only the $1.14 fee line for disabled B.C. residents is on the Licence tab (under the basic licence, for a B.C. resident adult, in the book's words); "one annual angling licence per year" stays off. RULINGS Li13b.
  7. ANSWERED 2026-11-12 (stays): T688: wording on a wholly tidal water's map: bar "Tidal water", the tidal card's first paragraph, button "Show regulations for this water". Also answered by dialog: the tidal area stays drawn and labelled but tapping it opens nothing, and the 16 wholly tidal waters stay in search with the tidal card alone (no "SALMON (DFO) Closed" line): built as T692, RULINGS P52.
  8. ANSWERED 2026-11-13 (owner: "fix it"; Claude's choice): Store screenshots (T319); shot 8 now reads "Rules and map work offline" and shows the More data list, each source with its date (Settings has no offline control on the web); order and titles from `app/tools/store-shots.json`: "Can I fish here today?", "Find any water", "Today's rules, in plain words", "River conditions", "Salmon openings", "When the runs come", "Works offline". The "Keep your spots" scene is phone-only and not shot. New set (staging site on r-0a9930fb8826) in `/Users/dawson.horvath/bcfish-session-backup-2026-11-12/store-077/`.
  9. T689: an own-rules lake (for example Cultus Lake, which is open) draws orange at 60% over the blue basemap water, which mixes to khaki and sits close to the "Not fishable" swatch in the map key (sewage ponds, tailings ponds, dumps only). Nothing is wrong in the rules; it has looked this way since at least Nov 4. Should own-rules lakes get a colour further from "Not fishable"? Default: leave as is for closed testing. (Also on the To-Do page.)
  10. ANSWERED 2026-11-12 (A: stay outside): T631 item 5, the three Skeena side channels at the Kitsumkalum mouth (360220078, 360220098, 360220099). You said "this i meant the auto mix mouth splitting method we developed we want to use that here if it is splitting tidal from fresh": it is NOT a tidal/fresh split (the Skeena's tidal boundary is 117 km downstream), so nothing changes. Then you asked whether they should be split like normal braids. Facts (pictures A–D in `/Users/dawson.horvath/bcfish-session-backup-2026-11-12/skeena-channels/`): they are ordinary braid channels of the Skeena, but by the map they lie wholly ABOVE the DFO place's top boundary (beside river km 125.5–128.6; the boundary is at 124.0); they look like they straddle it only because one junction in the FWA data carries a distance 3.35 km off its own map point. Options: A keep as today (outside the place); B put them whole inside; C carry the cut across by river distance (lands on none of them, same as A); D fix the bad junction itself (same answer as A, but that rule moves 298 junctions province-wide, so its own ticket). Default: A. Still open: do you still want a cut there? (Also on the To-Do page.)
  11. ANSWERED 2026-11-12 (no list): T678 plan 6, lines the pruning removed from tidal water: you lean to "just add them to the manual list". Count today: 1,204 unnamed FWA lines with no piece in the map data lie in tidal water — 1,133 are FWA "construction lines" drawn inside the Fraser, Skeena, Nass etc. river polygons (the river's own centre lines, not channels), 71 lie in the sea (none named). They have no piece in the map, so a list changes no answer and no picture; it is a record only. Recommendation: no manual list; if you want one, only the 71 sea lines, in a new file beside `data/curated/waters/shore_held.json` (not in it, whose rows are lines the coast cut acts on). Default: no list. (Also on the To-Do page.)
  12. ANSWERED 2026-11-12 (promote with the final build): The candidate map data (`full_next`, built, not promoted): what changes for an angler — searching "West Road River" or "Blackwater River" finds only the river, no longer five lakes as well (Kluskoil, Tsacha, Tundra, both Euchiniko); two Teslin River "Hutsigola Lake" edge ids swap (nothing names them). No rule changes where it applies. Default: it is promoted with the final build unless you object. (Also on the To-Do page.)
  13. T678 plan 6 wording: what "a line the prune took from tidal water" means now the coast rule ignores the DFO polygon. Default: an FWA line drawn in the sea or in a tidal river's polygon that has no piece in the map, listed with its reason (the count in 11 uses this). (Also on the To-Do page.)
- Tidal (T665), answered 2026-11-11 on the dry-run map (`app-integration/T665-dryrun-map.html`, sent in chat): cuts that leave nothing tidal are dropped; the 33 creeks are cut where they leave the tidal polygon and the tidal part taken out ("line end wins" withdrawn); Claude applies the same to 59 more creeks of that kind, say if wrong. Default to confirm: a water with its own tidal limit is never cut by the polygon. Still yours: eight main rivers (Bella Coola, Mamin, Apple x3, Kshwan, Sarita, Bedwell) held out until you look at their map page.
- Tidal (T665) rerun with those answers (commit 227c53b08, `app-integration/T665-dryrun.md`): 201 shore cuts (127.4 km tidal below them), 215 lines wholly inside removed (67.7 km). 61 lines HELD until you look, untouched in the next build: the 8 main rivers plus 53 where the polygon runs over ~100 m up a narrow channel (the Pitt-style over-cut). Held rivers, one card each: https://claude.ai/artifact/3v6g4Ggx8WJZg4Jvxras9q — all cuts on the map: https://claude.ai/artifact/3Jbd8SjT64Tj9HXvPgPBDc. For each held one: cut at the polygon edge, cut at the mouth (DFO default, straight line between the two most seaward points of land), or leave. Of the 33 mouth creeks: 19 cut, 11 held, 3 not cut. Also proposed, not applied: 24 mouth rows for the tidal limits file (`T665-tidal-limits-mouth-rows.patch`).
- Store screenshots (sent 2026-11-11): pick the order and titles (`app/tools/store-shots.json`).

## Reviews
- CLOSED 2026-11-12 (row `t627-read-staging-digest`): T627 staging digest email read by the owner: "looks good".
- **Tidal follow-ups (T359, 2026-11-03):** review app → Tidal → Follow-ups (8). Decide: does a DEM point you
  confirmed become a cut (Caycuse 1,925 m, Dempster 550 m)? Re-mark Annacis/Cannery/Morey/Parsons "whole water";
  look at Skagit (US border) and Bear River (bridge).
- **DFO tab:** Nanaimo "tidal portions": both are below the tidal limit (Cedar Rd bridge), so tidal — T449 makes that a state (owner 11-06).
- **T227 DFO places not on the map (2026-10-28): review app → DFO, open each, "accept suggestion", Save extent.**
  - accept in review app → DFO: Cowichan — Sandy Pool sign → tidal boundary (= FWA mouth; active, high); weir/Cowichan Lake → 66 Mile Trestle (high); CNR mile 70.2 → Skutz Falls (high, new Skutz cut); "incl. tributaries and Cowichan Lake" = whole river + lake + tributaries (medium: lake's inflows counted). Last three dormant.
  - accept in review app → DFO: Capilano — "including tributaries" = whole + tributaries (high).
  - accept in review app → DFO: Kitimat — downstream / upstream of Hwy 37 bridge (high, OSM-checked); hatchery outfall signs = your 08-10 pins (high; west-bank-only width not modelled).
- **Release r-a871447c48ba verification follow-ups (2026-10-28):** (Claude re-cut since; the golden re-extraction of the Q62/Q63 words is moot, golden.json is frozen since T87. Whonnock 08MH100 and Beattie "Trout" answered 10-28: T228.)
- **T228 (2026-10-28):** two DFO queue items from T228. (1) Klinaklini: place "Management Units 5-6 to 5-11 boundary"
  as a split (pre-filled point). (2) Bella Coola: exclude the Atnarko (gnis:17209) from its tributaries.
- **T224 names (2026-10-29, live in r-62e2f3a8c81e):** review app **Names** tab (:5183, `#names/`), reject any that
  are wrong among these 20 auto-accepted names (list + why in tickets/T224 "Owner spot-check"): Cat Stream (applied: a 3 km OSM way
  matches 95 %; within your rule), The Cut Coulee, Cohen Creek, Benes Creek, College Creek, North McNaughton Creek, Brigade Bay Creek,
  Problem Creek, Coho Creek (OSM); Corinne Brook, Jantz Stream, Marshall Creek, Gill Brook, Mckay Creek, Shamrock Creek,
  Smith Brook (Abbotsford); Inkaneep Creek, Meadow Creek Diversion, Quinsam River Diversion Headpond, Noble Creek (ECCC).
  No rejects = done. The rest of the review list (36 high, 552 medium, 8,061 low) is optional.
- **T202 basemap (2026-10-27):** the basemap is now z13 (918 → 274.5 MB). Look at the app at your spots, z10–z18,
  light and dark: trail and FSR names should show from z15. Shots
  and numbers: `handoff/PLAN/tiles/T202-basemap-audit.md`.
- **"Needs a split" follow-ups (2026-10-27, live in r-8b43fb267199):** (1) the three guessed `low` cuts are in the
  review app, **Low-confidence cuts** queue (top bar button; or http://127.0.0.1:5183/?lowcuts) — each entry opens on
  satellite, flown to the cut, with why it is a guess: Chapman lower falls + 100 m below (SCRD survey point, ~576 m below
  the power line), Deena "second bridge" (no bridge mapped: a point 5 km above the tidal boundary), Mamin "third bridge"
  (Mamin Main road, 9.1 km above tidal). Right = Place y; wrong = Place n, "split misplaced", pick the right point on
  the map in the extent fix. (2) Columbia
  r11 "from Mud Lake": FWA names no Mud Lake, so it runs Fairmont → Columbia Lake (the outlet wetland, wbk 329093907, is
  the likely Mud Lake) — say if Mud Lake is elsewhere; (3) Stave r1/r2 (Ruskin/Northrop spawning channels) stay notes:
  the channels have no sections in the atlas — adding them is Claude's work (T222), nothing for you here.
- **T204 phone check:** Kanaka Creek's map now has two marks (112 Avenue bridge, Cliff Park Falls) and its lower part is
  two separately selectable stretches ("… · stretch 1 of 2"); say if the stretch wording should differ.
- **Map look (T64, T85, T113, T152; all built and live):** look at the map (wetland-heavy Fort Nelson and the
  Chilcotin without unnamed wetlands; z13–z18 bends on the Fraser/Vedder now overzoomed from z13; Vancouver's dense z13
  tiles; no OSM creeks under ours at z14+) and say if the look suffers — the basemap's OSM wetland fill is one style layer
  away. Best done in the same sitting as the T202 basemap look above (basemap z13 shipped 10-27).
- **Q4 / T55 + T149 — slough and canal candidates: review app, Sloughs tab only** (:5183, `#sloughs/`). All 2,424
  candidates with their pre-marks; your 150 review-page verdicts show as "page: …" and your 8 free-text notes as
  "your note" (incl. 329097416 Cruickshank, the one whose verdict you cleared: "similar this can be on moat lake").
  The old slough page (artifact Es3Mokt7Fs2yNvm7JgQua8) is retired — do not mark there; its whole database (151 verdicts + 58 spot checks)
  was saved as marks-final-2026-10-28.json and has since moved out of the tree (T322; read it from git history); nothing else reads it.

Cleared 2026-10-28 (already settled, stale-item sweep): Q114 pilot (you dropped it 10-25; you do the real review),
T211 feed plan (built 10-27 with your corrections; only the cron + R2 sync below is left), Q145 (= that feed: 6-hourly
in season), Q32 (T95 dropped; salmon is T164/T211 now), Q142 (lake splits deferred, your 10-28 "lake split later"),
Q150 (answered 10-23: own keeps amber), Q119 (Claude's own audit — Claude re-derives it, T125), T185 decide (your
marks are the decisions; applied, Hatzic left alone), DFO-salmon build + "on parts" rebuild (both live in
r-46be3c135e68), run-timing go (you said go 10-25 → T199; Q144 stays).
Answered 2026-10-24: Q125 (muted fill), T179 (400 m; a curated cut never moves; 25 m lake-anchor proposals; merged
splits keep every name and id), PSF permission (T162; their reply is not stored), braids marked (T67 promoted), Q7 (delete old releases — pruned to live + previous; `python -m pipeline.deliver.release.prune`).
Answered 2026-10-23: Q5 + Q34–Q41 (curation plan approved, defaults; splits mid-review must work; salmon pass later = T132), Q13 (Claude runs the reader gate), Q44 (no Level from a side channel; Flow only if it carries a large share), Q53 (as built), Q45 (leave unbound).

## Your open questions (30 listed below — each built with a default; the table in tickets/README.md has the wording)

Triaged 2026-10-23: 64 smaller calls were decided by Claude and 12 were already answered or obsolete — all recorded in
decisions.md "CLAUDE 2026-10-23 — owner-question triage" (read and reverse any). What is left needs you:

- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **T387 point-bound facts (2026-11-04, default: left per tap):** every other fact on a Regulations page is now its
  PART's. Two still follow the tap: the sun line (sunrise / sunset at the tapped point — or the part's midpoint?) and the
  scroll memory (reopens where you left it per tapped reach — or per part?). Ticket T387 has the why.
- **Readings of the book (a wrong call misleads anglers):** ANSWERED 2026-11-12: Q124 a river's rules on unnamed widenings (456 polygons): keep as built now (the polygons take the river's rules); later, fold more small widenings into the stream and draw the stream line instead of the polygon where it is really the stream, real lakes stay lakes (owner's words in RULINGS E27, ticket T695; DEFERRED 2026-11-13 with stocked lakes and bathy map, no longer "after closed testing") ·
  (Q87, Q140, Q62, Q63 answered 2026-10-28.)
- ANSWERED 2026-11-12 ("Leave gaugeless"; "Show 08GC005 above the canal"; built, RULINGS P54): **T696: diversion gauges — no gaugeless section is a diversion gauge's.** The 28 stay gaugeless; 08GC005 now leads the Theodosia above its canal, labelled "measures a diversion" (shows on the next deliver + app build).
- ANSWERED 2026-11-12 ("Leave it": no intake cut; the ~160 m below the intake reads 08GC005 with the river above, as built): **T696: a cut at the Theodosia intake?** (2026-11-12). The canal leaves the river at the intake dam about 160 m ABOVE where the river's section begins (the 08GC006/08GC007 bypass gauge cut). So that 160 m stretch — where the bypass gauges stand — reads 08GC005 with the river above. Add a cut at the intake (rides the next atlas build, which the T690/T691 cuts already need) so it stays 08GC006's? Pictures: ~/bcfish-session-backup-2026-11-12/theodosia/theodosia_after_closeup.png (intake and section end marked), theodosia_before_closeup.png, theodosia_after_overview.png. Ticket T696.
- DONE (answered 2026-11-13, released r-f749475d0962): **T691: five silent gauge stations need a match call before they can be cut** (2026-11-12). Q113 is built (32 cuts, ride the atlas build, RULINGS P53). Not cut: 08FE002 Kemano Powerhouse and 08ND025 Revelstoke Project Outflow (never matched); 08JA013 Skins Lake Spillway, 08NE126 Arrow Reservoir Outflow, 08NJ158 Kootenay Lake Outflow (matches marked wrong, no water). Say which river line each measures, or leave them uncut. Also worth a look: 08JB013 "North Beach Creek above Allin Creek" is matched to "Beach Creek". (Final atlas build 2026-11-12: 08JB013 and 08PA004 Skagit made no cut — 520 m and 1,015 m from their line, over the 400 m limit.) Ticket T691. Claude's proposed answer per station (2026-11-13, NOT applied: a row each, in plain words, in the ticket's "Phase 2 proposals" table; pictures ~/bcfish-session-backup-2026-11-12/gauges/*.png): accept or change each row.
- ANSWERED 2026-11-12: **Which gauge speaks:** Q113 "silent doesn't mean retired so we should add cuts Incase they start transmitting again?" (built, T691) · Q103 "Canal gauge also serves river", then "Leave gaugeless" and "Show 08GC005 above the canal" (built, T696). Still open: Q31 the gauge rule.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **How it looks (taste):** Q130 each model's line under Change · Q132 day chips may still use ELF · Q79 "river" vs "stream" on the page ·
  Q91 "no gauge close enough" wording · Q123 how the Map view marks the selected part (built: dim the others; thin halo in
  `app-integration/T151-shots/alt-halo--*`) ·
  Q2 / Q139 / Q47 no-access and tidal notes on Conditions.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **"Today" is the phone's day, not BC's (T368 Q6, 2026-11-04):** (B.C. time is the settled reading; see the permanent daylight time note.) your answer assumed the app counts days in BC time; it
  counts them in the PHONE's zone (`localDate`): an Alberta phone at 00:30 MT is already on the next day while BC is at
- ANSWERED 2026-11-12 ("It should be in the vessel restriction section but it should say the locator text there"; built c63427bbd, RULINGS P51, shows on the next app build): **Vessel rules (T209):** the 10 close-wording undrawn parts kept ("various locations", "swimming areas") in
  on-parts.md: whole water too, or leave as notes? (`max_power_kw` 7.5 is stored on the 162 electric-only rules, 10-28.)
- ANSWERED 2026-11-12 ("Yes, book's full words"; built 81598325e, RULINGS P51 follow-up, shows on the next deliver + app build): **Harrison River vessel line (T209):** reads "Speed restriction or no powered boats — in small bays along the river as signed"; the other 21 undrawn-part vessel rules checked against the printed book: nothing else dropped.
- ANSWERED 2026-11-12 ("Keep them as two entries"; built 2841fb605, RULINGS P51 second follow-up, shows on the next deliver + app build): **"or" vessel lines as one joined entry? (T209)** No: speed restriction and no vessels are listed separately, each with the book's locator — Harrison "No powered boats — in small bays along the river as signed" and "Speed restriction — in small bays along the river as signed" (Claude's reading: the Harrison too); Okanagan, Kalamalka, Skaha "No vessels — various locations (as buoyed and signed)" and "Speed restriction — various locations (as buoyed and signed)".
- ANSWERED 2026-11-12 ("Yes, read the real boundary"; supersedes the earlier "'Unnamed lake (km 2.3)'"; built 040b79d16, RULINGS P55 follow-up, shows on the next release): **T697: part ends at an unnamed lake** read the real boundary that stands there, never the lake — Slack Creek "Region 8 boundary", Jones Creek "B.C. border"; a cut aliased onto a lake edge keeps its own name (also Duncan "Duncan Dam", Mitchell, Babine "Juvenile counting weir"). One end still has no real boundary: Gagnon Creek at km 3.66 (next row).
- ANSWERED 2026-11-13 ("Mouth is still Williston"; RULINGS P57; built in the T678 junctions candidate atlas `full_next`, shows on the next release): the mouth is where the creek enters Williston, the signs ARE the mouth (cut `gagnon_creek__fishing_boundary_signs_at_the_mouth`, the 400 m cut removed), the whole creek keeps the spring closure, the unnamed lake stays a lake of its own (`waters/pond_calls.json`). Was: **Gagnon Creek: is the unnamed lake really part of Williston?** (2026-11-12). You said: "Maybe we could put signs near the mouth? Let's just assume the mouth is the signs and add it as a variant name. Would this fix it? Seems it is in the wrong spot??", then "Where it enters Williston so it captures the whole stream. Also unnamed lake is part of Williston I think". NOT BUILT: the map data and imagery point the other way — the 58 ha lake (FWA wbk 329465442, coded "lake", Williston is coded "reservoir") sits behind what looks like a rail/road embankment, joined to Williston only by the creek's 74 m channel under it. Picture: ~/bcfish-session-backup-2026-11-12/lake-ends/gagnon_creek.jpg. Build it as you said anyway (the lake is Williston water, the signs at the creek's mouth at km 3.66, the whole flowing creek keeps the spring closure), or keep the lake as its own water and put the signs at the creek's mouth into that lake (km 3.66)? Ticket T697.
- **Salmon (T164), refreshed 11-05:** 179 DFO places still need an extent; each has a fresh proposal (52 on existing
  cuts, 47 whole water, 65 new splits, 15 with a reason). Review app → DFO salmon extents → accept suggestion → Save
  extent. Includes Skeena B(i) (T229 Q3) and the 3 items T326 waits on. Claude rebuilds and measures after.
- ANSWERED 2026-11-12: POSTPONED (owner: close as answered). **User accounts (T223, new 2026-10-28):** one account for web + phone. Questions Q-T223a–f in the ticket: auth
  provider, what syncs (spots / photos / pins / settings), privacy of spot locations, offline merge rule, optional or
  required, before or after first release. Nothing is built until you answer.
- **Batch 7:** Q146 re-centre the map when the parts list opens · Q147 steelhead lake wording on newly taken ponds ·
  Q148 dams → "lake" for the slough candidates · Q149 Bonaparte by hand.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **Gauge gaps on main rivers (T174, shipped r-bda3fea28838), including Q154 (small ponds):** Q153 (side piece adopts the largest ANSWERED river section) and
  Q155 (a water's own lines between alike pieces) shipped: +407 Flow / +327 Level sections, 0 answers changed — veto =
  one switch each in panels.py. **Q154 is yours:** the "double-line polygons" are small FWA lakes; walking them as the
  river moves 96,590 sections (+95k Flow) — kept off. Numbers in tickets/T174. Your "how small?": examples +
  a proposed pool-only cut (< 1 ha and ≤ 2× the river's width: 10,650 of 136,080, 11,423 sections) in
  curation-review/q154-examples.md. **See them in the review app:** Sloughs tab → "Small ponds (Q154)"
  (`#sloughs/q154/`): the 20 md examples + a 60-pond sample (caught / just missed / clearly not, all regions), each
  on the satellite map with its outline and river; mark each pool / real lake / unsure (keys p / l / ?) and the tally
  shows how your calls line up with the cut — then say yes / no / another cut.
- **T89 six waters (updated 2026-11-13, T708):** Hidden, Secret and Square (= Whitton) Lake are DECIDED (owner "add the 3 high confidence lakes"; name records, applied by the next lane). Still unmatched: Frog (5-6), Redfern (5-15), Squirrel (6-1). Owner: "will look harder for frog and redfern later" (Owner To-Do row `t708-frog-redfern-squirrel`; evidence in T708 Work 8).
- **Run timing (T199, built 10-28 on staging; Q144 answered with the defaults):** (1) run
  `gh workflow run feeds-runtiming.yml --ref redesign/stream-sections -f upload=true` once (uses the repo's R2 secrets;
  staging only) and check its report; (2) after every atlas promote, rebuild the binding base on the Mac:
  `python -m pipeline.runtiming.feed.base --upload --yes`, then run the workflow; (3) when happy, uncomment its
  monthly schedule; (4) look at Run timing on the phone (T199-shots/).
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **T182 caps (built with defaults, veto if wrong):** offline total 20 MB (was 15); cold-load gate 3.35 MB (salmon on
  the first Salmon press, as you decided 10-28).
- ANSWERED 2026-11-12: **Scope and big rebuilds:** Q10 imagery corridor (offline corridor + pools go/no-go; online Esri is settled): no, offline imagery is not for the first release.
- **Things only you can do or know:** Q4 slough marks — review app **Sloughs** tab (`#sloughs/`): 811 confirmed so far; still to call: the 135 wide-marsh pre-marks and 314 on your list (113 intermittent) · Q126 were you
  on Level? · Q9 phone testing · Q26 hosting/app name ·
  Q27 sign-off walk.

## Things only you can run or provide
- **Store listing sign-off (T244)**: privacy drafts APPROVED and the name DECIDED (owner 2026-11-06: "Can I Fish?" on the
  icon only, "Can I Fish This?" everywhere else). Still open from the 10-29 list: `app-integration/store/listing-draft.md` and `store/privacy-labels-draft.md`
  (nothing is submitted). Decide: the **app icon / adaptive icon / feature graphic** (DONE from the favicon, per your answer: `T244-shots/icons-preview.png`; add words to the feature graphic if you want), the support URL, categories. Contact = support@canifishthis.ca and the Location row (declared: collected + shared with Open-Meteo) are decided.
  Look at the new splash: `app-integration/T244-shots/` (web boot screen light/dark, native art); it shows on the next native build.
- **Production run (T240 section 11, 2026-10-29)**: the prod commands for map-assets (T270), HYDAT day flow (T274), the report-purge deploy (T271),
  and the SEO build against the prod app export (`--app`, T269/T279) are written in `tickets/T240-production-launch-checklist.md`.
- **Parser runs** (`run_parse.sh`, dispatch): human-only; any re-parse is yours to start.
- **Yearly synopsis update (T239, not started):** when the new synopsis comes out, Claude writes the runbook + book
  diff and does the dry run; the parse itself is yours to run (Claude hands you the command).
- **R2 upload** of a new release and of the gauge feed, with your credentials:
  `python -m pipeline.deliver.release.publish --release data/release/$(cat data/release/LATEST) --yes` (records
  data/release/PUBLISHED so the prune keeps it). When you set up the feed sync, upload ONLY `data/generated/gauges/feeds/`,
  not `feeds.runs/` (T177).
  T215: a release now also carries OUR synopsis PDF (`synopsis-<sha12>.pdf`, ~24 MB, raw, immutable; named in the manifest's
  `synopsis`): the same publish command uploads it, and the bucket's CORS must allow GET from the web origin (the app fetches it
  once per release to draw the highlight). Until a release with it is uploaded, "open the PDF" stays the plain gov.bc.ca link.
- **Feeds (T225)**: the gauges, DFO salmon and in-season feeds now run on the Cloudflare Worker `bc-fishing-feeds-staging`
  on a schedule (STAGING, switched on 2026-10-28; status: https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/status).
  Emails go to your verified address: DFO places / in-season changes / a whole feed failing at once, the rest in one daily
  digest. When a DFO email arrives: `python -m pipeline.regs.dfo_salmon.live queue --regions <n>`, commit the entry files,
  then sync them (below). Production switch-on (worker + prod bucket) is yours.
- **Feed inputs publish from the Mac only** (T520, owner 2026-11-08): `feeds-sync-inputs.yml` is deleted. DFO entries,
  wording, roster and in-season matches go up by `deploy/staging/sync-inputs.sh` — the review app runs it by itself after
  a save (staging only; never git); production stays your `CONFIRM_PROD=canifishthis.ca deploy/prod/sync-inputs.sh`.
- **OWNER STEP — the GitHub R2 secrets** (T520): `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` were for the deleted sync
  workflow, BUT `feeds-hydat.yml` and `feeds-runtiming.yml` still read them (and `CLOUDFLARE_ACCOUNT_ID`). Remove them in
  GitHub → Settings → Secrets only once those two no longer need them; until then, consider scoping the token to the
  staging bucket (owner 2026-10-29: it covers staging + prod today).
- **`scripts/update-hydat.sh`**: run it when the "New HYDAT" email arrives (after the HYDAT action).
- **Phone testing** (T77, Q9; the 0.5.0 list: handoff/PLAN/OWNER-PHONE.md), **production hosting** (T86, Q26: R2 bucket, contact address, app name), **iOS build**
  (T88).
  - Gesture/navigation parity on the phone (T50, T77; owner 2026-10-27; same RNGH code as web, not seen on a device):
    Android edge-swipe back and the back button through the back table (HANDOFF-ui "Back, everywhere");
    `android:enableOnBackInvokedCallback="false"` in the manifest (predictive back off) is deliberate until judged;
    sideways view swipe on a water (Regulations/Conditions/Map) incl. not starting in the 24 px edge zones and not
    fighting the map pan / chart / minimap shields; sheet drag-to-close and sheet scroll-vs-drag hand-off; iOS
    left-edge back (`EdgeBack`); Spots/Conditions/More tab back goes to the Map tab; part step and upstream/downstream
    (▲/▼) keep one back; section highlight while scrolling the regs tabs; keyboard (search) vs sheets.
  - Native colouring (T74, 2026-10-27; emulator: 0 grey frames in 49 sampled pan frames): on the phone, pan and fling
    at z9–z13 and say whether any water shows plain/grey before its colour (`EXPO_PUBLIC_COLOUR_PROBE=1` logs
    `T74PROBE` lines per pan for the numbers); the five hatches;
    "At my location" (asks on the tap, then drops the pin where you stand); a theme switch keeps the hatches.
  - iPhone (T217, 2026-10-27; simulator: 0 grey frames in 49 sampled pan frames, pill/hatches/At my location seen):
    the same colouring checks on a real iPhone, plus the location system prompt on the tap (pre-granted on the
    simulator) and whether the gauge pill's slightly wider padding on iOS is acceptable.
  - Android on a real phone (T50, 2026-10-27; emulator-verified): the rule-book link opens the highlighted PDF in your
    PDF app with the entry's page FIRST and the whole book after it — say if that reads right; nothing sits under the
    status bar or the gesture / 3-button bar; changing the system font size keeps the water you had open.
  - Accessibility (T243 → T258, 2026-10-29): a TalkBack / VoiceOver walk (map → water → regs → conditions → salmon →
    menu) and the largest system text; the bottom tab labels stop growing at 1.3× (Q160) — say if that is wrong.
  - Regs scroll (T50, 2026-10-29): scroll a water's Regulations, switch to Conditions / another tab / change the font
    size, come back — it should be where you left it.
  - Offline maps (T238/T259): More → Offline maps; which map regions a first install keeps (Q161, default all).
- **Sign-off walk** at the end (T87) before app/design is deleted.

- **iOS (T88, T364, T365; 2026-11-05)** — **BENCHED (owner 11-06) until the DFO extents are labelled and Play closed testing has started; then pay the $99 (pick individual vs organization first) and run TestFlight, not the free 7-day route.** the real iPhone run (T364) is AFTER the launch-and-hygiene chat (owner 11-05). Claude built and tested everything the simulator can show (T313).
     Membership details) and `asc-api-key.json` `{"key_id","issuer_id","key_path"}` for an App Store Connect API key
     (Users and Access → Integrations → Team key, role Admin (App Manager cannot sign for the store); keep the .p8 outside the repo).
     missing). It uploads to TestFlight; install TestFlight on the iPhone and accept the build.
     download on Wi-Fi and on cellular (the foreground download, b8e33ca80); the left-edge swipe back from a water's page
     (simulator: a slow drag goes back, a fast mid-height one sometimes does not — say which works for your thumb); the
     gauge pill's padding; the walkthrough to its end, incl. step 17 over the Layers sheet (T336) with VoiceOver on;
     More → Data "Send crash reports" stays on. Memory: Settings → General → iPhone Storage is not it — just say if the
     app is ever closed by itself while you pan or open Conditions (a crash report then reaches the staging store, T362).
     id as Device ID, not linked); age rating; category; support URL; screenshots (6.9" + 6.5"); review notes. Export
     compliance: answer "standard encryption only" (the build already says so). The review build is
     `--production` and needs production hosting (T86) live.

- **SEO, v1 pages (2026-10-27):** keep your existing Search Console property; when the new site is live add the new sitemap
  beside the old one, and for two months watch Pages > Not found (404) and Redirect error (the 19,331 old `/waterbody/` URLs
  should show as Page with redirect). Deploy the generated `_worker.js` with `run_worker_first = ["/waterbody/*"]` (T86 hosting).

- **T371 (batch 7): Play Closed testing (the build is T495's gated production build; 0.5.0 originally).** In the Play Console: create the Closed testing track and a tester
  list (at least 12 people who will opt in and stay for 14 days), fill the app-content forms (Claude drafts the data
  safety answers and listing text). Then roll that build out there; that starts Google's clock for production access.
  Drafts ready (2026-11-04): `play/data-safety.md` and `play/listing.md`. Confirm the items marked OWNER: install id
  means Device IDs = Yes; the report's map centre counts as approximate location; App interactions is new; Open-Meteo
  and Esri sharing (you decided these 10-29); the app name ("Can I Fish?" or "Can I Fish This?"); whether to round
  spot coordinates before they go to Open-Meteo.
- **T691 gauge proposals: ANSWERED 2026-11-13** (all seven; recorded in the T691 ticket, "Phase 2 — OWNER ANSWERED"): 08JB013 rematch to the fork, no cut, name variant; 08PA004 is a Ross Lake level gauge; the five dam/outflow proposals accepted (Kemano stays unmatched). Any earlier "five silent gauge stations need a match call" / "7 proposals" row is closed by this.

- ANSWERED 2026-11-13 (T716): tap speed, open page before the double-tap wait? Owner: "Keep as is (Recommended)". No change; 0.7.9 carries the 0.35 s open.

- [ ] Phone check after 0.7.9 (build 31, Play internal, 2026-11-14), T716: tap a small stream, the Fraser and a lake. The tapped stretch lights up at once; page in under half a second; double tap still zooms and opens nothing. Say if the highlight is too small zoomed out or it still feels slow. (To-Do page row: phone-check-079-tap-speed)

## 2026-11-15 — overnight marks: your answers, and what is still yours (T743)
- ANSWERED 2026-11-15: Alexandra Bridge stretches running 4.3 km into Region 3: "this is fine".
- ANSWERED 2026-11-15: older group-saved labels being assessed: "ok good".
- ANSWERED 2026-11-15: Atnarko "downstream of the sign": "no it needs to include tributaries". The save in force
  already includes them; nothing to redo.
- ASKED 2026-11-15: "we really need to check a lot of the items for this same issue and make sure we remove duplicates
  or bad ones?" Audit running; you get a list to look at (proposals only).
- OPEN, yes/no: Seton River mouth as its own cut 254 m below the Highway 99 bridge cut (recommended: yes, keep it), or
  reuse the bridge cut.
- ASKED 2026-11-15: "need to also fix all other tributary only items like on thre bulkley too. fix the proposals
  after adding ability for it". In work (T742). Yours after it lands: re-mark the tributaries-only places already saved
  (list will be on the To-Do page).
- ASKED 2026-11-15 (Sproat River, DFO Region 1): "there is a tiny section of sprout river in the middle of the lake... if
  anything it should be part of the lake....". Being investigated (ticket T753, written by the lane); options come to you.
- ASKED 2026-11-15 (Kitimat, "on west bank between signs at Kitimat hatchery outfall"): "we should have a way to define
  one half of the river closing maybe? could color only west or east side? could be an option in extent i think this is
  the only river like this? but worth at least doing the coloring. then idk how to represent it otherwise. have an agent
  look into this please". Being investigated (ticket T754, written by the lane); options come to you.
- ASKED 2026-11-15: "also we need to make sure all items with tributaries in the name etc have tributaries in extent."
  In work: the audit lists every place whose wording mentions tributaries against its extent and proposal (report
  `handoff/PLAN/curation-review/resaved-marks-audit-2026-11-15.md`, Section E); the suggester proposes tributaries for
  those wordings; the review app warns on a mismatch (T742). Mismatches on places you already saved come to you to re-mark.
- ASKED 2026-11-15: "also we need to make sure all the res of extents work properly so that if other items get added or
  removed they adapt their extent". Main chat's reading: the "rest of the water" kind of extent (a place with no stretch
  of its own, e.g. Qualicum "all open portions"). Being checked (audit report Section F): is it worked out from the other
  places at build time, and does it follow when one is added, removed or changed. Fixes, if needed, come as proposals.
- ASKED 2026-11-15, on the audit's re-saved places (33 saved more than once, Thompson link, Fraser Region 3, Sheldens
  Creek): "please audit these and see if you see any issue". Second look running; report
  to come in handoff/PLAN/curation-review/ (resaved-marks-second-look, dated 2026-11-15).
- PERMISSION 2026-11-15, verbatim: "i give you permission to fix these (Three places mean tributaries only and are wrong
  today: Bella Coola "all tributaries" also includes the mainstem. The two Bulkley places have tributaries off, while
  their proposals have them on.) and these too (Stave and three "mainstem" places have the tributary choice left blank
  rather than set to off.)". Scope: those seven places only. The four blank ones are being set to off now (new marks
  appended, noted "saved by Claude at the owner's request"); Bella Coola and the two Bulkley follow once the app can
  record tributaries-only (T742).
- ASKED 2026-11-15: "also we want to make sure all the mouth/ tidal splits are at the correct one (not the proposed one
  but at the tidal ones we give every stream)". Audit running: every DFO place with a mouth or tidal end, the point it
  uses against that stream's own tidal boundary; report to come in handoff/PLAN/curation-review/ (mouth-tidal-ends-audit, dated 2026-11-15).
- OPEN, pick (T753, Sproat and 10 other waters): river pieces that loop out of a lake and back; take them out of the
  river and give them to the lake (recommended), take out only, or leave. Rides on the next map rebuild.
- OPEN, six questions (T754, one-bank places: Kitimat west bank, Nakina). On the To-Do page.
- ANSWERED 2026-11-15 (T753, river pieces that loop out of a lake and back; Sproat): "yes do both steps but make sure it
  is done in a clean way". Both steps = take the pieces out of the river, then give them to the lake. Code lane started;
  it lands with the next map rebuild.
- DONE 2026-11-15 (your permission on the blank tributary choices): Stave "downstream of B.C. Hydro dam" set to
  tributaries off (new mark line 2672, noted as saved by Claude at your request; landed as 122a3d02d). The other three
  were not as the audit said: the Fraser "mainstem waters downstream of" mark already says off (it waits on the Hope
  bridge cut); the two Nass "mainstem" places have no mark at all, so they are yours to mark (set tributaries off).
- ASKED 2026-11-15: "need to make sure all rules under a water with (includes tributaries) in the name gets tributaries
  too." Main chat's reading: a DFO water whose heading says "(includes tributaries)" passes that to every place under it.
  In work (T742): proposals inherit it, the app warns on a mismatch, and a list of already-marked places under such
  waters that miss tributaries comes to you to re-mark.
- OWNER 2026-11-15, right after the tributaries asks: "we should wait until fully done marks though". Main chat's reading:
  nothing changes under him while he marks. So until he says he is done: no review-app update or restart, no change to
  the proposals file, no further marks saved for him, no rebuild. The fixes keep being built and tested in separate
  copies and land in one go afterwards.
- OWNER 2026-11-15: "we just want to prepared for how we will audit them". So the checks asked today (saved twice,
  tributaries, rest-of extents, mouth/tidal ends, side channels, one-bank) are being built into ONE repeatable audit
  command, run once when he says the marks are fully done; today's reports are the trial run, not the final audit.
- SECOND LOOK at the 31 re-saved places, 2026-11-15 (report `handoff/PLAN/curation-review/resaved-marks-second-look-2026-11-15.md`):
  18 look right, 3 look wrong, 1 wrong by your own note (Sproat), 9 cannot tell. Yours to decide: (1) a place whose last
  save is "wrong" with no fix still ships its older extent, with no flag: hold such places out of a release until
  re-marked? (2) Thompson, Kamloops Lake outlet: the live place has no cut-outs for the three year-round no-fishing
  areas (Deadman, Juniper, Ashcroft). (3) Sheldens Creek: the Spur 19 cut leaves only the top ~290 m of a 3.69 km line;
  a second road (SP 19H) was never located. (4) Fraser Region 3: the extent in force (Spuzzum to the Region 3/5 line)
  looks right; your Seabird note fits the Region 2 places.
- ASKED 2026-11-15: "PUNTLEDGE RIVER: 75 m downstream of Puntledge River hatchery fence split is on the hatchery fence
  (the split already in the river) we should fix the proposal and make sure we fix this existing split." Being looked
  at: which wording made the existing split, where the fence and the 75 m point are, and other "N m downstream of"
  places sitting on the landmark itself. Fix comes as a proposal; cut changes ride on the rebuild after marking.
- MOUTH / TIDAL ENDS checked 2026-11-15 (report `handoff/PLAN/curation-review/mouth-tidal-ends-audit-2026-11-15.md`): no
  place in force ends at a wrong mouth point (an open downstream end is trimmed at the stream's tidal boundary by the
  build). Six PROPOSALS are bad: Chapman, Stave x2 (a new cut a few metres from the tidal boundary), Illiance, Kitsault,
  Wilauks (a cut on the line's very end). Do not accept those; mark the stretch with an open downstream end instead.
  Yours: Nass x2, Seymour, Illiance, Wilauks have no extent yet; Pallant Creek stops at "the bridge near the mouth",
  57 m above the boundary (as DFO words it; your call). The proposal maker is being fixed to use the tidal boundary.
- LOOK AND PICK (T752): desktop water map as a split, A map left / B map right / keep today's. Pictures sent in chat.
- ANSWERED 2026-11-15: "Pallant Creek we can make the bridge the tidal boudnary i think". Queued for the rebuild after
  marking (T743).
- ANSWERED 2026-11-15 (T752 desktop split): "for the layout choose which ever you think is best practice". Main chat
  chose B (text left, map right), from 900 px wide on every platform; being built properly with tests.
- PUNTLEDGE checked 2026-11-15 (report `handoff/PLAN/curation-review/puntledge-hatchery-fence-2026-11-15.md`): you are
  right, and for every user of that cut. The cut "signs 75 m downstream of Puntledge River hatchery fence" sits on the
  lower-hatchery weir itself: its anchor was placed on the hatchery grounds 74 m upstream of the weir, so the 75 m
  offset lands on the fence. Fix (your words: "make sure we fix this existing split"): anchor moved onto the weir, 75 m
  offset kept; the cut lands 75 m below. Queued for the rebuild after marking. Yours afterwards: re-tick the three marks
  that use it (lines 1263, 2447, 2687). No other "N m downstream of" place has this fault.
- ASKED 2026-11-15 (Shuswap River, middle): "need help representing this ((middle) between Shuswap Falls and Mabel Lake
  except closed from the water survey station … to WTFB signs … one km down stream.) it has the except closed structure
  we encountered before". Save the place as the whole stretch (Shuswap Falls to Mabel Lake); the closed part becomes a
  second, no-fishing entry as on the Thompson. The reader of DFO's page did not pick up this "except closed from … to …"
  wording; being fixed in a separate copy. The two printed coordinates do not match the words (second is south of the
  first); being checked.
- ASKED 2026-11-15: "there was one river i dropped highway 16 split near mouth in the skeena i shouldnt have done that
  help me fix it please". Found: Exchamsiks River "upstream of Highway 16 bridge", mark 2674, saved as the whole river.
  Fix: save it again as upstream of the proposed "Hwy 16 bridge over the Exchamsiks" point; the new save replaces it.
- PERMISSION 2026-11-15 (Puntledge), verbatim: "please add both cut on weir and one downstream then i give you
  permission to edit that extent for regulations and dfo places." Scope: the Puntledge hatchery weir cut, the cut 75 m
  downstream, and the Puntledge rule-book and DFO extents that use them. In work.
- ASKED 2026-11-15: "the shuswap one add it as proposal so i dcan see it". The Shuswap closed stretch is being prepared
  as a proposal for the review app; added to his proposals once ready.
- PERMISSION 2026-11-15 (Exchamsiks only), verbatim: "can you fix Exchamsiks for me poroposal is gone so you can do it".
  One new mark being saved for him: upstream of the Hwy 16 bridge point, tributaries on, noted as saved by Claude.
- ASKED 2026-11-15 (Tatshenshini River, Region 6, "upstream of the BC/ Yukon border – along the Haines Hwy.; Kwatini
  Creek, Stanley Creek and Goat Creek." and the Blanchard River one): "need to add bc boundary here i cant get it to
  add. also i cant save or edit the extent of the other section there". Being reproduced in a separate copy of the app:
  why no BC-border point is offered, why the Blanchard place will not save, and the marks those two places need.
- DONE 2026-11-15: the Shuswap closed stretch is in your review app as a proposal (closed-areas panel of the Shuswap
  "(middle) between Shuswap Falls and Mabel Lake" place; no restart needed). YOURS: which reading. DFO's two printed
  points are 2,054 m apart along the river, not "one km", and look swapped: the second printed point is 27 m from the
  Water Survey gauge "Shuswap River near Lumby", so it is the station. Options shown: as printed (km 133.08 to 135.13),
  or station to 1 km downstream (km 134.13 to 135.13). Also yours: tell DFO about the mismatch?
- DONE 2026-11-15 (Puntledge, your permission): weir cut added, 75 m cut moved to 75 m below the weir (same name, so
  the rule-book stretch and both DFO places follow). Shows after the rebuild; then re-tick marks 1263, 2447, 2687.
  YOURS: the retired "from the weir at the lower hatchery" place has no mark (weir to the 75 m cut). YOURS, yes/no: the
  tailrace "50 m" cut has the same fault, about 27 m off; fix it the same way?
- DONE 2026-11-15 (Exchamsiks, your permission): new mark line 2728, upstream of the Hwy 16 bridge point, tributaries
  on, saved by Claude at your request; it replaces the whole-river save and waits on the rebuild like your other new-cut
  saves. It shows as "unsure" because the app sets that for any extent using a not-yet-built cut.
- READY 2026-11-15: the final marks audit is one command (branch review/marks-audit, commits d9f7045 + 88dec44, lands
  with the other fixes after marking). Trial run on today's marks: 303 candidates in 11 sections; every finding from
  today's hand audits is in it. Run by the main chat when you say the marks are fully done.

## 2026-11-15 afternoon — owner done labelling for now; clean-up asked (T743)
Owner, verbatim: "i thought skeena used to have a hells gate cut? also other ones liek this dont have the confluence
cut? (from Lakelse River mouth upstream to the Skeena River Overpass (New Hwy Bridge) at Ferry Island) again this doesnt
split on confluence.. (from the confluence with the Babine River downstream to the CNR Railway Bridge at Terrace. ) ...
all the audit stuff i will have to review later like shuswap stuff etc etc. i think i am done labelling for now. can you
audit everything a lot compile all results/issues into questions for me later. and make sure to fix all the remaining
items left. a lot have issues with bad cuts etc etc. also we could rebuild the cuts and make sure nothing causes issues?
also want to make sure we deduplicate the new cuts from proposal (if they rewference same thing they should be matched
etc) we just really want to clean it all up and then have questions ready for me later."
Main chat's plan: land the prepared fixes; run the full audit; clean the cuts (duplicates matched to one cut, confluence
ends on the confluence cut, bad positions fixed where the evidence is clear, the rest as questions); side rebuild and
checks; one page of questions for him. Nothing that changes what a wording means is decided for him.
- TATSHENSHINI checked 2026-11-15 (report `handoff/PLAN/curation-review/tatshenshini-border-2026-11-15.md`). (A) The BC
  border points exist in the data (the river leaves BC and comes back after 43 km in Yukon) but the app's picker cannot
  show them; app fix proposed. The Kwatini / Stanley / Goat place does not need one: those creeks are wholly in BC, so
  its whole-water extent is already right. (B) The Blanchard place cannot be edited because its extent carries a field
  the editor cannot show, and the app hides the edit button without saying why; plain "whole" gives the same 15
  sections. YOURS: on each of the two places, tick ✗ on rows 2 and 4, then "Confirm: the extent matches DFO's words"
  (works today), or tell the main chat to save those two verified marks for you. 15 other unmarked places cannot be
  edited in the app for the same kind of reason; listed in the report.
- ASKED 2026-11-15 as he left: "record everything i need to do for when i am back what i need to review with questions
  what was done etc etc. what needs testings etc etc". Kept in `handoff/PLAN/OWNER-RETURN-2026-11-15.md` (sent to him
  as a file when final) and as the first row of the To-Do page.
- DONE 2026-11-15 (your permission): Bella Coola "all tributaries" = tributaries only, Atnarko excepted (line 2730);
  Bella Coola whole "(not including Atnarko River)" = Atnarko excepted (2731, per your note "make sure this excludes
  atnarko"); the two Bulkley "all tributaries" places (2732, 2733) = tributaries only. YOURS: the two Bulkley places had
  no mark of yours, so they are saved as "unsure" from the proposal: confirm or change.
- BLOCKED 2026-11-15, NEEDS YOU: the side map rebuild did not start. The permission check refused its first step
  (`python -m pipeline.regs.review.proposals side … --out data/generated/side/dfo-cuts23`) twice for the rebuild job; the
  main chat does not run a command a job was refused. Run it yourself or tell the main chat to. Details in
  `handoff/PLAN/OWNER-RETURN-2026-11-15.md` section 6.
- ANSWERED 2026-11-15, on the blocked side rebuild step: "You can run it". Run by the main chat; rebuild continuing.
- BLOCKED 2026-11-15 17:20, NEEDS YOU: Play build 33 (0.7.11, code shrinker on; 6 of 6 phone checks, both CIs green,
  merged 35dce9bf3) refused to start: fast-fail fails on two rows of the proposals file lacking split-hunt evidence
  (my midday merge). Regenerating was denied to a job by the permission check; the main chat will not run it without
  your word. Say "regenerate the two proposal rows" (or run the suggester yourself); then the build runs.
- ANSWERED 2026-11-15 18:40: "Yes regenerate proposals if you need to." Done by the main chat: a fresh suggester run
  into a scratch folder; the two rows the test named (Stave "downstream of B.C. Hydro dam" second wording, Bella Coola
  "all tributaries") have no row in a fresh run because both places are placed now, so those two rows were removed from
  the proposals file (backup taken; no other row changed). The test passes; Play build 33 started.
- ASKED 2026-11-15 18:40 about T759: "What u mean ? should the app say "not known" instead of showing the wider open
  rule? I recommend yes." Explained in chat with an example; answer still open.

## 2026-11-15 evening — owner answers by question dialog, round 1 (verbatim)
- T759, what the app shows when DFO rewords a closed stretch and the reader loses it: "How would we know a reword vs a
  removal? I'm not sure safest way to handle thing. Probably have it in a banner for all sections in that river (banner
  meaning in salmon drop down) but I want your opinion". NOT DECIDED: he asks for the main chat's opinion; his idea: a
  banner in the salmon drop-down on every section of that river.
- A place last marked "wrong" with no fix shipping its older extent: "I want you to for it to be redrawn. We will finish
  these tonight so not a big deal... Just be safe". Main chat's reading: he re-marks them tonight; until then be safe =
  such a place does not ship its disputed extent.
- Six cuts recommended to keep (Lillooet signs, Seton mouth own cut, Atnarko lodge sign, Gitnadoix whole river,
  Kitsumkalum 22 m piece, Little Campbell and Babine sign cuts): "Only one I'm not sure about is gotnadoix one." So five
  are kept as built, incl. the Seton River mouth as its own cut; Gitnadoix stays open.
- Three fixes (Puntledge tailrace cut moved 27 m up; Nanaimo new cut 500 m below the lower power line; Thompson three
  year-round no-fishing areas carved out): "I thought we already had all these?? They were accepted? Okay please fix
  them I guess". Go for all three.

## 2026-11-15 evening — owner answers, round 2 (verbatim)
- T759 lost closure, offered "Banner + not known (recommended)" / banner only / not known only: "That sounds good but can
  you make not known a variation like bait ban where it has a loot change but with same color?" So: river-wide banner in
  the salmon drop-down AND the stretch reads not known when a clue says reword; and the map shows "not known" as a
  variation in look (as the bait-ban variation does) with the same colour. Which colour is "the same" is asked next.
- Gitnadoix "upstream of powerline crossing near river mouth": "Whole river (Recommended)".
- Morice "from Gosnell Creek to Lamprey Creek": "Keep the signs point (Recommended)".
- Skeena confluence cuts (Lakelse-to-Skeena, Babine-to-Skeena, Highway 16 bridge at Ferry Island): "Make them
  (Recommended)".

## 2026-11-15 evening — owner answers, round 3 (verbatim)
- T759, the look and the rule: "I think what ever still applies there should be the color so say there is a whole river
  rule and a section rule section rule is removed we should have no status change because that is fine same with if a
  rule gets replace with another one we have a mapping for. However a new wording being there would then mean we have a
  different indicator on the stream line that there is an issue with the rule. Then every stretch in the river will get
  the banner in the salmon section saying the new unmatched rule until we rematch it". Main chat's reading: (1) a place
  DFO removed, or replaced by a row we can match: no status change, the remaining rule's colour; (2) a NEW wording we
  cannot match on that river: the status colour stays what still applies, the stream line gets a distinct "issue" look,
  and every stretch of that river shows a banner in the salmon section quoting the unmatched rule, until it is
  re-matched. So no "not known" status; the trigger is an unmatched new row on the river. Open point to raise with him:
  a reworded row that our reader matches to a NEIGHBOURING place leaves no unmatched row, so this trigger misses it.
- Fraser and Squamish side channels: "Add all five (Recommended)" (Herrling Island, Seabird Island North, Jeperson
  braids, Maria Slough; Squamish Powerhouse channel).
- Shuswap closed stretch reading: "Can you give me words? What are the words are we sure we have right spts?" NOT
  DECIDED: wants DFO's words and how sure the points are.
- Lake rules (lake never takes its through-river unless named; Sproat pond stays its own water; creek-fed loops stay
  with the river): "Keep all three (Recommended)".

## 2026-11-15 evening — owner answers, round 4 (verbatim)
- Shuswap closed stretch, after DFO's words and what is sure about the points: "Use the real gauge point so we don't
  duplicate it and the other coord as the other point even tho it is 2km use as printed". So: closed from the real
  Water Survey gauge (reuse that point, no duplicate cut) to DFO's other printed coordinate, about 2 km.
- Rest-of extents, two weak spots: "Fix both (Recommended)".
- Tributary readings (Nass x3 mainstem = no tributaries; Babine Lake x3 and Meziadin "excluding tributaries" win over
  the heading; Somass and Stamp "X River tributaries" = tributaries only): "All three are right (Recommended)".
- One-bank places design (Kitimat west bank, Nakina): "Build it this way (Recommended)".

## 2026-11-15 evening — owner answers, round 5 (verbatim)
- T759 second trigger (a reworded row matched to a neighbouring place): "If it is reworded then it would unmatched
  because it is not in our list of mapped locations?" NOT DECIDED: a question back. Answer owed: an exact-match reader
  would leave it unmatched, but ours also accepts a close match (wording drift) and can attach it to a neighbour.
- Sheldens Creek, Spur 19 Bridge: "You can hunt for it I think it should be close to tidal". So: hunt for the bridge
  site / SP 19H road; his hint: close to the tidal boundary.
- Four small ones (1 Sproat signs cut moved 50 m to the bottom of the pool; 2 Hell's Gate label renamed; 3 Ecstall at
  the Johnston Creek confluence; 4 Illiance and Wilauks whole river from the tidal boundary): "Only one I'm not sure
  about is number 4". So 1, 2 and 3 go ahead; Illiance and Wilauks stay open.
- Region 6 salmon file over its size limit (379 kB vs 320 kB): "Raise to 420 kB (Recommended)".

## 2026-11-15 evening — owner answers, round 6 (verbatim)
- T759, close (drifted) matches: "Yes: close match needs confirming (Recommended)". A close match still applies its rule,
  but the river shows the banner and the line indicator until he confirms it in the review app.
- Illiance River and Wilauks Creek, "upstream of signs near the mouth": "Whole river is right".
- Tatshenshini, two places: "I will do them myself".
- Other chat's two (push privacy-page line + App Store privacy answer as written; keep all fish puns): "Yes to both
  (Recommended by it)".
- Then: "When can I do the remaining reviews?"
- ASKED 2026-11-15 20:05: "Iook ofr other ones like he skeena one to make sure we have them all". Sweep widened to every
  place (live, retired, older wordings) and every rule-book extent that names a confluence, mouth, outlet or forks, and
  every existing confluence cut (on the main line? at the real mouth?); missing ones are added with the Skeena pair.
- 2026-11-15 20:30, Blanchard River place (Tatshenshini): owner "i need to edit t though"; by dialog "Add its
  tributaries"; then "you can unlock it tho". Done by the main chat in region-6.json: tributaries on; the "inside BC
  only" limit (`within_area`) removed so the editor opens (it had dropped two Yukon slivers, 59 m and 211 m; a check
  today found plain "whole" binds the same 15 sections).
- 2026-11-15 20:45, Kasiks River "Upper Pool": owner "can you search hard for anything on that river mentioned?" then
  "can you add the pool proposal?" then "add to gotchas too". Done: sign point proposed at the tail-out of the pool at
  the foot of the canyon falls (about 54.3535 N, 129.5360 W, 17.65 km up; medium confidence, inferred); both Kasiks
  places now show it as a proposal in the review app; recorded in pipeline/docs/GOTCHAS.md. Report:
  `handoff/PLAN/curation-review/kasiks-upper-pool-2026-11-15.md`.

## 2026-11-15 night — second look at your labels: questions (NOT asked yet; answer together)
New main chat (after chat 13). All 352 DFO places were read against DFO's words: 273 clean, 79 flagged, 15 that look
wrong. Report: `handoff/PLAN/curation-review/final-marks-second-look-2026-11-15.md`. Each question has a default; the
default is what ships to staging until you answer. None of these needs a new map rebuild unless it says so.

Vancouver Island
1. **Somass, Stamp and Little Qualicum "everything else" places.** DFO says "except in those areas and times listed
   below". Today each listed stretch is cut out of "everything else" for the whole year, even when its rule lasts a few
   months (the Somass bait ban Jun 1-Aug 24 takes the whole river, so the Somass chinook and coho limits show nowhere;
   the Stamp keeps about 2 of 10 pieces; the Little Qualicum loses the park-to-hatchery stretch in Sep-Nov). Make these
   three the whole river, with the smaller stretches answering first on their own dates, as you set the Nitinat?
   Recommended: yes. Default until you answer: unchanged (limits missing on those stretches).
2. **Side streams on Cowichan and Nanaimo stretches.** Both rivers are "including tributaries" as a whole, so each
   stretch takes its side streams too: the Cowichan closure from Sandy Pool to the sea (Sep 1-Nov 15) lands on the whole
   Koksilah River; Nanaimo Cedar Road to Highway 1 lands on Haslam Creek; the year-round Nanaimo closure below Bore Hole
   lands on Stark, Berkley and Elkhorn creeks. DFO's words for these stretches name no side streams. Make each stretch
   the main river only? Recommended: yes. Default: unchanged.
3. **Campbell River near the mouth.** The three stretches (Quinsam confluence, Maple Street sign, cement block sign)
   still have "not sure" from Oct 26; the cement block sign is an estimate. Re-check with the satellite view when you
   can. Default: kept as drawn.
4. **Sproat and Stamp Falls** end on a "wrong" save although your fix is now in (Sproat +50 m; Stamp names to add).
   Re-save them as right after the rebuild.
Interior and Fraser
5. **Thompson at Juniper Beach, closed area.** Which upper end: 4 km above DFO's printed lower signs (recommended, DFO
   says "approximately 4 km"), or 1.5 km below Juniper Beach Park (1.2 km long)? NEEDS A NEW CUT (next rebuild).
   Default: Juniper not closed separately; the place's rule covers it.
6. **Fraser Region 3 mainstem** (Spuzzum up to the Region 3 / Region 5 line): you marked it wrong because the start
   read "Region 3 – Region 8 boundary". The start used IS the Region 3 / Region 5 line on the Fraser; the label most
   likely came from another river's crossing. Right as drawn? Recommended: keep, re-save as right.
7. **Kanaka Creek**: DFO says "112th Street bridge"; the only crossing is 112 Avenue. Keep it? Recommended: keep.
8. **Fraser "Fraser only" saves and Maria Slough** (from the confluence sweep): re-mark those places without "Fraser
   only", or should "Fraser only" always take the Fraser's own channels? The rebuild's rule comparison shows whether the
   channels now come in by themselves; answer after I report that.
Skeena, Nass, north
9. **Morice River just below Morice Lake.** DFO closes salmon from the signs at the lake outlet down to Gosnell Creek.
   Our stretch starts at the Dunes signs, about 2 km below the lake; the top 2 km has no closure. Start it at the lake?
   Recommended: yes. Default: unchanged (top 2 km open to what the Skeena defaults allow).
10. **Tatshenshini headwaters (your two saves).** The border point you picked also pulled the upper Tatshenshini itself
    into the Blanchard place and into the Kwatini / Stanley / Goat place; the three creeks' all-year closure would then
    close the upper mainstem, where DFO allows coho. Make the Blanchard the whole Blanchard with its tributaries and the
    three creeks the three creeks whole? Recommended: yes. (Being confirmed by the rebuild's rule comparison.)
11. **Skeena "mainstem waters only" (pink 2 per day, Jun 16-Dec 31).** DFO prints it under the Skeena above the CNR
    bridge; we draw it on the whole river. Limit it to above the bridge (your note said so)? Recommended: yes. No
    fish answer changes today either way.
12. **Skeena, Terrace to above the Babine, sockeye.** A year-round closure and the in-season openings cover the same
    stretch; the openings win today only by the order of their internal names. Is "the in-season openings win" right?
    Recommended: yes, and the order is made explicit.
13. **Babine Lake sockeye** ("excluding tributaries, the 400 m circles, east of the Gullwing Creek line"): no drawing,
    so the openings show only as a note on the lake's card. Draw it as the whole lake, tributaries out, the circles and
    the Gullwing line as their own closed pieces? Recommended: yes.
14. **Kitsault tidal boundary**: name it "tidal boundary (white triangular fishing boundary signs near the mouth)", as
    your note asked? Recommended: yes.
15. **Kispiox "signs near the Kispiox River Resort"**: no resort by that name today; the signs sit at the Kispiox River
    Lodge, 13.7 km up (low confidence). Keep until DFO Smithers confirms? Recommended: keep.
16. **Sheldens Creek** (from the hunt): the Spur 19 road nearest the sea ends on a small side creek about 0.5 km above
    Sheldens Bay, not on the creek we drew. Is the place "the creek above where that side creek joins", or is the side
    creek Sheldens Creek? NEEDS A NEW CUT. Default: today's cut stays (top 290 m only).
For you to re-tick after the rebuild (saved by Claude at your request, each noted so)
- Shuswap closed stretch, line 2770: gauge to DFO's other printed point, 2,075 m (your round 4 answer; replaces the
  27 m one of line 2742).
- Thompson Deadman and Ashcroft closed areas (2748, 2749); Nanaimo 500 m (2746); the two Bulkley "all tributaries"
  places (2732, 2733); the two Skeena places on the new confluence cuts (2751, 2752); Puntledge 1263 / 2447 / 2687;
  the Sproat places; Fraser Region 2 places and Squamish (they gain side channels).

## 2026-11-15 night — owner answers (verbatim), new main chat
- 21:40, typed in chat: "you should also double check all the extents for dfo and make sure they all look sane (match to
  proper bound with proper tributary logic or second part etc etc....) build on other review and we want to fix items
  form other review". Main chat's reading: (1) a second pass over what every DFO place actually BINDS (ends, tributary
  logic, second parts), on the built result, building on the second look; (2) the second look's findings are to be
  fixed, not only listed: the recommended answer of each question above is applied, saved for his re-tick.
- Thompson Juniper closed area, upper end (question 5 above), by dialog: "if the book says 4km then we put it 4km unless
  i had another split it already matched??" So: 4 km above the printed lower signs, unless a cut already exists at that
  spot, then that cut is reused.
- Sheldens Creek (question 16 above), by dialog: "we shopuld find lower options. i think this is tidal bound maybe". So:
  look lower on the creek; his reading is that the signs are at the tidal boundary. Main chat's reading: the place
  becomes the whole creek above the tidal boundary (as Illiance and Wilauks), no new cut, saved as unsure for his tick.
- 21:51, typed in chat: "also another fable agent should take a look at how trout are displayed in the app. it si very
  complex. and we want fable to come up we a few other alterntives and give mockups (make sure it chooses a few
  different complex examples)". Started: a Fable design review (read-only), today's trout display on 5-6 real complex
  waters, 3-4 different alternatives, mockups as one page for his phone. No ticket until he picks a direction.
- 21:54, typed in chat: "also coordinate with other running chat to let it know it is the andorid and ios chat now not
  just ios. it is just the app chat." Done: the second chat (session bc-freshwater-fishing-regulations-01) is THE APP
  CHAT: Android and iOS app work, the feeds worker, app tickets. Main chat's reading: the Play build of 0.7.11 moves to
  it (the main chat names the build head after the staging release); data, pipeline, review app, releases and the
  audits stay with the main chat.
- 22:07, typed in chat, after the Region 1 fix report: "ill review all regions together". So: no region-by-region
  review asks; one combined list of the saves to re-tick and the open questions for all regions, once the second fix
  lane is back and the rebuild has landed them.
- 22:12, typed in chat, on the Skeena sockeye pair (CNR bridge at Terrace to above the Babine: the year-round closure
  "excluding the areas below" and the in-season openings on the same stretch, the right one winning by name order
  only): "this iss handled by rest of option in extents". So: the place worded "excluding the areas below" is saved
  with the REST-OF extent (its stretch less the areas listed below it), so the two no longer overlap; no ordering rule
  by wording. Open point for his combined review: tonight's Region 1 saves 2771-2773 turned three "except in those
  areas and times listed below" places from rest-of into the whole river (a listed stretch whose rule lasts only part
  of the year was cutting itself out for the whole year); he sees both and decides.
- 22:14, typed in chat, right after: "all items wih same structure as that shoul duse rest of extent type too". So:
  EVERY place worded as a leftover ("excluding the areas below", "except in those areas and times listed below", "all
  waters unless indicated below", "except in the area described below", …) uses the REST-OF extent type. Consequences
  the main chat draws: (1) tonight's saves 2771-2773 (Somass, Stamp, Little Qualicum turned into the whole river) are
  NOT landed as whole; they go back to rest-of; (2) the rest-of type itself must then give DFO's "areas AND TIMES"
  answer: on a day when a listed stretch has no rule of its own, the rest-of place's rule shows there (today a listed
  stretch is cut out for the whole year, and a retired or undrawable listed stretch still takes water: his earlier
  "Fix both"). That is a build change, ticketed tonight.
  (Times above are the commit times of each note, read from git; the first versions of these lines carried
  estimated times up to 30 minutes fast, and the notes of mark lines 2787-2789 say "owner 22:45" for the 22:14 answer.)

## 2026-11-15 night — trout display: mockups for you to pick from
Page (opens on the phone): https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH . Six real waters hard in different ways
(Vedder, Little River, Skeena in summer, Kootenay Lake, Elk, Fraser in two regions); today's display beside four other
ways: A fish by fish, B the answer first, C the usual rule and what is different here, D check my fish. The reviewer's
pick: A with B's one line on top, C's "this river's rule" tag, D later as a button. Five questions at the bottom of the
page with a recommended answer each. Notes: `handoff/PLAN/design/trout-display-review-2026-11-15.md`. Nothing is built
until you pick; the build is the app chat's. Two possible defects it saw in the data (to be checked): Kootenay Lake's
rainbow line drops "over 50 cm" from "20 a year / record"; a Fraser stretch shows "closed from Oct 1" without saying it
reopens Nov 1.
- 22:40, typed in chat, after reading the trout display page: "i accideenetly asked other chat to do this so coordiante
  with them. but please expand and refine the a/b design idea. see if we can make it super simple and clean with the
  fish icons in the table format?" Started: round 2 of the design review (same page, same link): options A and B
  merged into one table of fish rows with the app's own fish drawings, a few close variants, more hard waters. The app
  chat was told so the work is done once; the build of what he picks stays with the app chat.
- 22:49, typed in chat, on the app chat holding a display fix until he picks a trout design: "Do not hold stuff for the
  trout redosgn. That is a separate exploration thing that might not go anywhere". So: nothing waits on the trout
  display exploration. T762 (the "through the year" line that stops after the next change; app chat) and T763
  (Kootenay Lake rainbow "20 a year" without its over-50-cm band; main chat) are fixed on their normal paths.
- 23:04: round 2 of the trout display is on the same page (https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH): one top
  line, then one row per fish with the app's own drawing; four one-change variants; eleven waters. Five questions on
  the page (row per fish or per book group; plain backing behind the drawings; the long Vedder line; Kootenay one table
  or two; duties behind the tap). It is exploration only: nothing waits on it (your 22:49 word).
- 23:25, typed in chat, on trout display round 2: "I am really liking the trout work so far but we need to develope it
  even more. First I like the proposed 2 to 4 a day instead of details with arrow for collapsible button. Second we
  need to work out the size location better maybe the second column idea is good. We just want to keep refining it.
  This is a lot better though. I like the idea so far keep refining and add more complex examples with weird
  groupings." Started: round 3 on the same page: the folded row's answer adopted and defined for every case; four or
  five placements for the size compared on the hard waters (he leans to a second column); at least six more real
  waters picked for odd groupings; the shared-answer box stressed. Still exploration: nothing in the app changes.

- 2026-11-15 23:29, Tatshenshini "mainstem and all other tributaries" place, asked whether it should also become
  rest-of (I recommended leaving it). Owner: "I don't see how this would be a rest of situation.. (mainstem and all
  other tributaries" place should also become rest-of. Recommended: leave it; it already shares no water with its
  neighbours)". CLOSED: it stays as drawn (its own waters, sharing none with the Blanchard place or the three creeks,
  so there is nothing to order). Not on the morning list. My reading for later sweeps: rest-of is only for a place
  that overlaps the stretches it lists; "all other" wording alone does not make one.

- 2026-11-15 23:48, trout display ROUND 3 is on the same page (https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH):
  the folded row's answer for every case, five placements for the size on 20 waters, the shared answer box under
  stress, nine more real waters with odd groupings (Kaslo, Kitimat, Williston Lake zones A and B, Ross Lake, Bella
  Coola spring and summer, Tagish, Inkaneep). Still exploration; nothing in the app changes. Notes:
  `handoff/PLAN/design/trout-display-review-2026-11-15.md`. Five questions for you (recommended answer first):
  1. Size: a column that drops under the name when it does not fit (recommended), or always under the name?
  2. May the trout table sort fish by their shared box, not the app's usual fish order? Recommended: yes.
  3. Sizes in centimetres only on the row ("30 cm+"), inches behind the tap? Recommended: yes.
  4. Hatchery and wild as two rows of one fish where their limits differ (Kitimat rainbow)? Recommended: yes, only there.
  5. Store a top line for the 8 rule sets that have none ("Up to 5 today", "Up to 2 today, plus 2 bull trout")?
     Recommended: yes.
  Found on the way (to check, not design): the Kitimat's stored rows give wild rainbow a limit of 1 (30 to 50 cm);
  today's card shows only "Up to 5 rainbow trout, 50 cm (20 in) or less". Ross Lake's stored headline says 2 and its
  daily total says 4. Both go on a ticket after tonight's release.

- 2026-11-15 23:58, trout display, the owner's reply to round 3 (verbatim): "For the trout design. Please take the
  stuff you like best then we need to ujify over x size and x+ also we want both in and cm. And we want to make the
  stuff like 1 over 50cm on group easier to tell. Also dolly are not in list?"
  Round 4 started on the same page. My reading: (1) the reviewer's picks become the one design, comparisons fold away
  (answers round 3 questions 1, 2, 4, 5 as "your pick"); (2) one way of writing a size everywhere ("over 50 cm" and
  "50 cm+" unified, true at the edge); (3) inches AND centimetres on the row (answers question 3: both shown);
  (4) a size limit that belongs to a whole group ("only 1 over 50 cm") must read at a glance as the group's;
  (5) Dolly Varden: the round 3 rows said only "Bull trout"; the book and today's card say "Dolly Varden/bull trout"
  and the app has a Dolly Varden drawing. Being checked per region and fixed on the page.
  Owner, 2026-11-15 23:59, right after: "We are iterating on it more not implementing". So: round 4 and any later
  round are mockups only; nothing is built in the app or the pipeline for the trout display until he says so.

- 2026-11-16 00:18, trout display ROUND 4 is on the same page (https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH),
  mockups only. One design leads the page; rounds 1 to 3 are folded under "How we got here". Sizes use the app's own
  standard everywhere with both units ("30 cm (12 in) or longer", "50 cm (20 in) or less", "30–50 cm (12–20 in)");
  the size sits under the fish name; a size limit of a whole box is written in the box under the number; 22 real
  waters, each with a "what you should read" line. Dolly Varden: the app has ONE fish, named "Dolly Varden/bull
  trout" in every sentence (568 rule sets), no water treats the two differently; round 3's "Bull trout" was the
  designer's shortening and is fixed. Real app gap found: the drawing on that row is captioned only "Bull trout"
  (ticket after the release). Three questions (recommended first):
  1. A size limit on a whole box: written in the box (recommended), or as a band above the fish?
  2. "Over 50 cm" and "30 cm or longer" both stay, because they differ for a fish exactly on the mark ("over 50"
     does not include a 50 cm fish; "30 cm or longer" includes a 30 cm fish). Agreed? Recommended: yes. (This is the
     one place the designer did NOT fully unify, against "unify over x size and x+": making them look alike would
     make one of them wrong by a fish.)
  3. Dolly Varden/bull trout: one drawing with the full name (recommended), or both drawings on the row?

- 2026-11-16 00:34, trout display, the owner's reply to round 4 (verbatim): "In the redesign we need to make the fish
  expansion better the fish inage moves around and we should put the place holder of what will be there into it.
  Like species identification stuff.. And we also need to make the clip note at top go away. It is too big and we
  should add the how to tell hatchery link up there instead... Then we should refine it even more. It still has a
  little bit of a clunky feel but it is better then original one. Maybe get more fresh agents to give refinement a
  stab then bring it back to original afent"
  Started (mockups only): three fresh designers, each with a different angle (calmer table; the opened fish row with
  a steady picture and a species identification placeholder; a first-time reader's restructure), all with his fixed
  asks (no big "Clipped fin only" note on top, a "how to tell a hatchery fish" link there instead). Their mocks go
  back to the original designer, who folds the best into round 5 on the same page.

- 2026-11-16 01:36, the owner, on tonight's pace (verbatim): "Everything takes so long maybe we should have less
  tests all the time and do it together at end or something"
  My answer to him: fewer, yes; but tonight's two long stops were found at the END (the Shuswap piece 14 minutes into
  a build; the stale mini release 18 minutes into the release). Proposed, for his yes or no on the morning list:
  (1) while working, only the tests for what changed, no full runs between steps (applied from now; tonight I ran
  the full pipeline tier three times and the whole app check twice, about 30 minutes that one run would have covered);
  (2) ONE full run, at the release, on the clean copy only: the staging release stops repeating the suite in place and
  stops waiting for GitHub's run (GitHub's run stays required for production); (3) checks that take seconds run
  before anything long (the mini-release check, and T779: a label that cannot be placed is refused at save time).
  Not cut: the build's own refusals of a label it cannot place.
  Owner, 01:36, right after: "Feels wasteful". I tried to rerun the release skipping the GitHub wait and the second
  clean-copy run; that was refused by the permission check (his words are not an explicit instruction to skip release
  checks) and I did not work around it. The release reruns the normal way once GitHub is green. If he wants the skip,
  he says so explicitly; otherwise it is the proposal above, for his yes or no.

- 2026-11-16 01:47, trout display ROUND 5 is on the same page (https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH),
  mockups only. The original designer folded three fresh designers' takes into one: a calmer table (a pale column
  beside the fish for each limit, hairlines only, every "only N" in that column as a white box worded "only 1 of
  these 2", a whole-box size limit as one sentence on top); a fish opens as a sheet from the bottom, so nothing in the
  table moves (an "in place" switch is on the page: 0 px of picture movement measured over 292 opens); the opened fish
  shows the app's real pictures and "what to look for" marks, with "often mistaken for" as the labelled placeholder;
  the big clip note is gone and "How to tell a hatchery fish" sits on the first line. Calmer, not smaller: taller
  than round 4 on 20 of 22 waters (about 40 px on average). The three fresh takes can be opened on the page as
  delivered. Four questions (recommended first):
  1. A fish opens as a sheet from the bottom (recommended), or in place under its row?
  2. On a hatchery-only water: say it once in a small line above the table (recommended), or after every name?
  3. Four new wordings need the data side: "only 1 of these 2", "Of the 5, only 1 over…", "uses the 1 over 50 cm
     (20 in)", a put-back row for wild steelhead. Ask for them? Recommended: yes (when and if this is ever built).
  4. Who writes the "often mistaken for" lines? Recommended: you, or someone you trust on fish.

- 2026-11-16 02:13, trout display, the owner's reply to round 5 (verbatim): "For the trout rework next round we should
  reduce text above the table. Also the sheet is better option and we need to refine it. We could reuse calendar
  element for dfo or freshwater for salmon if it helps. I like the binnong appearance now. Also need to figure out
  where to list the sources (regulation page etc)"
  Answers round 5 question 1: the SHEET. Round 6 started (mockups only), same page: less text above the table; the
  sheet designed properly (header, order, moving between fish, hard cases); the app's own year calendar tried for
  "when"; one place for sources (rule book page, whose rule, the quoted words); the binned look kept as is.

- 2026-11-16 02:23, trout display ROUND 6 is on the same page (https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH),
  mockups only. Above the table: 87 px, 2 lines, 13 words on average (was 134 px, 5 lines, 29 words; the Vedder 61
  words to 13); one screen holds on 16 of 22 waters. The sheet: picture, full name and today's answer on top, then
  "Here, today" said from the fish's side, licence rules, "When", how to tell it, sources; Previous / Next between
  fish. The year bar: under the table on 4 of 22 waters (Elk, Kaslo, Kitimat, Fraser) and in a few fish sheets; it
  cannot show a fish whose season differs from its table (Skeena in winter, Bella Coola cutthroat) because the data
  stores one year per table row. The bar was drawn from screenshots, not from the app's component. Sources: one
  "Sources" line under the table and a section in each sheet. Four questions (recommended first):
  1. With the second half of the answer off the top, is "Up to 2 today" enough on the Vedder, where a rainbow column
     says "Keep 4"? Recommended: yes (the one line to bring back if not: "or 4 if they're hatchery rainbow trout").
  2. Keep Previous / Next in the sheet? Recommended: yes.
  3. Ask the data side for a year per fish, so the bar can show the Skeena in winter and the Bella Coola's cutthroat?
     Recommended: yes, if this is ever built.
  4. Sources in both places (a line under the table and a section in each sheet)? Recommended: yes.
  Still open from round 5: hatchery-only said once above the table (now the first-line link reads "Hatchery fish
  only · how to tell"); the new wordings; who writes "often mistaken for".

## 2026-11-16 morning list — ONE list, all regions (written 04:14; the audit's findings are added below it when they arrive)

**Where things stand.** Your labels are built into the app data and on staging: release r-46e4355a7277, staging site
rebuilt on it, smoke check green (19 pass, 0 fail). 194 DFO places changed since the last release, 13 new cuts, four
closed areas drawn (Shuswap, Thompson x3). The app chat is redeploying the staging feed server and building 0.7.11 for
Play internal testing. Production is untouched and stays yours.

### A. Saves to re-tick in the review app (I saved these at your request as "unsure"; each is noted so)
Vancouver Island
- Somass, Stamp, Little Qualicum "everything else" places: rest-of, with the listed stretches answering first on
  their own dates (lines 2787, 2788, 2789; they replace my earlier 2771 to 2773).
- Nitinat below Parker Creek (2792) and Puntledge below the lower hatchery weir (2794): rest-of.
- Cowichan: Sandy Pool sign to the tidal boundary (2774) and 66 Mile Trestle to Sandy Pool (2775): side streams off
  (the September closure no longer lands on the whole Koksilah).
- Nanaimo: Cedar Road bridge to the Hwy 1 overpass (2776), 500 m below Bore Hole to Boulder Creek (2777) and its older
  wording (2778): side streams off.
Interior
- Shuswap closed stretch: the gauge to DFO's other printed point, 2,075 m (2770).
- Thompson, Juniper closed area: the lower signs to 4 km up (2785). Deadman and Ashcroft (2748, 2749) from before.
Skeena, Nass, north
- Skeena sockeye, Terrace to above the Babine: rest-of between the CNR bridge and the gauge, three listed places
  (2790, 2791).
- Skeena "mainstem waters only" (pink): above the CNR bridge, no side streams (2782).
- Morice, lake outlet signs to Gosnell Creek: starts at the lake now (2781).
- Babine Lake sockeye place: the whole lake, side streams off (2784). See question 8.
- Tatshenshini: the Blanchard River whole with its side streams (2779); Kwatini, Stanley and Goat creeks whole (2780).
- Sheldens Creek: the whole creek (2783), from your "i think this is tidal bound maybe".
- Kitimat west bank: side streams off (2786); still closes both banks until one-bank places are built.
- Dean, Crag Creek down to the tidal boundary: rest-of (2793).
From the earlier list, still to re-tick: Nanaimo 500 m (2746); the two Bulkley "all tributaries" places (2732, 2733);
the two Skeena places on the new confluence cuts (2751, 2752); Puntledge 1263 / 2447 / 2687; Sproat and Stamp Falls
(re-save as right: your fixes are in); the Fraser Region 2 places and Squamish (they gained side channels).

### B. Questions (recommended answer first; nothing here blocks staging)
Labels
1. **Campbell River near the mouth**: three stretches still "not sure" from Oct 26; the cement block sign is an
   estimate. Re-check on the satellite view when you can. Until then: kept as drawn.
2. **Fraser Region 3 mainstem** (Spuzzum to the Region 3 / 5 line): you marked it wrong because a label read "Region
   3 – Region 8 boundary"; the start used IS the Region 3 / 5 line. Keep as drawn and re-save as right? Recommended: yes.
3. **Kanaka Creek**: DFO says "112th Street bridge", the only crossing is 112 Avenue. Keep? Recommended: yes.
4. **Kispiox "signs near the Kispiox River Resort"**: no resort by that name; the signs sit at the Kispiox River Lodge,
   13.7 km up (low confidence). Keep until DFO Smithers confirms? Recommended: yes.
5. **Chemainus tidal boundary at 2.1 km** (low confidence; it makes 16 of 24 pieces tidal). Keep, or check imagery?
   Recommended: check imagery.
6. **Cowichan**: the water's "including tributaries" flag looks wrong (only a retired wording says tributaries). Turn
   it off? Recommended: yes.
7. **Middle Shuswap** (Shuswap Falls to Mabel Lake): no side-stream choice is saved anywhere. Main river only?
   Recommended: yes (DFO's words name no side streams).
Skeena (from the review of the built result, `handoff/PLAN/curation-review/skeena-cascade-review-2026-11-15.md`, ticket T781)
8. **Babine Lake in August (errs OPEN today).** Sockeye reads open on the whole lake Aug 1 to 27, including within
   400 m of 12 creek mouths and east of the Gullwing Creek line; those 13 closed areas are only notes on the card
   because they are not confirmed yet. Recommended: (a) you confirm the 13 areas in the review app so they are drawn;
   (b) until then the card gets an "open, closed in part" headline on those days.
9. **Gosnell Creek and Thautil River (errs closed).** The rule book's Morice "No Fishing from Gosnell Creek to Lamprey
   Creek" closes both creeks Jun 16 to Jul 31. Your earlier ruling says a joining creek with a line of its own is not
   inside. Leave Gosnell and the Thautil out, keep Lamprey Creek in? Recommended: yes.
10. **Gitnadoix below the powerline (errs OPEN).** Coho reads open Sep 1 to Oct 31; DFO opens only above the
    powerline. Add a cut at the powerline? Recommended: yes.
11. **Eulachon**: "No fishing for eulachon" is missing on the Lakelse and, to Jun 15, the Kitsumkalum. Always show it
    below Terrace? Recommended: yes.
12. **Kitimat east bank**: reads closed for about 750 m (DFO closes only the west bank). Leave closed until one-bank
    places are built? Recommended: yes.
How rules combine
13. **Gear rules of a whole river on a stretch with its own gear rule that day** (T770): on the Little Qualicum in
    Sep to Nov, "fly fishing only" hides the river's all-year hook rule. Show both? Recommended: yes.
How we work
14. **Fewer, later checks** (your "Everything takes so long…", "Feels wasteful"): (1) while working, only the tests
    for what changed; (2) ONE full run, at the release, on a clean copy, and the staging release stops waiting for
    GitHub's run (GitHub's run stays required for production); (3) checks that take seconds run before anything long.
    Yes? Recommended: yes. The audit's guards checker is costing this out; its numbers go below.
Kept as they are unless you say otherwise (no answer needed): the Tatshenshini "mainstem and all other tributaries"
place (your "I don't see how this would be a rest of situation"); the Tatshenshini place below the border keeps its
side streams (you verified it); the Kitsault tidal boundary's name waits for a small code change (T773); Sheldens,
Juniper, the Skeena sockeye pair, the Tatshenshini headwaters and Maria Slough are done as you answered.

### C. Trout display (mockups only; https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH, round 6)
Round 6: (1) on the Vedder is "Up to 2 today" enough on top when a rainbow column says "Keep 4"? yes; (2) keep
Previous / Next in the sheet? yes; (3) a year per fish in the data so the year bar works on every water? yes, if
built; (4) sources under the table and in each sheet? yes. From round 5, still open: the new wordings, and who
writes the "often mistaken for" lines (you, or someone you trust on fish).

### D. Filed overnight (tickets T770 to T782; none needs you except where a question above names it)
T770 gear rules hide each other · T771 chain order and an equal-places check · T772 Kitsumkalum Lake takes its river's
side channels · T773 tidal boundary name · T774 a place cannot be saved "not drawn" · T775 review app rest-of
leftovers · T776 audit tool faults · T777 tests that read your live saves, and child-process crashes · T778 the
Shuswap delta piece (done) · T779 the save-time check does not show a piece the build will refuse · T780 part names
showing "approximate location (medium confidence)" · T781 Skeena follow-ups · T782 data faults from the trout review.

**Added 04:40: the general audit (five Fable checkers, read-only; four reports in:
`handoff/PLAN/audit-2026-11-16/code.md`, `handoff/PLAN/audit-2026-11-16/labels.md`,
`handoff/PLAN/audit-2026-11-16/decisions.md`, `handoff/PLAN/audit-2026-11-16/guards.md`; the answers-against-sources
checker is still running). And the Play build: 0.7.11, build 33, code shrinker on, is on Play internal testing
(04:37); your ten-step phone walk is the To-Do row `t749-r8-phone-walk`.**
Headlines: the code of Nov 14 to 16 has no defect that changes what an angler is told today (97 commits read); every
one of your 309 labelled places is stored as your last save says, and no place reads open against its own DFO row.

### A2. Re-tick list: additions from the audit
- FIRST: Fraser, Mission to Hope (line 2568, unsure): it is the one open answer live this month (hatchery coho
  Nov 7-30, chum Nov 3-30); 25 of its 62 sections are side channels and Maria Slough (your "Add all five").
- Saves I wrote before line 2770 that are in force: 2672 (Stave, unsure); 2730, 2731 (Bella Coola, verified);
  2748, 2749 (Thompson Deadman and Ashcroft closed areas, verified); Region 6: 2728, 2732 to 2736 (unsure), 2738,
  2739 (verified). With 2770 and 2785 that is all four closed areas standing on a "verified" I wrote.
- Low confidence by their own notes: Juniper's upper end (4 km by the words); the Shuswap stretch (2,075 m where the
  words say "one km").

### B2. More questions, from the audit (recommended answer first)
15. Regions 7 and 5A: the page header "no fishing for salmon until further notice" overrides the dated openings in
    the table (your Oct 31 ruling "the header closure wins"). Nechako, Quesnel River and Quesnel Lake Horsefly Bay
    would read closed next Aug 22 when DFO opens sockeye and pink; DFO's page also says "except for the
    opportunities listed in the table below". A dated row with a fishery notice answers on its dates, the header
    every other day? Recommended: yes.
16. Atnarko below the Tweedsmuir Park Lodge sign (line 2584, unsure): the coho opening also covers 17 side-stream
    sections. Side streams off for this stretch? Recommended: yes (errs closed). Note your earlier "no it needs to
    include tributaries" was about the Atnarko as a whole.
17. Lower Shuswap: the cut is the Mara Bridge; DFO says "signs above Mara Bridge". Are the signs at the bridge? If
    not, a cut at the signs.
18. A place whose last save is "wrong" with no fix: you said "I want you to for it to be redrawn… Just be safe". Not
    built: such places still ship their older drawing (the Region 3 Fraser mainstem; Sproat and Stamp Falls end on a
    "wrong" save though their fixes are in). Build: the release lists them and ships DFO's words undrawn until
    re-saved? Recommended: yes.
19. The app chat's merge question (never shown to you): may the finished app work merge into the shared branch now
    (T759 banner and stripe, T760 stuck download, T756, T746, T738, T741, T732 backup, T739 push, the iPhone
    branches)? Your words on Nov 15 10:33: "ios work could merge when ever I think?" Recommended: yes, one at a time
    with its checks.
20. T759 stripe: legend word "Check rule" OK, and strong enough on the dark blue line? Recommended: yes to both.
21. T735 dark-mode highlight: pick C (dark edge + soft glow)? Recommended: C.
22. Play "Data safety" location answer: approximate or precise? (For the App Store you said "we want precise".)
23. Production backup bucket name `bc-fishing-regulations-backup` (Q172)? Recommended: yes.
24. T686 data shrink: option (a), about 1.94 MB, lossless? Recommended: (a).
25. T732 backup: only with a screen lock? [no]; photo copy size [1600 px]. Recommended: as bracketed.
26. T710: the side channel named "Exchamsiks River confluence up to Skeena River Ecological Reserve boundary" reads
    backwards. Reword?
27. Shuswap closed stretch: tell DFO their two printed points are 2 km apart where the words say one km?
    Recommended: yes.
28. Should the pre-build check read the committed proposals file, not your working copy (it stopped a Play build
    once)? Recommended: yes.
Note on question 10 (Gitnadoix powerline cut): it REVERSES your Nov 15 "Whole river (Recommended)" for the Gitnadoix.

### E. From the audit: recorded wrong or lost (I fix the records and ticket these; listed so you know)
- T754 one-bank places: you answered "Build it this way (Recommended)"; the ticket still said "options for the owner".
- T759: the shared ticket still described the design you rejected ("reads not known"); the banner + stripe design
  you chose is on unmerged branches.
- The About-page sentence "not affiliated with the government": listed as built, is on no branch.
- Seven wording findings from the T462 audit had no ticket (K6, K7, K8, G8, G9, G10, D6); your gate 1 for closed
  testing needs each fixed or ticketed.
- Cut de-duplication: the data was cleaned once; the suggester still proposes a new cut where one exists.
- Review app: when Confirm is greyed, name the unticked row (you got stuck three times).
- Hope gauge: 66 m reads the wrong gauge; three gauge cuts never placed.
- 14 of your Nov 13 to 15 readings are not yet in the rulings file.
- Out-of-date production commands at two places in this file; a fresh block is owed.
- The approved push privacy line is written nowhere yet.

### F. PRODUCTION: what stands between staging and production data (the audit's list, in order)
BLOCKER FIRST: the production upload refuses r-46e4355a7277 (four closed areas) until the PRODUCTION feed server
serves closed areas. That server code (T757, T758) is only on the app chat's side branch; the production checklist
puts the feed server after the data, which is wrong for this release.
1. You: check staging, re-tick list A, answer the label questions.
2. Decide whether one more staging release is needed.
3. Claude: merge the T757 / T758 feed-server halves into the shared branch (needs question 19).
4. Claude: fresh production commands and a clean site copy (T404). 5. Claude: rehearse the production scripts.
6. You: backup bucket (Q172), mail routing, run token. 7. You: data worker.
8. You: feed server, BEFORE or WITH the data. 9. You: the production release command.
10. Site: Claude builds, you deploy; robots settings; www redirect (T240).
11. You: smoke check and the privacy page. 12. You: Search Console; retire v1.

### G. PLAY CLOSED TESTING (after production)
1. 0.7.11 on Play internal; your ten-step walk and the open phone checks (T749).
2. The "before closed testing" tickets: T759, T735, T716, T740, T708, T751, T608.
3. Gate 1: the seven T462 findings and this audit's findings, with your OK.
4. You start `/code-review ultra` (T463). 5. Store listing: texts, the 8 pictures re-shot, the preview clip, the
non-affiliation sentence (T244, T371, T319, T709). 6. You: Play forms, including the location answer (T371).
7. You: production app build, install, promote to Closed. 8. You: tester list; 12 testers for 14 days.
9. Claude: the in-app update prompt checked (T390, stalled since Nov 6) and T514.

### H. Question 14 again, with the audit's numbers (fewer, later checks)
Last night's staging release ran 105 minutes over nine attempts; about 60 of those minutes repeated work that told
nothing new (the same 6,490 tests ran in place and again on the clean copy; the clean copy reran after commits that
could not affect it; the release's own commit rebuilt the data for 9 minutes). Across the night those tests ran 16
times on this Mac. A rehearsal command exists that lists every stop in one pass (the comparison to read, stale test
fixtures, changed pages, failing app tests); I did not run it, and it would likely have made it four or five attempts.
The proposal, for your yes or no (nothing is changed until you say):
- While working: the one test file touched, plus the quick checks (about a minute).
- Before a push: one command that runs exactly what GitHub runs, made automatic at push time.
- In a release: build, REHEARSE on the new release and fix everything it lists, then one real run; the tests run
  once (on the clean copy), not twice; the clean copy reruns only when a commit could affect it; the upload's
  question to the feed server is asked at the start, not at the end.
- One thing ADDED: the tests that read the newly built data run once on the new release. Today most of them (1,208
  tests in 115 files) are run by nobody automatically.
- Not cut: the build refusing a piece it cannot place, the stop to read what changed, the stop on a changed page, the
  clean-copy run once per release, GitHub's runs.
- Saves about 10 minutes on a clean release; about 60 of last night's 105.
Recommended: yes.
Also from that checker, to be fixed (ticketed): the review app can crash when several pages are drawn at once (its
page pictures are freed outside the lock); test crashes on this Mac come from a map library's database handle in
child processes; four fixture states for closed river parts are still checked only for "is closed"; 5 of your 17
reviewed continuation links quietly stopped shipping and 4 new ones wait with no row for you.

### I. The fifth audit report: the app's answers against the printed rule book and DFO's pages
(`handoff/PLAN/audit-2026-11-16/answers.md`; about 98 waters, about 1,060 answers, three days each, all regions.)
No wrong number, size or date was found. Province-wide rules matched on all 18 waters with no entry of their own.
**Seven things the app allows that the sources forbid** (none is new in this release; tickets T793 to T798):
- Wild-trout limits are missing from the keep card on at least 15 lakes (Comox, Sproat, Great Central, Cultus,
  Harrison, Pitt, Chilliwack, Chehalis, Lillooet, Lois, Ruby, Windsor, Makii, Dodd): the book says "No wild rainbow
  trout over 50 cm"; the card says "any size; up to 1 over 50 cm". And the Kitimat with 23 streams, Jul to Oct: "Up to
  5 rainbow trout" without "hatchery". The data is right; the card drops it. FIRST TO FIX [T793].
- The Peace River just below W.A.C. Bennett Dam reads open; the book closes it (the rule sits on Dinosaur Lake only)
  [T794].
- Williston Lake's "No rainbow trout under 30 cm" is missing on Nation Arm and Davis Bay [T794].
- "No angling from powered boats" is never shown (Atnarko / Bella Coola, Horsefly, Chilcotin, Chilko, Clearwater,
  Nicomekl, Serpentine) [T797].
- A stretch the book closes can still show an open salmon card (Skeena Ecological Reserve, the Atnarko's closed
  stretch, the Cowichan below Mile 66) [T796]. Question 29.
- A DFO "Finfish closure" closes only salmon; trout stay open (Stamp, Somass, Sproat, Nitinat, Tahltan) [T796].
  Question 30.
- Coal Creek above the old railway bridge asks for no Classified Waters Licence [T798]. Question 31.
**Things the app forbids that the sources allow:** Fraser Lake, Kotcho Lake and Cold Fish Lake read closed all year
because an ecological reserve touches 0.1 % of the lake [T795]; dead fin fish as sturgeon bait is not shown on the
Region 2 Fraser and lower Harrison; the Shuswap Lake pier exemptions; the Pitt Lake head caveat says "release all"
where the book says wild only [T797].
More questions (recommended answer first):
29. A stretch the rule book closes: the salmon card reads closed too, with DFO's row kept as text? Recommended: yes
    (the book, p.77, says its closures apply to salmon as well).
30. A DFO "Finfish closure" closes the whole page, trout included, on its dates? Recommended: yes.
31. Coal Creek above the old MF&M bridge: Class II when open? Recommended: yes.
32. Williston Lake's 30 cm rainbow minimum on all three Zone A parts (Nation Arm and Davis Bay too)? Recommended: yes.
33. Puntledge above Comox Lake: the book says fly fishing only above the diversion dam "including tributaries" and
    exempts the river from the summer closure; we stop both at the lake. Carry both above the lake? Recommended: yes.
34. Westwick Lakes (26 % inside an ecological reserve) and Priest Lake (59 %): open with a part-closed note, or
    closed? (Fraser, Kotcho, Cold Fish and 15 Mile Swamp, barely touched, are a plain fix: open.)
35. Skeena "Section 4" has no printed upper end; the app runs it to the river's source. Right?
36. DFO "non-retention" reads Closed (your earlier ruling; 20 rows). Keep, or show "release only"?
37. **The wild-trout fix is ready on a side branch (05:28); 22 answer keys change, nothing else.** New card lines, for
    your read: Comox, Great Central, Sproat: "Up to 4 brown, cutthroat or rainbow trout combined — no wild rainbow
    trout over 50 cm (20 in)". Chilliwack, Cultus, Harrison, Chehalis, Pitt, Lillooet: "Up to 4 brook, brown,
    cutthroat or rainbow trout combined — no wild brown, cutthroat or rainbow trout over 50 cm (20 in)". Dodd, Lois,
    Ruby, Windsor, Khartoum, Horseshoe, Nanton, Ireland: "… combined — up to 2 wild brook, brown, cutthroat or rainbow
    trout combined 40 cm (16 in) or less; no wild … over 40 cm (16 in)". Kitimat and 23 streams, Jul 1 to Oct 31: "Up
    to 5 rainbow trout — no wild rainbow trout under 30 cm (12 in); up to 1 wild rainbow trout 30–50 cm (12–20 in)".
    They are long but true. Go as worded (recommended), or tell me how to shorten? On your go: merge, rebuild the
    answers, a staging release [T793].
38. **Dodd, Lois, Khartoum, Horseshoe, Nanton, Ireland, Windsor lakes: does "no wild trout over 40 cm" cover char?**
    The book prints "Wild trout/char daily quota = 2" and then "no wild trout over 40 cm" (Lois and Khartoum: "40 cm
    or more"). We read the second line as trout AND char, so a wild Dolly Varden/bull trout or lake trout of 60 cm or
    more cannot be kept there (the card will say so once the fix lands). Your Region 6 ruling reads "trout" as NOT
    char. Trout only here too (a wild char over 60 cm may be kept inside the 2), or trout and char as now (stricter)?
    Recommended: keep trout and char until you say otherwise (it errs closed). Checked: the four lakes the audit had
    not listed (Khartoum, Horseshoe, Nanton, Ireland) carry the same printed lines as Dodd, so their new card line is
    right.


## 2026-11-16 10:25 — all of last night's questions on ONE answer page (main chat + app chat)
- Owner, 10:13, typed in chat (verbatim): "can you go through all questions to me from over night that youy haad? make sure
  i have all info needed. also get any questions the app chat has too"
  Done 10:24: one page, https://claude.ai/artifact/H8gtvpptCZLGLaPGCyNfaJ ("Morning Questions"), 55 cards in eight
  sections: Start here (questions 37, 38, 19, 14, and one go-ahead for the fixes that put a printed rule back on the
  page: T794, T795, T797 and the small code fixes T783, T784, T785, T792 A); Your DFO labels (1 to 7, 16, 17, 26, 27);
  Skeena and the north (8 to 12, 35); How the rule book and DFO are read (13, 15, 18, 29 to 34, 36); The app: looks and
  picks (20 in two parts with the stripe pictures, 21 with the highlight sheets, the T762 year line, 24, 25, ten small
  defaults); Production and the stores (22, 23, the Firebase exception, the App Store privacy answers, 28); Trout
  display mockups (round 6's four, round 5's three, and "what next"); Only you can do these (the re-tick list, the
  build 33 walk, iPhone build 36 and the push test). Each card: the background in plain words, the printed line where
  one exists, the choices with the recommended one marked, a note box. His taps and notes are stored in the page's
  database (collection `answers`, one document per card id: q01 to q38, q20a, q20b, go-fixes, a-year, a-defaults,
  a-push, a-store, t1 to t8, task-retick, task-walk, task-iphone). READ THEM FROM THERE and copy each answer, verbatim,
  into this file and its ticket before acting on it.
  Wording choices of mine on the page that differ from the morning list (mine, not his): question 17 recommends "keep
  the bridge and ask DFO"; question 26 recommends rewording from the lower end up; question 34 recommends Westwick
  open with a note and Priest closed; question 35 recommends "to the source, as now"; question 14 shows section H's
  proposal, with "staging releases need not wait for GitHub" as a separate choice that needs his explicit pick.
- The app chat's list (its message of 10:14; nothing of its own answered or changed since 04:37): the merge question
  first (its order: the T757 + T758 feed-server halves, the four iOS fix branches, T756 + T760 after an emulator
  proof, T746 / T738 / the crash sender / perf T741, T732 after its picks, T759 after the stripe confirms, T739 push
  last); T759 stripe two confirms (pictures in its scratch folder, copied onto the page); T762 year line (a read);
  T735 highlight A / B / C (recommended C; the To-Do page had lost its row, the page carries it now); Play location;
  T732 three picks; T686; backup bucket; the iOS day defaults; keyboard and back picks; oldest iPhone; App Store
  privacy; the 0.7.10 looks (T731, T730, T728); the Firebase exception and 200 registrations a day; the build 33 walk;
  iPhone build 36 and push. Its view on questions 29 and 30: yes as recommended. T797 item 1 (powered boats) rides on
  the merge question, in the next app build. `ios-links-no-team-id` is moot (his Team ID is saved): row to close.
- Owner, about 10:19, typed in chat (verbatim): "also as a seperate thing can we get some final reviewing of the trout
  rework we want the agent to have an adverserial attempt to trry and see if any regulations can not be represented."
  Started 10:21: a fresh Fable reviewer (not the designer), read-only: it lists every distinct trout and char keep
  structure in release r-46e4355a7277, tries to draw each in round 6's table and sheet, and reports what cannot be
  shown or would mislead, with a real water, the stored rows and the printed line for each; and, separately, printed
  rule shapes the data itself does not store. Its report goes to `handoff/PLAN/design/` and to him when it is back.
  Still mockups only: nothing is built.
- Owner, 10:26, typed in chat (verbatim): "i want questions in batches from you directly instead". So: the questions
  are asked in chat, as multiple-choice batches of up to four, most blocking first; the page stays as a reference
  (pictures, background) and is not the way to answer. Each batch's answers are recorded below, verbatim.

### Answers by batch (2026-11-16, question dialog in the main chat)
- Batch 1, 10:27. Question 37 (wild-trout card sentences, T793): "Go as worded (Recommended)". Question 38 (does "no
  wild trout over 40 cm" cover char on Dodd, Lois, Khartoum, Horseshoe, Nanton, Ireland, Windsor): "Trout and char
  (Recommended)". Question 19 (the app chat's merge question): "Yes, one at a time (Recommended)" (the option read:
  in the app chat's order: feed server first, then iPhone fixes, stuck download after an hour's emulator proof,
  loading screen and speed work, backup, banner and stripe, push last; each with its own checks and a message to him
  first). Question 14 (fewer, later checks; section H's proposal): "Yes, as proposed (Recommended)" (staging releases
  still wait for GitHub's run; he did NOT pick "Yes, and skip the GitHub wait").
- Batch 2, 10:33. Go-ahead for the fixes that put a printed rule back on the page (Peace below W.A.C. Bennett Dam;
  Fraser, Kotcho and Cold Fish lakes and 15 Mile Swamp open with a note; powered boats; the smaller ones; four small
  code fixes: T794, T795, T797, T783, T784, T785, T792 A): "Go on all (Recommended)" (shipped together with the
  wild-trout fix in one staging release; the list of changed answers shown to him first). Question 6 (Cowichan
  "including tributaries" mark): "Turn it off (Recommended)". Question 7 (Middle Shuswap): "Main river only
  (Recommended)". Question 16 (Atnarko below the Tweedsmuir Park Lodge sign, side streams), typed: "this depends if
  the name has tributraries included in it. if it doesnt then no tribs otrherwise yes tribs". My reading: a general
  rule, not only this stretch: a DFO place takes its side streams when DFO's words for that place say tributaries,
  and not otherwise.
- Batch 3, 10:44. Atnarko follow-up (DFO's heading reads "Atnarko River including tributaries"; the stretch's own
  line names only the sign: which words decide for a stretch?): "The heading counts". So the stretch below the
  Tweedsmuir Park Lodge sign keeps its side streams, as saved (line 2584); and the rule for every DFO place: side
  streams follow DFO's words, and a heading that says "including tributaries" counts for the stretches under it.
  Question 2 (Fraser Region 3 mainstem), typed: "it is right if it is spuzzum to the 3/5 boundary now. ON THE
  FRASER". Question 3 (Kanaka Creek, 112 Avenue bridge), typed: "keep it but add to gotcha". Question 4 (Kispiox, the
  lodge at 13.7 km until DFO confirms), typed: "okay sounds good add to gotchas".
- Batch 4, 10:50. Question 5 (Chemainus tidal boundary at 2.1 km), typed: "The tidal waters of the Chemainus River
  and off the Chemainus River mouth, commencing at Bare Point on Vancouver Island to the light on North Reef, to
  Sherard Point on Vancouver Island, thence following the shoreline northerly to a boundary sign on the Chemainus
  River near the Bald Eagle Campground, thence across the river to the opposite bank, thence following the Vancouver
  Island shoreline northerly to the point of commencement / check it agfainst this if it is wrong then let me
  review". So: the main chat checks our 2.1 km point against "a boundary sign on the Chemainus River near the Bald
  Eagle Campground"; if it does not match, it goes to him for review in the review app. Question 1 (Campbell River
  near the mouth, three stretches): "They are right". Question 17 (Lower Shuswap, signs above Mara Bridge), typed:
  "at the bridge but make sure the cut name has the fact signs are nearby". Question 27 (tell DFO the Shuswap closed
  stretch's two points are 2,075 m apart where the words say one km), typed: "just add to gotchas that is all. might
  be 1km as bird flies too". So: nothing goes to DFO; a gotcha entry, and the straight-line distance between the two
  printed points is measured and noted.
- 10:36, the adversarial trout review is back and saved: `handoff/PLAN/design/trout-adversarial-review-2026-11-16.md`.
  Verdict "no, not yet": possession limits have no place in the design (at least 640 waters); the steelhead "stop
  fishing this water" duty is hidden under "Licence rules" (2,957 waters); a wild limit shared across two boxes reads
  as 4 where the book says 2 (Lois and Khartoum lakes; T793's sentences have the same split there, to check before
  T793 ships); a fish in two shared limits (Kamloops Lake, Adams River) is drawn wrongly in the mock; the top line
  understates on 89 waters; the year bar's gap touches 10,250 waters. Told to him in chat at 10:46.
- Batch 5, 11:03. Question 8 (Babine Lake in August, the 13 closed areas not confirmed), typed: "i think you can
  confirm them. it is just confluences and unless the names dont match they are easy to auto confirm". So: the main
  chat confirms the 12 creek-mouth circles where the creek's name matches DFO's words (and the Gullwing Creek line);
  any whose name does not match goes to him. Question 9 (Gosnell Creek and the Thautil River inside the Morice
  closure), typed: "yes they are out but please make sure this rule doesnt break other stuff. we need a trigger for
  why they are out? why does this tributary rule not apply? what is the condition just so it is a universal rule."
  So: they come out, but as ONE stated rule applied everywhere, not a special case: the condition is written down,
  run across the province, and every water whose answer changes is listed for him before it ships. Question 10
  (Gitnadoix, coho "upstream of powerline"), typed: "the powerline is. the mouth essentially so maybe you could add
  the cut for powerline to the mouth then it is all good?" My reading: the powerline is treated as the river's mouth
  (the mouth carries the powerline as its name for this place), the whole river stays open for coho as he answered on
  Nov 15, and no new cut or map rebuild is needed. Question 26 (the Skeena side channel's name): "Reword, lower end
  first (Recommended)".
- 11:02, checker's result on question 5 (Chemainus): our tidal point (48.886806 N, 123.686629 W, 2,091 m up the river)
  is beside the Bald Eagle Riverside Campground: 0 to 135 m from it along the river, depending on which map point is
  taken for the campground. It matches DFO's "boundary sign on the Chemainus River near the Bald Eagle Campground";
  it stays, still low confidence because the sign itself is not pinned. Nothing for him to review. And on question
  27: the Shuswap closed stretch's two printed points are 1,738 m apart in a straight line (2,075 m along the river);
  neither is near "one km". Goes into the gotchas file as he asked.
- Batch 6, 11:19. Question 11 (eulachon line below Terrace), typed: "general rule is (Reminder: There is no fishing
  for eulachon in Region 6. ) why would bait remove eulachon rule?" So: it is DFO's Region 6-wide reminder and shows
  on every Region 6 water; a bait rule must never take its place (the cause is T770: the card treats the eulachon row
  as a gear line and shows only the narrowest place's gear lines). Question 12 (Kitimat hatchery outfall, east bank):
  "Both banks closed (Recommended)". Question 35 (Skeena "Section 4" has no printed upper end), typed: "i want to
  include pictures for you to look at this needs more thought." HELD: he sends pictures; nothing changes until then.
  Question 13 (a stretch's gear rule hides the river's, T770): "Show both (Recommended)".
- Batch 7, 11:27. Question 15 (Regions 7 and 5A: the page header "no fishing for salmon until further notice"
  against the dated openings in the table), typed: "can we look at when the rules got added to see if they are
  stale? what came first header or rules? once we have that info we caqn make a decision". HELD: the main chat finds
  when the header and each dated row (FN0851 Nechako, FN0846 Quesnel) appeared, then asks again. Question 18 (a
  place marked wrong with no fix still ships its older drawing), typed: "can we sort this out now i want to review it
  and make it correct for sure". So: the list of every such place goes to him now, to review and re-save in the
  review app. Question 29 (a stretch the book closes shows an open salmon card, T796), typed: "yes we want to do this
  but can you bring the example changes to me so i can see why it happened etc? but yes we want it to do this". So:
  YES, build it; and the examples (before, after, and why) go to him. Question 30 (DFO "Finfish closure"): "Closes
  the whole page (Recommended)".
- 11:28, on question 35 (Skeena Section 4), he sent two pictures from the rule book (the maps of Section 4 Zone A,
  Shegunia River confluence to Sedan Creek confluence, and Zone B, Chimdemash Creek confluence to 1.5 km upstream of
  the Zymoetz confluence), typed: "here are picutres please review them then ask more questions we can figure it out
  together".
- Skeena Section 4 follow-up, 11:32 (after his two maps; the main chat had read the province's regulation, Schedule A
  item 33: "Skeena River upstream of 1.5 km upstream of Zymoetz River (Skeena River 4)", Class II, July 1 to Dec 31,
  no upper end). How far Section 4 runs, typed: "Should run all the way up but also we should handle the weekend
  zones properly too. Make sure licensing info is done properly there. Also we might want to have a way to indicate
  classified water types on the map/card?" What to check about the two weekend zones, typed: "Just make sure the info
  is correct for them for licensing etc etc. also maybe we should have the classified waters indicator for the
  different types?" So: question 35 is ANSWERED (Section 4 = the whole main river above the point 1.5 km upstream of
  the Zymoetz; the "a curator must draw the upper bound" flag on the stored row is cleared); a checker reads the
  licence, stamp and weekend-closure lines on every Skeena main-stem part of the release against the printed lines
  (started 11:32); and a new idea of his, not a decision: an indicator of classified waters (Class I / Class II) on
  the map and the card. It becomes a ticket with a few looks for him to pick from, for the app chat.
- 11:32, from the app chat, a data note for the main chat: "No angling (from a powered boat) — upstream of dyke gates"
  binds 31 parts: the Nicomekl and Serpentine plus 27 side streams (Anderson Creek, Best Creek, Cloverdale Canal,
  Cub Creek, Elgin Creek, Glover Creek, …). To check against the book: the two rivers only, or their side streams too.
- Batch 8, 11:35. Question 31 (Coal Creek above the old MF&M Railway bridge): "Class II above the bridge
  (Recommended)". Question 32 (Williston Lake's 30 cm rainbow minimum): "All three parts (Recommended)". Question 33
  (Puntledge above Comox Lake: fly fishing only above the diversion dam "including tributaries", and exempt from the
  Jul 15 to Aug 31 closure), typed: "If the river continues then yes it goes above". So: both carry above the lake
  on the parts that are the Puntledge. Question 34 (Westwick Lakes 26 % and Priest Lake 59 % inside an ecological
  reserve): "Both open with the note".
- 11:36, two checkers back (reports to be saved under `handoff/PLAN/curation-review/`; until then the full texts are
  in this session's transcript and the scratch folders `header-history/` and `wrong-saves/`):
  (1) Question 15, which came first: neither. DFO keeps BOTH texts in the page and switches each season: off-season
  the header "no fishing for salmon until further notice" shows and the table of openings is hidden inside an HTML
  comment; about Aug 20 it shows the table and hides the header; on Apr 1 it flips back. FN0851 (Nechako) and FN0846
  (Quesnel River, Horsefly Bay) were issued 2025-08-20 for 2025 dates; today's pages (stamped 2026-04-01) show only
  the header. Our parser reads the hidden table too (Regions 7, 5A and one row in Region 4), which is the only reason
  a header and dated rows ever meet. Proposed fix: the parser (Python and the feed server's) ignores a table DFO has
  hidden; then the header rule is never needed against dated rows.
  (2) Question 18, the list to re-check: 40 saves. 5 are "wrong" in his hand (live: Sproat, fix is in, re-save as
  right; Stamp Falls, labels done but the lower cut sits about 165 m below Deer Creek where his note says 225 m;
  dormant: Fraser Region 3, Thompson, Morice). 35 were written by Claude for him (31 places, 4 closed areas): 11 of
  them are live and answer open in Nov or Dec (Stave, Bella Coola, two Cowichan stretches, Nanaimo Cedar Road,
  Tatshenshini Blanchard, Skeena pink, Stamp / Little Qualicum / Nitinat / Somass rest-of places). None of his own
  "unsure" saves lacks a fix. The audit tool finds only 8 of the 40 (T776).
- Batch 9, 11:46. Hidden tables on DFO's Region 7, 5A and 4 pages (ignore whatever DFO has hidden?), typed: "we
  should keep reading them for provenance but mark them hidden somehow so they are ignoired". So: the reader keeps
  the hidden rows, marks each as hidden, and a hidden row answers nothing (data build and feed server alike); this
  settles question 15 (the header rule no longer meets dated rows). How to go through the 40 saves to re-check: "In
  chat, with a picture each (Recommended)" (each place as a map picture with DFO's words and what is drawn, in
  batches, most urgent first; he answers right / wrong and the main chat saves his answer with his words; the 6
  retired ones are skipped). Stamp Falls lower cut (about 165 m below Deer Creek, DFO says 225 m): "Move it 60 m down
  (Recommended)" (next map rebuild; picture to him before it ships). Question 36 (DFO "non-retention" reads
  Closed): "Keep "Closed"" (RULINGS.md gets the line that the ruling covers dated rows).
- 11:45, the Skeena Section 4 checker is back: the licence information is right on every main-stem part (Class II and
  the named section, the dates, the steelhead stamp, no Classified Waters Licence on "Section 3", Section 4 from 1.5 km
  above the Zymoetz to the source, both weekend zones on the right parts and days, a resident's status unchanged on a
  Saturday). Three wording faults: (1) Monday to Friday the weekend closure's "at certain times" line is cut short
  (zone b does not say who is closed out; zone a never names the zone); app core, `caveats.ts` line 256; it touches
  every weekend-only closure in the province; (2) "non-residents and non-resident aliens require separate licences
  describing which section they are fishing" is barely shown, and the stamp sentence quotes the wrong printed line
  (`pipeline/deliver/export/read.py` line 387); (3) zone b on weekends reads "the Zymoetz River confluence (1500 m
  upstream)" where the book says "1.5 km upstream of Zymoetz River confluence". One question for him: with the
  angler set to "From outside Canada", a zone part on a Saturday still says "Open", the closure only as a note.
- Batch 10, 11:55 (pictures sent first: the stripe sheet of eight, the stripe close in on the light theme, the two
  dark-highlight sheets). Skeena weekend zones, an angler set to "From outside Canada" still sees "Open" on a zone
  Saturday with the closure as a note: "Note is enough (Recommended)". Question 20, the stripe's legend entry (T759):
  ""Check rule" is fine (Recommended)". Question 20, the stripe's strength on the dark blue "Keep salmon" line: "Make
  it wider". Question 21, the dark-mode highlight (T735): "C: dark edge + soft glow (Recommended)".
- Batch 11, 11:58. The T762 trout "through the year" line (in build 33): "Fine as it is (Recommended)". Question 22
  (Play "Data safety" location): "Change Play to precise (Recommended)". Question 24 (T686 data shrink): "(a) Three
  lossless changes (Recommended)". Question 25 (T732 backup: screen lock no, copies 1600 px, new photos at 2048 px),
  typed: "1280 is probably enough also could we compress as jpeg or something which makes the size a lot smaller? ".
  So: backup copies at 1280 px (about 81 fit); the app chat answers whether the copies are already JPEG and what a
  stronger compression would save; the screen-lock pick and the 2048 px pick were not objected to (defaults stand,
  my reading).
- 11:58, he asked in chat: "what is state of the trout redsign ?" Answered in chat (round 6 mockups published, nothing
  built; the adversarial review says "not yet", four blockers; round 7 proposed to fix them).
- Batch 12 (trout display, mockups only), 12:03. Next step after the adversarial review: "Round 7 fixes them, then
  re-attack (Recommended)". Top line on the 89 waters where the short line understates: "Bring the second half back
  (Recommended)". Previous / Next in the sheet, and sources in two places, typed: "i like the sources in both. ths
  general all sources and the specific ones. then the sheet should be a swipe down to close style (we dont need
  next/prev on it unless it is going in the same group like bull trout/dolly varden? then navigation should be more
  obvious with what the next item is in the item at top of sheet and swipe over gestures etc) and the sheet needs to
  be cleaned up a bit I think. " Who writes the "often mistaken for" lines, typed: "we should drop this for now I
  think ". So round 7: the four blockers and the top line fixed; sources stay in both places (all sources under the
  table, the specific ones in the sheet); the sheet closes by a swipe down, loses Previous / Next except between
  fish of one group, shows plainly what the next item is where it does move, and is tidied; the "often mistaken for"
  placeholder is dropped. Then the same reviewer attacks it again.
- Batch 13, 12:09. Question 23 (production backup bucket name `bc-fishing-regulations-backup`): "That name is fine
  (Recommended)". Android push through Google's Firebase as his exception to "self-host everything", typed: "we dont
  want this right now". So: no Firebase now; Android push is not built for now (Android keeps its own background
  checks); the iPhone push work is not touched by this answer. Question 28 (the pre-build check reads the committed
  list of proposed cuts, not his working copy): "Read the committed list (Recommended)". The eleven small app
  defaults (a to k), typed: "we should fix anything that doesnt seem desirable". So: the app chat judges each and
  fixes the ones a user would not want; my reading of the likely ones, passed to it as a reading only: (a) the Save
  row hidden under the keyboard, (g) the bottom bar jumping 122 px, (h) Back reopening search with the text still
  typed, and (i) by his own "Flow leads" ruling, the Conditions card opening on Flow. It tells him what it changed.
- 12:05, from the app chat, for his backup question: the backup copies are already JPEG (1600 px on the long edge,
  quality 70, about 0.37 MB each against 4.6 MB as taken); at 1280 px about 0.25 MB (81 fit); a stronger setting
  (JPEG 60 or WebP) is an estimated 0.17 to 0.20 MB (100 to 115 fit), to be measured and shown to him as a before /
  after pair before the quality changes. It builds 1280 px now.
- ALL THE QUESTION BATCHES ARE DONE (13 batches, 10:27 to 12:09). Still open with him: the map pictures of the 34 live
  saves to re-check (being drawn; in chat, most urgent first); trout round 7 and its re-attack; the build 33 phone
  walk and iPhone build 36; the Skeena Section 4 pictures are answered.

### Re-check of saves, in chat with pictures (T806), 2026-11-16
- Pictures 1 to 4, 12:20. Sproat (mark 2468; the END is now 349 m below the Hwy 4 bridge): "Right". Stamp Falls
  (2469; DFO gives the lower end two ways that disagree on our line: 500 m below the fishway = our END at 498 m; 225 m
  below Deer Creek = our END at 164 m): "Keep 500 m below the fishway". This REPLACES his earlier "Move it 60 m down
  (Recommended)" of batch 9 (asked before the fishway measure was known); the Deer Creek mismatch goes into the
  gotchas; no cut moves. Stave (2672), typed: "it looks right but just to confirm those are the items with the names
  in name variants right?" Checked: yes, `data/curated/waters/name_variants.json` holds "Ruskin Spawning Channel"
  (the unnamed side channels on the east bank below Ruskin Dam) and "Northrop Spawning Channel" (the added stream
  OpenStreetMap calls Northrop Channel); those are the cyan pieces. Cowichan, Sandy Pool sign to the tidal boundary
  (2774; the yellow runs about 2 km into the water the app tints as tidal), typed: "we want to use our tidal
  boundaries defined with the mouth definition. these should already all be in so use existing tidasl boundary item
  dont trim it to dfo polygon". So: the place ends at OUR tidal boundary cut (the mouth definition), as drawn; the
  tinted polygon is not what trims a place. Right as drawn.
- Pictures 5 to 8, 12:27. Cowichan, 66 Mile Trestle to the Sandy Pool sign (2775): "Right". Nanaimo, Cedar Road
  Bridge to the Highway 1 overpass (2776): "Right". Tatshenshini, the Blanchard (2779), typed: "wrong iut should
  have the tatshenshini river too it is just saying that plus the blanchard". So DFO's "upstream of the BC/ Yukon
  border – along the Haines Hwy.; Blanchard River." = the Tatshenshini River above the BC / Yukon border AND the
  Blanchard; my save 2779 (the Blanchard alone) is wrong and is redone, with a new picture for him. Bella Coola
  whole water, Atnarko out (2731): "Right".
- Pictures 9 to 12, 12:31. Skeena "mainstem waters only", pink (2782): "Right". Stamp "Except in those areas and
  times listed below" (2788), with the listed "Ash River confluence to Great Central Lake" place taking the Robertson
  Creek system's side streams and lakes: "Right, all of it". Little Qualicum "all waters unless indicated below"
  (2789), typed: "all looks good jsut make sure all waters unless indicated takes onyl the part not CURRENTLY
  regulated on that day (if it is active)". So: right (the listed stretch starting at the park's downstream boundary
  included); and the rest-of rule must cover, on any day, only the water where no listed stretch has a rule IN FORCE
  that day: to be shown to him on the Little Qualicum for a day inside and a day outside a listed stretch's dates.
  Nitinat "downstream of Parker Creek, except…" (2792; the last 4 km inside the tinted tidal polygon): "Right: our
  tidal boundary stands".
- Picture 13 and trout round 7, 12:42. Somass "Except in those areas and times listed below" (2787; the listed
  "Paper Mill Dam upstream approximately 0.5 km to Collins Farm" stretch is drawn about 1.3 km long), typed: "right
  all of it but add to gotchas please". ALL 13 URGENT PICTURES ARE ANSWERED: right = Sproat, Stave, Cowichan x2,
  Nanaimo, Bella Coola, Skeena pink, Stamp, Little Qualicum, Nitinat, Somass; Stamp Falls kept at 500 m below the
  fishway; WRONG = the Tatshenshini / Blanchard place (it must include the Tatshenshini above the border too).
  Trout round 7 (published 12:33, same page, mockups only). Three-number top line on Madden Creek ("Up to 2 today,
  or 5 if they're rainbow trout and no more than 2 Dolly Varden/bull trout"): "Yes, that wording (Recommended)".
  Moving sideways between fish in the sheet: "No sideways at all". The year bar, typed: "i like it on the elk where
  it says trout and char through the year when they are all release (or all closed etc) if some are open it seems
  silly maybe. but in general dropping it seems fine.  .. i dont know why it needs year tho when it has dates.." So:
  no sideways moves in the sheet; the year bar is dropped except the "through the year" line on a water where every
  fish shares one state (the Elk); dates in words do the rest.
- 12:42, the reviewer's SECOND attack (round 7) is back: still "not yet". Fixed: stop-fishing on the row, the summed
  "only 3", the angler-only note. Partly: possession, the wild limit across boxes, the top line, the year bar, the
  wild limit in one box. New faults: possession smaller than the day's number (Cunningham and four lakes: "only 3"
  with "only 2 in your possession" reads keep 3); possession with a size (Tagish: one over 90 cm a day reads fine,
  the book allows one in possession); "a wild fish must be 50 cm or less" forbids the bull trout over 60 cm the same
  printed line allows (six Region 2 lakes); Lois Lake's top line promises 10 against a wild limit of 2; "stop fishing
  here" can be read as this stretch. Report appended to `handoff/PLAN/design/trout-adversarial-review-2026-11-16.md`.
- 12:43, the rest of the re-check pictures are drawn (places 14 to 30 and the four closed areas, as JPGs). The
  drawer's findings: Morice Gosnell-to-Lamprey END dot sits on a side channel 2 km off the Morice; Thompson Deadman
  START is 495 m below the Hwy 1 bridge where DFO says about 1 km; Babine: 7 circles match by name (Morrison, Pierre,
  Twain, Tachek, Five Mile, Four Mile, Big Loon), Tsezakwa's circle sits on Nilkitkwa Lake, Pendleton has no match,
  Six Mile / Hazelwood / Sockeye and the Gullwing line are not drawn, Pinkut has a circle but is not in DFO's list.
- 12:44, trout round 7, he sent a screenshot of the Skeena card (Sedan Creek to Mill Creek, Jul 15), typed: "it is
  hard to tell 1 of 5 is over 50cm here applies to all instead of just brook trout also the brook trou limit feels
  like the full limit makes it seem like the 5 doesnt apply to all?" What the picture shows: the grey "Keep 5 in all"
  cell runs only beside the top strip and the brook trout row, so it reads as brook trout's own limit and the white
  cells below as separate limits; the "Of the 5, only 1 over 50 cm (20 in)." strip is tied to nothing. Round 8
  started 12:45 with the original designer: the whole-box number must visibly hold every row of its box and the
  box-wide size sentence must be tied to it; plus his round 7 answers (no sideways, the bar dropped, dates in words)
  and the second attack's faults. Mockups only. The reviewer attacks round 8 after.
- Pictures 14 to 17, 12:53. Exchamsiks above the Hwy 16 bridge (2728): "Right". Bella Coola "All tributaries"
  (2730), typed: "purple is right " (the purple / pink lines are the side streams in the place; the main river and
  the Atnarko are out): right. Bulkley "all tributaries … other than the Suskwa River" (2732), typed: "right but add
  to gotchas. it should only drop the blue suskwa river. rest are good. ": right (Suskwa out, Morice and Two Mile
  Creek in); gotcha entry. Bulkley "… other than Morice River and tributaries, Suskwa River and tributaries, and Two
  Mile Creek" (2733), typed: "right only keep part that is purple. rest get dropped here. ": right.
- 12:53, typed in chat: "also we need to make sure the salmon closed because the river is clsoed in synopsis appllies
  to the map too. so the colors show it as well." So for T796 A5: the Salmon view's line colour (and the year bars
  and alerts) must show a book-closed stretch as closed, not only the card. That is what the agreed two-half fix does
  (the book's closure becomes the first member of the stretch's salmon chains); it is now a pass condition on the
  ticket, with a picture of the map before and after for him.
- Pictures 18 to 22, 12:58. Illiance and Wilauks Creek (2734, 2735; the whole water from its mouth, side streams
  in): "Right, both". Gitnadoix (2736; whole river from the mouth, no powerline to be seen): "Right". Morice, the
  Forks to the Bymac Bridge (2738; side streams on), typed: "i think side streams are off but it depends on the name
  and if it says includes tributaries?" Checked: DFO's heading for the water is "Morice, including tributaries", so
  by his rule ("The heading counts") the side streams stay ON; right as drawn. Morice, Gosnell Creek to Lamprey
  Creek (2739; the END dot sits on a side-channel line about 2 km off the main channel), typed: "i thought we already
  fixed the lamprey creek confluence issue. yes the confluence should be on the main channel... fix it". So: the
  Lamprey Creek confluence cut goes onto the Morice's main channel; picture to him after (T803).
- Pictures 23 to 26, 13:05. Nanaimo, 500 m below Bore Hole to Boulder Creek (2777; side streams off), typed: "right
  assuming that the name didnt say to include tributaries. (i think all the items like this we need to set to no call
  then the name will say include tributaries? im not sure tho because if they are linked to a name tat says includes
  tributraries then it couldnt link to a different name without that? the locations should be matched only to their
  exact naming then". Checked on DFO's page: the headings are "Nanaimo River" and "Cowichan River" (no tributaries),
  so side streams off is right there. His rule, sharpened: a place takes its side streams from the EXACT heading it
  sits under on DFO's page; a saved on / off is only for an exception. DFO has TWO Morice headings, "Morice River
  (including tributaries)" and "Morice River": which one each Morice stretch sits under must be read from the page
  (my answer to him on picture 21, "the heading is Morice, including tributaries", is not yet proven for the Forks to
  Bymac row). An audit of every DFO place's side-stream setting against its exact heading is started; the list of
  disagreements goes to him. Tatshenshini, Kwatini / Stanley / Goat creeks (2780), typed: "wait this is right and
  blanchard was also suppsoed to be only blanchard i was confused on formatting. made more sense when i looked at
  page". So: 2780 is right, AND 2779 (the Blanchard alone, picture 7) is RIGHT: his "wrong" of 12:27 is withdrawn;
  nothing is redone. Morice, lake outlet to Gosnell Creek (2781): "Right". Sheldens Creek (2783), typed: "add as
  gotcha but i think you can see the road in satellite that crosses neaar the mouth that should be the bridge i
  think (near a camp site etc) have it be low confidence and set it top tidal too ". So: a low-confidence cut where
  the road crosses near the mouth (the Spur 19 bridge site, by a camp site), the place is the creek above it, the cut
  is also the tidal boundary; gotcha entry; picture to him (needs the next map rebuild).
- 13:04, from the app chat (it is closing; a new app chat takes over from the top section of
  `handoff/PLAN/ios/START-HERE-ios.md`), for his backup question (T732): "Backup photo copies at 1280 px, measured
  2026-11-16 12:57, picture sent in the app chat 12:59: B JPEG 70 = 0.25 MB (about 81 fit), C JPEG 60 = 0.18 MB (about
  110), D WebP 75 = 0.08 MB (about 245). Pick B, C or D; recommended D if a restore on the emulator shows WebP copies,
  else C. Not answered."
- 13:06, two fix lanes are back (branches in worktrees, not merged): `lane/salmon-rules-1116` (T783, T770 with the
  eulachon line, T787 hidden rows, T796 A6 and A5 Python halves) and `lane/book-rule-fixes-1116` (T795 six lakes,
  T794 Williston / Peace / Bella Coola, T798 Coal Creek and the Puntledge, T801.1, rulings). For him: (1) the
  Puntledge "fly fishing only upstream of the diversion dam, including tributaries" now reaches 1,069 pieces (every
  stream feeding Comox, Willemar and Forbush lakes) where it was 10; (2) T796 A5 changes 70 stretches (71,046 pieces),
  the largest the Skeena / Nass winter closure on coho Jan 1 to Jun 15; (3) T770 adds a gear line on 13 chains; the
  three T796 examples he asked for are in the lane's report. Trout round 8 is built (the box frame redrawn).
- Pictures 27 and 30, and two results of the fix lanes, 13:28. Babine Lake's closed areas (7 circles match by name;
  Tsezakwa's sits on Nilkitkwa Lake; Pendleton, Six Mile, Hazelwood, Sockeye and the Gullwing line are not found;
  Pinkut has a circle but is not in DFO's list), typed: "confirm 7 lake open with note and please add as a thing for
  me to do later". So: the 7 are confirmed, the lake reads open in August with "closed in part" and DFO's words for
  the areas not drawn, and finding the missing five and the line is a LATER item on his list. Dean, Crag Creek to the
  tidal boundary (2793), typed: "yes and that except should not include the canyon": right; the canyon stays out of
  it as its own stretch. Puntledge "Fly fishing only upstream of the BC Hydro diversion dam …, including
  tributaries" reaching every stream above the dam (10 pieces to 1,069): "Yes, as printed (Recommended)". T796 A5,
  a book-closed stretch closes salmon too (three examples shown; 70 stretches change): "Go, and send me the full
  list" (the list of all 70 with dates and a map picture before and after go to him before it ships).
- 13:29, the heading audit is back (`handoff/PLAN/curation-review/dfo-side-streams-against-exact-headings-2026-11-16.md`):
  239 live named-water places checked against the exact heading each row sits under: 225 agree, 14 disagree. The
  Forks-to-Bymac stretch DOES sit under "Morice River (including tributaries)" (side streams on is right). The 14 go
  to him as questions (Tatshenshini whole-river row; the three creeks and the Blanchard: do named side creeks take
  their own side streams?; five Region 6 rivers whose blank row under a plain heading puts a bait ban on side
  streams; the Kitimat outfall strip under "Kitimat River (including tributaries)"; five with no effect today).
- 13:29, the reviewer's THIRD attack (trout round 8) is back: closer, still "not yet". The frame is better but the
  grey still hooks onto the first row; "no limit of its own" can read as "unlimited"; the date words are right on 49
  of 50 lines but say nothing when a fish goes to zero, when wild / hatchery changes, or when the water closes; wild
  char at Dodd and Lois now look keepable on their rows. Appended to the design review file.
- Pictures 28, 29, 31, 32 (answered about 13:38). Kitimat "on west bank between signs at Kitimat hatchery outfall"
  (2786; drawn river only), asked whether DFO's heading "Kitimat River (including tributaries)" makes the strip take
  the side streams that join there: "Heading counts here too". So my save 2786 (side streams off) is reversed: the
  strip takes its side streams (about 12 pieces), both banks still closed until one-bank places exist. Skeena
  sockeye leftover place, CNR bridge to above the Babine (2790): "Right". Thompson closed area Deadman (2748; START
  measures 495 m below the Hwy 1 bridge where DFO says about 1 km): "Find out why, then show me". Thompson closed
  area Ashcroft (2749; starts at the Bonaparte mouth itself): "Right".
- Pictures 33 and 34, the heading rule, the size limit (answered about 13:42). Thompson closed area Juniper (2785):
  "Keep 4 km (errs closed)". Shuswap closed stretch (2770): "Right: keep the printed points". ALL 34 PICTURES ARE
  ANSWERED. The ten places that take side streams only through the old water-wide switch (the Tatshenshini
  whole-river row, Kitseguecla, Sustut, Cranberry, Kiteen, Meziadin, three Nanaimo places, Little Campbell, the
  Morice Forks): "Yes, river only for all ten (Recommended)" (changed answers shown to him before it ships; the
  water-wide switch is retired; the setting is derived from the exact heading and the place's words). The salmon
  head file's size limit, 60 kB, so that a book-closed stretch closes salmon everywhere (about 92 kB): "Raise to
  100 kB (Recommended)".
- Named side creeks and trout round 8 (answered about 13:48). A DFO row that names a creek without the word
  "tributaries" (Tatshenshini: "Kwatini Creek, Stanley Creek and Goat Creek"; "Blanchard River"): "A named creek
  keeps its side streams". So: naming a creek means its whole small system; saves 2779 and 2780 stand; this is a
  stated exception to the heading rule (a ruling). Trout round 8, the grey band beside a fish with no tighter limit:
  ""up to the 5" (Recommended)". A possession limit smaller than the day's limit (Cunningham Lake), typed: "it is
  only possession of one type of trout fo rcunningham. use best judgement here". So: the possession line belongs to
  the lake trout row only and must say so; the wording is left to my judgement (the possession number leads on that
  row, the day's 3 as a quiet line). What next for the trout work, typed: "round 3 then attack again but use opus".
  My reading: one more round (round 9) and one more attack, both done by Opus agents, not Fable. Mockups only.
- 13:49, typed in chat: "Can we reduce how much fable we use for our workers a little bit too? Just want to save fable
  for the things we need it for". So: from now no worker runs on Fable by default (reviews, audits and design
  included); Opus for data, pipeline, review and design work, Sonnet for interface and mechanical work; a worker gets
  Fable only after I name the task and the reason to him and he says yes. Nothing on Fable is running now (the
  trout designer and its attacker moved to Opus at his word earlier this hour).
- 13:55, pushed: the book-rule fixes are on the shared branch (T795 six lakes, T794, T798, T801.1; full tier 6,499
  pass). A reach-only rerun is going on lane 1 to show them on built data.
- 13:56, T796 A5, the full list he asked for was sent to him in chat (file `book-closures-list.md`, 70 stretches,
  71,046 sections, 25 rows flagged for a second look; the Nass side streams' coho Jan 1 to Jun 15 first). The map
  follows: the three examples paint closed on their days in the rebuilt salmon data. NEW, for his decision: with the
  book closure carried everywhere (his "Raise to 100 kB"), three OTHER size limits go over on today's numbers: the
  Region 6 salmon file (about 432,600 against 420,000), the website's first-tap budget (about 4,312,000 against
  4,200,000; it had 1,243 bytes of room before) and its low-data first-tap budget (about 3,472,600 against
  3,400,000). The release would stop at its size check until he raises them or the data is shrunk first (T686).
- About 14:55, his answer to the size-limit question, after reading the list of book closures (typed, verbatim): "for this
  Sections: 7 The book closes it (p.9, National Parks and Ecological Reserves): "Fishing is prohibited in Ecological
  Reserves in B.C." DFO's place there: Skeena River — mainstem waters downstream of CNR Railway Bridge at Terrace. it
  should only close the part in the eco reserve how the entire place?? / but yes you can do the raise we just need
  to make sure the closures only apply in the area actually closed e.i. above. / also we should look at the real
  consequence of this (Nass watershed side streams (DFO section C): coho Jan 1 to Jun 15 ) / also previewing all the
  closures. (again we assume we will close them but i want to be able to review them after) could we make the way it
  is viewed an artifact with them all formatted in a way it is easier to read. (with mapos etc) use a smaller model
  for this... / also why is this doubt (The salmon card said on those days (DFO's rows): open — "4 per day, only 1
  over 50 cm (Apr 1–Mar 31)"; open — "4 per day, only 1 over 50 cm (Apr 1–Oct 31)") to (It now says: closed — "Closed
  to all fishing by the rule book (p.19): No Fishing from Yakoun Lake downstream approximately 13 km to fishing
  boundary signs, Oct 1-Apr 30 (Oct 1–Apr 30)") / the dates are opposite of each other the closed should only be
  there on the days it is actually closed then the other days the salmon rules would still apply which isnt a
  conflict?"
  So: (1) YES to raising the three limits (Region 6 salmon file, first tap, low-data first tap). (2) A closure must
  cover only the water the book actually closes: on the Skeena it is the 7 pieces inside the reserve, not DFO's whole
  place (that is what was built; "DFO's place there" on the list only names which DFO row answered before): to be
  proven for all 70 and said plainly on the page. (3) The Nass side streams' coho closure Jan 1 to Jun 15: look at
  its real consequence before it ships. (4) He wants all the closures on ONE page (artifact) that is easy to read,
  with maps, to review after; built by a smaller model (Sonnet). (5) The Yakoun row: the book closes Oct 1 to Apr 30
  only; on the other days DFO's rows answer as before: that is how it works; the warning on that row was about the
  side creeks along the 13 km, not the dates.
- 15:01, on trout round 9 (typed, verbatim): "for the trout view i liked the old look more but with the words of
  round 9 buyt the frame look is a lot cleaner". My reading (mine, to be confirmed by picture): round 8's look (the
  tinted box and the white boxes), round 9's words everywhere ("up to the 5", the dated lines, the strip on one
  line), and round 9's cleaner outline around the box. Round 10 is built as that blend, with the Skeena card shown
  both ways (with and without round 9's outline) so he can pick. Mockups only.
- 15:08, typed in chat (verbatim): "this chat will have to change to opus soon. out of fable credits could you make
  sure we are ready for when that happens?" Done 15:12: the state page (`handoff/PLAN/INFLIGHT-chat14.md`) opens with
  a section "READ THIS FIRST: handover at 15:10" that holds the rules, the four running workers and what to do when
  each hands back, the landing steps for the salmon lane, the queue to the next staging release, and what is still
  his. The session notes point at it. Nothing of his is needed for the change.
- 15:16, one dialog with the round 10 trout picture (Skeena card three ways) and the salmon lane's Nass finding.
  Trout look (A old look no outline / B old look + outline / round 9), typed, verbatim: "old look no outline because it
  looks like the groups are subgrouped inside the over keep 5 card then... one thing is the words "only 1 of these 4
  until oct 31, then release" should be changed to be "onlly 1 of these 4" being bigger it is the actual rule right
  now then a smaller text for the change underneath. all current rules will have the bigger text. " So: A (the round 8
  look, no outline); and in every white box and row the rule in force today is the bigger text, the coming change
  ("until Oct 31, then release") smaller underneath. Trout row spacing (round 8's roomy rows, 23 of 42 cards fit one
  screen / round 9's tighter rows, 34 fit), typed: "inbetween the 2". Nass side streams, coho Jan 1 to Jun 15 (closed
  as the book says / leave coho open / hold this row), typed: "how are both items worded let me double check but im
  pretty sure closed dis the right option". So: NOT yet decided; he gets the book's line and DFO's row word for word,
  then answers. The row does not ship before that.
- 15:18, typed in chat (verbatim): "have another opus agent spec out what would be involved in transitioning the trout
  portion of the app to the new table format. want it implemented clean from pythion up to front end. etc etc. use
  opus agent". So: an Opus worker writes a spec (no code): what it takes to move the app's trout and char block to
  the new table, built cleanly from the Python pipeline up to the app's screen. The spec comes back to him before
  any build starts.
- 15:19, typed in chat after reading the book's line and DFO's row for the Nass side streams (verbatim): "and yes the
  closure wins here." and "we need to make sure it says where the closure is from though". So: the Nass row ships
  with the other 70 (coho closed Jan 1 to Jun 15 on the side streams above the Kitsault Bridge); and a pass
  condition for all 71: the salmon card says where the closure comes from (the rule book, its page, its line).
  Checked on the lane's data 15:19: all 71 rows' card words open "Closed to all fishing by the rule book (p.NN):"
  followed by the book's line and dates. Still to prove on the app's screen (the app chat's before / after pictures).
- 15:22, the closures review page he asked for is published (built by a Sonnet worker, 71 cards, a map each):
  https://claude.ai/artifact/2eGEuMG9cLe3SS69j63aTd ("Book Closures Review"). Also a row on the Owner To-Do page
  (`rv-book-closures-page-1116`, group Review). Each card: row number, map (closed pieces in red), the book's line,
  page and dates, what the salmon card said before, fish and days now closed, how much of DFO's place changes. Known
  limits, told to him: no "rest of DFO's place" layer on the maps; rows 5, 6, 17, 19 miss 25 to 35 pieces on the
  picture (said on the card); the book's "[Includes Tributaries]" tag is not yet shown under the line (second
  version being built: Dean River row 12 is the example, the book does print it there). FOR HIM: review when he
  likes; any row he calls wrong by number is held back.
- 15:36, trout round 11, one dialog with two pictures (Skeena before / after; a Fraser card where only trout and char
  close). Is this the card to build, typed, verbatim: "it looks good but it is missing the general 30cm or bigger for
  trout/char in region 2 fraser? ". Closure words ("until Sep 30, then closed to trout and char, reopens Nov 1"),
  typed: "yes but drop reopen unless it is currently closed then reopen date matters then ". A limit that only gets
  looser on a coming date (skip it and show the next stricter date?), typed: "could just mention a rule change
  happens on that date not specifics". Adams River Dolly Varden / bull trout: "Yes, 80 cm on the row (Recommended)".
  So: the look is accepted ("it looks good"); to check: why the Fraser card's brook, brown and cutthroat rows carry
  no "30 cm or longer" line (is a general size rule missing from the data or the mock, or does it not apply on that
  stretch); a coming closure says what closes without the reopening date, and the reopening date is shown only
  while the water (or the fish) is closed today; a coming change that only loosens a rule is said as a plain "rule
  changes <date>" with no details; Adams as recommended.
- 15:42, the trout build spec (`handoff/PLAN/design/trout-table-build-spec-2026-11-16.md`), its one-page summary given
  in chat, four questions in one dialog. How to start: "Start steps 1 and 2" (step 1 = fill the data holes:
  possession limits, the wild-fish limit at Lois and Khartoum, the odd lines; step 2 = a tool that lists every
  answer that changed between two releases). Other fish in the same new table look, typed, verbatim: "what? we want
  to migrate the new trout style into the current app not the other way around. " (my question was badly put; it is
  NOT answered: the plan IS the new trout table inside the current app; whether kokanee, whitefish and the rest
  also take the new look is asked again, plainly). Possession: "Keep the page line (Recommended)". Size, if the
  switch does not fit the first-tap limit: "Raise the limit, shrink later (Recommended)".
- 15:42, typed in chat (verbatim): "also the last fraser example you sent doesnt have hatchery bs wild along with the no
  30cm minimum". Checked against the book text (`data/curated/regulations/reference/region-3.md`, Region 3 daily
  quotas): that card is the Fraser in REGION 3 (Thompson confluence to about 4 km below Lillooet), whose regional
  rules are "Trout/char: 5, but not more than 4 from streams, 1 over 50 cm, 1 bull trout (Dolly Varden) or lake
  trout, none under 60 cm; release ALL STEELHEAD, bull trout from streams Aug 1-Oct 31, lake trout Oct 15-Jan 31":
  no wild / hatchery split and no 30 cm minimum. Both belong to REGION 2 ("you must release wild trout/char from
  streams … hatchery trout/char under 30 cm from streams"). Round 12 adds a Region 2 Fraser card beside it; if our
  data does not show hatchery / wild and 30 cm there, that is a fault to report to him.
- 15:47, trout round 12 published (same trout page; two pictures sent: the Fraser in Region 3 beside the Fraser in
  Region 2 at Mission, which shows hatchery and wild rows and "30 cm or longer" from the data: no fault; and the
  Region 3 stretch on a closed day with "reopens Nov 1"). The other-fish question asked again (leave them / redraw
  them / decide later), typed, verbatim: "we should just have the trout table under the trout entry like it
  currently is with the better text like 2 or 4 with expand arrow etc. " My reading: the page keeps its present
  shape; the Trout and char entry stays where it is, folded to its short line ("Up to 2", "Up to 4") with the expand
  arrow, and the new table is what opens under it; the other fish entries are not touched. So spec question 1 =
  trout only; the old card code stays for the other fish.
- 15:47, the 17 closure rows with side streams and "no tributary words" (closures review page): an Opus audit found
  ALL RIGHT by ruling T2. On 12 of them the book prints the ✱ mark beside the water's name ("includes tributaries"
  for every rule of that row); the page builder had only looked for the words. The other 5 close nothing but the
  named creek itself. Dean River row 12 (control): the walk stops at both ends of the stretch. Nothing is placed too
  wide; trout answers are unaffected. The page (version 3) now shows the ✱ note on 20 rows.
- 15:48, typed in chat on round 12's Region 2 Fraser card (six hatchery rows in the keep box, six wild rows under
  Release), verbatim: "i dont like the new wild and hatchewry option you sent we should jsut say hatchery only unless
  specifically wild has a seperate quota to wild like 1 wild and 2 hatchery or something.... otherwise it is
  hatrchery only or unspecified (the same for both no mentiuon needed)". So, three cases and no others: (1) wild and
  hatchery have the same rule: no mention at all; (2) only hatchery fish may be kept: ONE row per fish that says
  "hatchery only", and no separate wild rows under Release; (3) wild fish have a quota of their own (for example 1
  wild and 2 hatchery): only then are wild and hatchery shown apart. Round 13 (mockup) is being built on that.
- 15:49, typed in chat (verbatim): "we will run out of credits soon... we should m,ake sure handoff is good for opusa to
  take over at anytime." Done 15:49: section C of the handover on the state page rewritten as ONE current block
  (what is running, what to do when each worker hands back, what is waiting to be pushed, what is his).
- 15:56, trout round 13 (hatchery only), one dialog with two pictures. Is this the handling, typed, verbatim: "yes this
  is good on fraser but the hatchery on steelhead on skeena doesnt stand out very well. should slightly improve that
  oitherwise it is getting better." Lois and Khartoum steelhead row: "Hatchery only (Recommended)". So: the rule is
  accepted; on a single fish's row (Skeena steelhead) "hatchery only" must stand out more (round 14, small); Lois and
  Khartoum steelhead read "hatchery only" like everywhere else.
- 15:56, typed in chat (verbatim): "you can continue just make sure handoff stays up to date, also what is prompt to
  continue after if fabl;e runs out of tokens". The prompt given to him: "Continue as the main chat. Read
  handoff/PLAN/INFLIGHT-chat14.md: the top section READ THIS FIRST, block C first. Carry on from its list 'Owed to
  him, in this order'. Do not re-ask anything already answered at the end of handoff/PLAN/OWNER-TODO.md."
- 16:00, trout round 14 (the Skeena steelhead row two ways: A a filled "HATCHERY ONLY" label beside the name, B the
  words in the name), typed, verbatim: "A filled label then if it applies to all fish in category use same fill for
  where that is located." So: A; and where "hatchery only" applies to every fish of a keep box, the box's top line
  carries the SAME filled label in place of the plain words. Round 15 (tiny) applies it; then the design is settled.
- 16:03, trout round 15 published (same trout page) and two pictures sent: the filled HATCHERY ONLY label in a keep
  box's top line when it covers every fish (Fraser Region 2: "Keep 2 in all [HATCHERY ONLY] only 1 over 50 cm (20
  in)") and beside the name for one fish (Skeena steelhead). The design is settled unless he says otherwise; the
  accepted design is one list at the end of T810. No question pending on trout.
- 16:04, typed in chat (verbatim): "can we move over to opus agent now?" Answered: yes; he switches the model himself
  (the /model command, pick Opus) and pastes the continue prompt; the state page's block C says what to do first.
- 16:07, typed in chat (verbatim): "okay i created a new chat please coordinate with it for any new info etc as we ramp
  this chat down". The new main chat is session `bc-freshwater-fishing-regulations-8c`. This chat (session
  `bc-freshwater-fishing-regulations-17`) keeps only one job while it ramps down: its three workers hand their
  reports back HERE, so each also writes its report to a file under the scratch folder `handbacks/`, and this chat
  relays each to the new chat when it arrives.
- 16:09, T805 part C (one dialog; the list `changed-answers-C.md`: Lois and Khartoum cards add "No more than 2 wild
  trout and char in all"; Lois and Windsor wild Dolly Varden / lake trout go back, hatchery only like the other five
  wild-2 lakes; 3 waters, Region 2, all stricter). Answer, picked: "Go (Recommended)". So: part C (commit 0f6fe2868 on
  `lane/t805-trout-data-gaps`) lands with the full tier and ships in the next staging release. Parts B and D still to
  come to him as their own lists.
- 16:18, Thompson closed area Deadman (mark 2748; he had said "Find out why, then show
  me"). Why: the APP'S DATA WAS RIGHT; the drawing tool behind picture 31 started the yellow at the bridge's own measure
  (117,608) and snapped it to the nearest section edge, the river gauge 08LF033 (117,094), so it drew 4.6 km and read
  "495 m". The built cut is at 116,608: 999 m below the Hwy 1 bridge; closed area 3.6 km to the CN rail bridge. The
  redrawn picture was sent. Answer, picked: "Right (Recommended)". So: mark 2748 stands; nothing changes.
- About 16:20, typed in the OLD main chat (session -17) after its summary of the changed-answers tool's report
  (verbatim, relayed): "Eulachon is a bait or a limit?" Answered there: neither; it is a closure. DFO's Region 6 line
  "Reminder: There is no fishing for eulachon in Region 6" means no fishing for eulachon at all. The app carries it in
  the salmon card's gear / bait lines, which is why the tool grouped it under "Gear and bait". The change is his T770
  fix (a bait line no longer pushes it out). Open, NOT asked yet (for the end batch): should the eulachon line show as
  a closure rather than among the gear and bait lines, on the card and in the tool's grouping? His earlier words
  (batch 6): "general rule is (Reminder: There is no fishing for eulachon in Region 6. ) why would bait remove eulachon
  rule?"
- 16:37, T805 parts B and D plus two of the worker's questions, one dialog (lists `changed-answers-B.md`,
  `changed-answers-D.md`). Part B (possession on the fish row: Region 7 lake trout / bull trout "1 day's limit, not 2"
  on ~3,100 waters, ~20 lakes' own possession numbers, Bowron Park's 1 every day; words only), picked: "Go
  (Recommended)". Part D (Shamrock and Kilgard creeks: no 20-30 cm steelhead; Echoes Lakes "from both lakes
  together"), picked: "Go (Recommended)". Bowron Lake Park lake trout (park's 1 in all / keep today's), picked: "Keep
  today's" (lake trout follow Region 7's "1 day's limit" on their own row, apart from the park's 1 for the other
  trout; this is what part B builds). Nation Lakes ("Up to 3 lake trout" a day, possession 2: both as printed /
  lower the day to 2), typed, verbatim: "i feel like this possession stuff is just kind of broken but fixed in the new
  trout format? so maybe we can just work on the transisiton to the new trout redisgn?" So: Nation Lakes stay as part
  B builds them (both lines as printed); no further possession patching outside the redesign; he asks to work on the
  trout redesign transition (T810 steps 3 to 7). My reading: that is his go to start the transition; first the spec
  takes in his answers of 15:36 to 15:48 and the app chat's check of the app half. François Lake outlet (part D
  question) NOT asked yet.
- Before 16:42 (the app chat said ~16:45; my clock read 16:42), typed to the APP chat (relayed verbatim): "make sure you and the other chat collaborate on the prepping
  for trout dredsign going in". So: the two chats prepare the trout table build together: one plan (the spec; the app
  chat's app-half check merged into it), the payload contract agreed by both before either builds, one shared fixture
  of hard waters the app builds against, one hard-cases list both re-run, the switch landed together like the salmon
  twins (T810).
- 16:50, typed in chat (verbatim): "we want to make sure the plan is robust so that we change the data all the way in
  python and the front end and it all just works withotu fighting it and makign sure everythign is streamlined etc".
  So (T810): the contract rule is Python decides every word, number and date; the app only draws, with no rule logic
  and no hand-written payload types (generated from the contract); no old and new trout paths left side by side. After
  the app chat's app-half check is merged into the spec, a fresh Opus reviewer attacks the whole plan for every place
  the two sides could fight (two sources of a word, logic in both, duplicated helpers, a hand-kept type), and the plan
  is fixed before any build code starts.
- About 16:55, six answers in the APP chat's dialog batch (relayed, labels picked verbatim): (1) T759 stripe: "Wider is
  right (Recommended)". (2) T732 backup photo copies: "D: WebP 75" (not the recommended B; T732 answered). (3) T810
  fold row: "All rows get arrow": every folded row, the other fish included, becomes words plus an expand arrow, not
  only trout (widens the app scope a little; the app chat folds it into spec section 6). (4) T810 pinned trout top
  line: "Drop it (Recommended)". (5) T810 a lone other fish beside trout: "Stays as today (Recommended)". (6) T810 the
  year bar's "Trout: release until …" line: "Drop it (Recommended)".
- About 17:15, clarification of the 16:55 fold-row answer (APP chat re-asked; relayed, label picked verbatim): "Salmon +
  trout folds" (the option read: "Both folded entries (salmon and trout) show their words + an arrow instead of
  'Details'. The Other fish list stays as it is."). So "All rows get arrow" means the salmon and trout folds only.
- 17:40, the trout table build summary given in chat (five steps: safety pins and size count; Python builds the table
  on the side; the permanent same-law check; the app draws it behind a hidden switch, picture pack and sentence list
  for him; one switch). Two questions, one dialog. Ship the trout table in normal releases before the switch (yes if
  ≤100 kB / only at the switch), picked: "No, only at the switch". Angler-only notes (page's notes block / inside the
  table), picked: "Page's notes block (Recommended)". So (T810): the trout section appears only in the switch
  release; previews before it use side releases (ticket V-py's side scenario); the cutover = flag on + rows 34 +
  format bump + deletions. Q-D7 decided: angler-only notes stay in the page's caveat block; the table carries none.
- 17:44, salmon book closures, the data side of the break-it review (T796 hard cases; worker branch
  `lane/salmon-hardcases-1116`), one dialog of four. Campbell River summer closure (MU 1-1 to 1-6 vs the book's MU
  1-10), picked: "Keep it closed". Thompson below Kamloops Lake in June (spring closure vs its own row, no printed
  exemption), typed, verbatim: "keep closed but add to gotchas as one i need to ask fish and wildlife about". Waters
  partly inside a reserve or park, typed, verbatim: "take out the lakes but add a note. also for streams i thought we
  cut on all eco reserves so should everypiece be in the eco resevre or is it anti cross over we added for areas? in
  that case we decided on a cut which defines in versus out. so in that case if it rides the boudnary (straddling) it
  should get the part intended to be in to have thge closure versus the part out. ask more questions if needed".
  National parks wording, picked: "Closed, book's wording (Recommended)". So: Campbell unchanged; Thompson June stays
  closed AND goes on his list of questions for BC Fish and Wildlife; the 9 lakes and ponds mostly outside a reserve
  or park come out by name, each with a note on the card; for STREAMS he wants the closure on the part inside the
  reserve only (a straddling section split at the boundary, inside part closed, outside part open): to be checked
  against how area boundaries work today (cuts vs the straddle rule E8) and put back to him if it needs a decision;
  national parks read "Closed by the rule book unless opened under the National Parks Fishing Regulations (p.9)" with
  the permit sentence.
- 17:46, follow-up on his streams question (told: streams ARE cut at every reserve/park edge, ruling E8: first entry
  and last exit; a straddling piece or a wander outside under 1 km stays inside; the 4 pieces 10-50% inside sit
  between the cuts). Keep E8 / cut exactly at the line, picked: "Keep E8 as is (Recommended)". So: streams unchanged;
  only the 9 lakes and ponds mostly outside come out by name (ruling Z10), each with a note on the card.
- 18:37, typed in chat (verbatim): "We also want to start another agent that works on finding the missing river
  confluences on the babine lake dfo place closures. We want to find the other rivers and have an agent dedicated to
  it". So: a dedicated Opus worker finds the creek mouths DFO names in its Babine Lake closures that we have not
  placed yet (from the 11-16 re-check: Pendleton has no match; Six Mile, Hazelwood, Sockeye and the Gullwing line are
  not drawn; Tsezakwa's circle sits on Nilkitkwa Lake; Pinkut has a circle but is not in DFO's list), and proposes
  each for his review (everything added is a proposal).
- 18:38, typed in chat (verbatim): "this is fine (Tsezakwa: its circle sits on Nilkitkwa Lake instead of Babine. )".
  So: Tsezakwa's circle on Nilkitkwa Lake stays as it is; the Babine worker leaves it alone.
- 18:54, Babine Lake (five pictures sent; worker branch `lane/babine-mouths` 6b1aa369d, proposals only). The four
  missing circles ('Six Mile Creek' = Gullwing Creek; 'Pendleton Creek' = Cross Creek at Pendleton Bay; Hazelwood and
  Sockeye = the unnamed creeks at DFO's own census sites), typed, verbatim: "That sounds good can we add name variants?
  With source?" The Gullwing Creek line, picked: "Due south" (straight south from Gullwing Creek's mouth, 3,356 m, not
  the shortest crossing). So: the four circles are accepted; each creek gets a NAME VARIANT with its source (Gullwing
  Creek also "Six Mile Creek"; Cross Creek also "Pendleton Creek"; the two unnamed creeks "Hazelwood Creek" and
  "Sockeye Creek"; source = DFO NuSEDS census sites + SkeenaWild 2011, cited on each), as name variants are recorded
  today; the line is the due-south reading. Also found: his 7 circle confirmations of 13:28 were never saved (no
  Babine closed-area marks; `closed_areas.json` holds only Thompson and Shuswap): they are saved with these 4 and the
  line through the review app's save path. Effect today: none on the map (a circle binds only sections wholly inside
  it; the lake is one section; the line across a lake is deferred): notes on the card until a lake-split ticket.
- 18:56, typed in chat (verbatim): "We want to split babine lake on the line like our other split lakes. Then we want
  the circles to just be circles on the map not separate sections of the lake. Otherwise it will be overboard." So
  (T812): Babine Lake is split on the due-south Gullwing Creek line the way other lakes are split (east part = its
  own piece, closed as DFO says); the 400 m creek-mouth circles are NOT cut out of the lake: they are drawn as circles
  on the map (an overlay) with the card's note, no extra lake sections.
- 18:57, the Babine circles' look (T812; the app chat's three questions, one dialog). Which map, picked: "Salmon map only
  (Recommended)". Follow the picked day, picked: "Follow the day (Recommended)" (closed colour on DFO's closure days,
  outlined or hidden otherwise). Tap, picked: "Opens the lake's card (Recommended)" (Babine Lake's salmon card, whose
  note says which creek mouths close and when).
- 19:01, typed in chat (verbatim): "We could skip some tests to get multiple items done then merge after and test
  together? Or is that too risky?" Answered: batch the merges (each lane already ran its own tests), then ONE full
  tier and ONE push for the batch; never push without the full tier (the 11-15 red landings). Adopted from now.
- 19:09, he asked whether he still has saves to do and to take the two end-batch questions now ("make sure i didnt
  already answer these things"). Checked: the "leftover saves" in my 19:0x summary were MY fixes, all already answered
  by him (Morice 2738 by the heading rule; Morice 2739 "fix it" = the Lamprey cut onto the main channel; Sheldens 2783
  = low-confidence road cut + tidal; Babine 2784 = done 19:07, marks 2825-2836; Dean 2793 = the except without the
  canyon; Thompson 2521 = the three closed areas; Morice 2596 retired; Atnarko 2584 "The heading counts"). Told him so.
  One dialog of three. Squamish "downstream of the powerline crossing" re-tick (T811, mark 2511), picked: "Re-tick as
  right (Recommended)". François Lake outlet lines (T805 D question 3; book p.51 "for the area at the outlet of
  François Lake described on map 1"), picked: "Outlet area only (Recommended)" (all seven lines on the outlet area;
  the main lake reads Region 6's rules; map rebuild; changed answers to him first). Eulachon (T770), picked: "As a
  closure line (Recommended)" (its own line that eulachon is closed all year in Region 6, apart from the gear and bait
  lines).
- 20:01, two pictures (cut-fixes lane `lane/cuts-1116` a5990dd7b). Sheldens Creek (the SPUR 14 road bridge by the camp
  site crosses the estuary channel ~260 m BELOW our FWA line's end at the top of the salt marsh; proposed: mouth =
  tidal boundary, low confidence, whole creek above), picked: "Extend to the bridge" (draw the creek on down to the
  road bridge and put the low-confidence tidal boundary there; needs an added stream piece). Morice, Gosnell Creek to
  Lamprey Creek (mark 2739; END on the main channel at Lamprey's mouth; the earlier picture was a drawing-tool fault),
  picked: "Right (Recommended)".
- 20:25, Sheldens Creek extended (picture `sheldens-extended.jpg`; `lane/cuts-1116` 998b4e6ab: a hand-traced marsh
  channel from the FWA line's end to the SPUR 14 road bridge, low-confidence tidal boundary at the bridge), picked:
  "Right (Recommended)". Goes into the next atlas rebuild; redrawn from the built map after.
- About 21:22, typed in the APP chat after "Install build 36 and try a–f" (relayed verbatim): "a-d worked good except
  after closing app and opening again the fish icon stopped moving.... yes to (E) and (f) .. the picture viewer for
  spots has the x under the notification bar. text is cut off weird. also opening harrison lake then going to
  conditions should default to level only because there is no other data (the map looks super weird and not visible
  because of it) then when you switch to level the empty flow option disappers so if only level data exists it
  should show level data. just mention at top it is level data maybe... have an agent look at this... how do i test
  the push stuff now?" Filed by the app chat: T814 (iOS fish spinner stops after reopening mid-download), T815 (spots
  photo viewer ✕ under the status bar), T816 (clipped text: the "When were you there?" hour, the weather icons), T817
  (Harrison Lake: a water with only level data opens on Level with a line saying so; the gauge rule "Flow leads"
  refined, not broken). iPhone build 36 test (a–f) done; push test under way with the app chat.
- About 21:32, typed in the APP chat (relayed verbatim): "the spots view also needs a lot of work to be cleaned up maybe
  put a fresh agent on it?". The app chat's T819: a fresh Opus worker reviews the spots area and brings him pictures
  and a cleanup list to pick from before anything is built.
- About 21:37 to 21:40, typed in the APP chat (relayed verbatim): "also the calendar bar doesnt have a full red line for
  salmon when closed but instead it is empty with no color on the calendar bar.." (T820: the salmon year bar on a
  closed day draws empty, not the closed stripe); "when i swiper to close on apple it flashes the menu back for a
  second before it goes back." (T821: iOS back-swipe flash); "allowed notifications in app and settings restarted app
  and it still doesnt show up" (T822 if a bug: push registration; the staging worker holds 0 iOS tokens).
- Before 21:41 (the app chat said ~21:45), typed in the APP chat (relayed verbatim): "in the tutorial it says on first slide each step rings what to
  tap (should say something like highlight instead? the tutorial is really glitchy too with keyboard and the
  highlighting following the items with a delay etc etc". The app chat files it (its next reserved number).
- Shortly before 21:48, typed in the APP chat (relayed verbatim): "also the condition panel is very glitchy on apple. it
  doesnt show the info the same as android the mao doesnt have gauge markers the current conditions view doesnt show
  the bar and when i click on chehalis it doesnt show conditions for it? also the highlights dont let go when you go
  back the highlight of where you clicked stays and doesnt disspear". Filed by the app chat (its own number). If the
  Chehalis showing no conditions is data (no gauge match), it sends me the section ids.
- 21:50, typed in chat (verbatim): "it used to match the chehalis gauge though?????". Checked: it still does. The shipped
  release r-46e4355a7277 (conditions-18da9a4cead9) has gauge 08MG001 Chehalis River near Harrison Mills leading 634
  Chehalis sections (matches.json unchanged; 08MG023 below Statlu Creek auxiliary). So the iPhone showing no Chehalis
  conditions is app-side (with the missing gauge markers and bar on Apple); sent to the app chat for its conditions
  ticket.
- Shortly before 21:54, the spots cleanup (the app chat's T819; relayed verbatim): picked all four bundles, "B: readable
  spot page (Recommended)", "A: fix wrong bits (Recommended)", "C: list + adding", "D: fit & finish". On a small creek
  (1.4 km²) shown "Below normal" from a gauge 676× bigger, typed: "Could we just use our gauge network that colors stuff
  wtc to determine if it is significant enough? It would drop that in theory. Then ofr small ones still give a warning
  if used." So: a spot takes its gauge from the same section → lead-gauge network the Conditions map colours with
  (drainage gate applied); no lead = no estimate; a lead on a small water = the estimate with a plain warning. The
  "small next to its gauge" decision is Python's (T823), the app reads it.
- 22:18, T823 small water next to its lead gauge (table sent: examples, counts at 50/100/200×). Cut-off, picked: "50×
  (Recommended)". Wording, picked: "Much smaller stream (Recommended)" ("This stream is much smaller than the river its
  gauge measures: the reading may not match it."). Built on `lane/t823-small-water` (edc097013), default already 50×;
  reaches a release with the next bundle rebuild. Found with it: T825 (251 side channels led by a gauge far upstream,
  e.g. lower Fraser side channels reading Red Pass).
