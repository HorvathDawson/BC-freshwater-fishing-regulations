order: 180

## Decisions that block work (nothing ships until you answer)

- **ANSWERED 2026-11-17 15:04 (T825 side-channel gauges): "Yes, use the rule (Recommended)"; being built.** Was: Page:
  scratchpad t813-t825/T825-side-channel-gauges.html (245 sections on 9 rivers, grouped by river, each with the gauge
  it reads now and the one it would read). PROPOSED RULE: a side channel, slough or wide pool reads the nearest gauge
  on its own main river that measures flow and drains about as much country (a tenth to ten times); if the river has
  none, it reads none. 244 get a nearer gauge, 1 gets none (lower Sumas). Worth a look: 2 Kootenay channels near
  Creston would read Corra Linn across Kootenay Lake. No gauge data changed yet.

- **ANSWERED 2026-11-14 (production move changed).** Owner, verbatim: "Production will use the latest version" and "It
  will also get new dfo places I will label tonight". So production is NOT deployed from the 9048fbc20 copy below: it
  waits for tonight's DFO labels, then a new data release with them, then the website built from the latest checked
  code and that release. Claude: staging site redeployed from the latest code now; after the labels, a new staging
  release, then fresh production folders and fresh commands here. The commands in the next item are OUT OF DATE.
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
- **Production release: after 0.8.0's data release (owner 2026-11-17: everything lands first).** Fresh commands come
  here with that release id (`handoff/PLAN/release/PRODUCTION.md`, "Every later release"; the feeds worker first when it
  carries closed areas). The two older command blocks (r-f749475d0962, r-a178299f6005) were removed (T790 item 7).
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
