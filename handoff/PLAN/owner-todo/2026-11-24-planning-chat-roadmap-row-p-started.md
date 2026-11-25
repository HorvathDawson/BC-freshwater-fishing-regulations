order: 760

## 2026-11-24: planning chat (roadmap row P) started

- Answered 2026-11-24, the build-assets bucket over R2's 10 GB free tier (11.49 GiB; one pinned source file is
  9.7 GB): "yes compress it then we should have somethign mention hopw to rebuild it to update every so often too."
  T1089: compressed pins plus a runbook for refreshing the sources. Yours later: confirm the delete of the old
  uncompressed objects once the compressed ones are proven.
- 0.9.0 release, two answers given to the 0.9.0 chat and relayed by it (recorded here by the planning chat, which
  now keeps this file):
  - ~11:30, on the stalled production Android build: "you can run it for me". The 0.9.0 chat ran command 1
    (`build-apk --production`) for him, that time only.
  - 11:59: "okay i am leaving you to the release of everything. can i release new prod data now?" The 0.9.0 chat's
    reading (its own, not confirmed word for word by the owner): go for 0.9.0's remaining CONFIRM_PROD steps from its
    session (iOS TestFlight production build, site, feeds worker), this release only. The owner ran the data release
    himself. Done as of the relay: production data, site (smoke 18/18), feeds worker, Play build 44 on Internal and
    Closed; the iOS build was still building. This does not change the standing rule: CONFIRM_PROD is the owner's
    unless he says so that time.
  - The v0.9.0 tag was moved to 65f84020c (versionCode 44) on his earlier OK; the merge-back PR carries the Play
    record updates.
- Store checks 2026-11-24 22:20 (planning chat, every source read): TestFlight nothing new; Play reviews none. Four
  new items, all ticketed: production, a web page whose stored data was cleared (T1092) and the first crash kind on
  0.9.0, a location request with the phone's Location off (T1093); staging, one low-memory exit on 0.8.5 (T1094) and
  a feature request for fish measurements on a saved location (T1095, design first, 0.10 or later).
  Answered 2026-11-24 22:35 (multiple choice): the T1095 request came from "A tester"; design it in "0.10,
  pictures first"; iPhone 0.9.0 (45), which `pipeline reports` showed as READY_FOR_BETA_SUBMISSION (not yet with the
  external TestFlight testers): "Submit it". The 0.9.1 chat's real run was refused by its permission check (22:55), so
  this is yours: see the Do row below.
- Store checks 2026-11-24 13:30 (planning chat, every source read): TestFlight 6 items, all T982; Play reviews none;
  production worker: one new crash kind on 0.8.5 (T1055); the four production reports of 11-21 are T985's (ids now
  written there). About 40 old items (0.7.x–0.8.2, mostly staging) show as new only because no ticket cites their id:
  T1057 ties them up.
- Answered 2026-11-24: "no play closed testing feedback all was through the app pathway. users dont know how to use
  play one and I dont either." Recorded in the questionnaire (Q3: four channels, no Play form) and the feedback log.
- Answered 2026-11-24: the release table carries a short goal per release (ROADMAP-after-0.9.0.md, "Release contents").
- New from you 2026-11-24: iOS store distribution and compliance (T1071), better screenshots and marketing content
  before the full release (T1072), the app-update notice only when the phone can install it (T1073, 0.9.2).
- **Do (you), small, only after PR #155 (T1058) has merged: remove your machine-wide Gradle workaround.** Open
  `~/.gradle/gradle.properties` and delete the Metaspace line you added for the hang (it sets the same value the repo
  now sets and overrides it; `build-apk.sh` prints a note while it is still there). Nothing breaks if you leave it.
- Done 2026-11-24 (seen 23:10 by the iOS chat's reports read): iPhone 0.9.0 (45) is with Apple for the external
  testers, state WAITING_FOR_BETA_REVIEW. Nothing to do until Apple answers (usually a day).
- Answered 2026-11-24 ~23:05 (in the iOS chat, relayed; T1072): "we want the android and iphone images to use the
  same set ideally. if it is possible. we should generate new images too." Written into T1072 and the store content
  chat's prompt (roadmap row S): one new set from the 0.9.x app, the same frames for both stores.
- Answered 2026-11-24 ~23:25 (multiple choice in the iOS chat, relayed; recorded in T1071): App Privacy "Linked to
  you": "Strict: linked" (no code change; T1098 and T1099 follow it). Countries: "All 175 countries" (so the trader
  status below shows on EU store pages). Build for review: "Newest by then" (0.9.1's iPhone build if it is uploaded
  and push is proven on it when the pictures are ready, else 0.9.0 (45)). Push proof: "Soon, on build 45": your
  three push-key commands (the Do row below), then the phone steps with the iOS chat.
- Answered 2026-11-24 23:55 (multiple choice, planning chat): who builds and uploads the 0.9.1 iPhone build (nobody
  was assigned; your "Newest by then" pick needs it): "0.9.1 chat". It builds it right after its Play Closed build;
  the privacy page, privacy manifest and iPhone build-number fixes (T1098, T1099, T1101) are its lane too.
- Asked 2026-11-25 ~01:10: "the spot page needs rework still it still feels very clunky and I think we should have
  a full fresh look redesign maybe in 0.9.2" → T1115, pictures first; you will be asked what feels clunky, then pick
  a direction. And "we shouldnt allow notifications without new data notifications" → written into T1110 (my
  reading: no other notice can be on while new-data notices are off; confirmed with you when its words are shown).
- Asked 2026-11-25 ~00:50–00:55, three tickets, all proposed for 0.9.2 (say if any should be sooner): the first
  download asks before using mobile data and again if Wi-Fi drops (T1109; "this was feedback from one user"); the
  app asks on first load to turn on new-data notifications (T1110); the More page refreshes by pulling down (T1111).
  You pick the words of the two questions from pictures.
- Asked 2026-11-25 ~00:40: "free offline maps for corridors we could integrate mapbox mobile sdk with no navigation
  and it will be free until 25k users ... we could implement that in 0.10 ?" Ticket T1108, research first, with the
  corridors chat (roadmap 4c). You pick after the comparison page.
- 2026-11-25 00:40, your ask: "add boat luanch and docks etc etc to the basemap (will always be displayed) we will
  get this data from osm ... will go in 0.10 at min". Ticket T1119. Nothing for you yet: a page with counts and
  pictures comes first, then three picks (what "etc" covers, the look, whether a tap does anything).
- 2026-11-25 00:38, your addition to T1108: "in 0.10 when we add offline corridors we will switch mobile to use
  mapbox online satellite too and remove esri from mobile." Recorded on T1108; the web keeps Esri unless you say
  otherwise. It changes the privacy page, the store data answers and the credits; the research page lists them.
- 2026-11-25 00:40, your ask: at the repo move "do a fresh install ... to make sure everythign can be redone
  properly". Ticket T1120, the last step of the repo-move chat. Your part then: confirm a list of secrets (names
  only) that you can restore each one, about 10 minutes.
- 2026-11-25 00:50, from the 0.9.1 chat: (a) the What's-new words for 0.9.1 are yours to clear; it drafts plain
  ones as a proposal and lists them in its morning summary. (b) Production data: `pipeline release --prod` refuses
  in every Claude session by code. The planning chat will not run the upload script under it and advised the 0.9.1
  chat to leave production data, then the site and feeds, for you with the commands ready. (c) T1122: read out
  Play Console's Data Safety answers so they can be checked against the new privacy page (about 10 minutes, before
  the Play application). (d) The 0.9.1 chat says the release must run from the main checkout (clean, on `main`);
  the planning chat has nothing in it and told it: clean tree only, fast-forward only, no stash or reset.
- Asked 2026-11-25 ~00:20: "in 0.9.1 we should make this page (https://canifishthis.ca/gauges) have a search
  option ... search by place or river etc to get gauge only"; "someone found the page by search so we want it
  usable". Ticket T1107, sent to the 0.9.1 chat; you get a picture of the page with results before it ships.
- **One-time permission, 2026-11-25 00:30 MST (clock read; the planning chat's other "~" times of this night run up to an hour fast), typed to the planning chat, for the 0.9.1 production release only:**
  "i give you permission to create a one time workaround for prod release" and "i will be sleeping so i give you
  permission to run it". How the planning chat uses it (told to you the same turn): it runs only an exact command
  from `handoff/PLAN/runbooks/release-by-hand.md` that the 0.9.1 chat's permission check refused, one at a time,
  after the 0.9.1 chat says the step's gates passed; it stops at the first failure or surprise and leaves the rest
  for you; it edits no permission settings and wraps nothing; if its own permission check refuses, the step waits
  for you. Not covered: the App Store submission, the Play production rollout, anything outside 0.9.1. Every
  command it runs is listed here afterwards with its result. This ends when 0.9.1 is on production.
- Relayed 2026-11-25 ~00:50 by the 0.9.1 chat (your answers in its chat): the push token after Don't Allow: "that
  behaviour change can move to 0.9.1" (T1116); the one-bank stripe look: "A: cues above the stripe" (T1029); the
  gauges page: "should use mute color and text. also make the button for going to map look better and more
  asthetic. this page should still look professional" (T1107). And a HIGH find, on production now: the book's
  "ANDERSON" LAKE row (a lake in the Walbran, MU 1-3) is bound to Anderson Lake on Dove Creek near Courtenay, so
  that lake and Dove Creek wrongly show fly only, bait ban, single barbless and catch and release (T1117). Your
  answer: "Yes, fix it in 0.9.1"; it is fixed in PR #166. An audit of every such cross-unit name binding is T1118
  (0.9.2, first lane; you review its proposals).
- Relayed 2026-11-25 ~00:25 by the 0.9.1 chat (your words typed in ITS chat, not seen by the planning chat): "would
  you be able to walk through the full release tonight of everything?" and "i will give you permission this one
  time to do all prod commands needed". Its reading: for the 0.9.1 release only it runs the production steps itself;
  not a standing permission; anything its permission check refuses comes back to you as a command. The standing
  rule is unchanged for every other chat and release: CONFIRM_PROD is yours. Also relayed: Dove Creek (T1053), your
  pick "Yes, if it is the right lake"; the lane is checking the Anderson Lake binding before #166 leaves draft.
  What may miss the 0.9.1 cut because it waits on a pick of yours: the T1029 look, the T1098 privacy sentences,
  the T1107 gauges-page picture, the T1052 part-year waters. T1098 matters most: the App Store submission waits on
  the privacy page being live, so if it misses the cut it needs its own site deploy afterwards.
- **0.9.1 cut, 2026-11-25 02:12 (from the 0.9.1 chat):** all four handed reviews are in. What it chose, each a
  proposal you can change in 0.9.2: #166 the head-lakes list as built, the "ANDERSON" LAKE row moved to the unnamed
  Walbran lake, nothing reopens anywhere, 3,813 sections gain closed days, Frosst Creek the one own row kept; #112
  stripe look A; #187 the gauge card with the button and the muted words "Reporting now / Not reporting today / Not
  a real-time gauge", no dot; #179 privacy rows 1–9 as listed plus a row 10 on the push token (sent only while
  notifications are allowed and one is on; deleted after 30 days unsent). Not in 0.9.1, moved to 0.9.2: T1061
  (Skeena blank band), T1093 (location crash kind), the part-year closed waters (now T1126).
- **Picks owed to you in 0.9.2, with pictures (nothing to do now):** T1124 the push line when notifications are
  blocked in the phone's settings (today it says "no notification is on", which is untrue then); T1125 what the map
  says for a bank place that cannot be drawn (Nakina) and for creeks whose side could not be read; T1126 how a
  part-year closure reads as one statement.
- **Handed to the 0.9.1 chat, 2026-11-25 00:50** (your words to the planning chat: "I told the agent they could do
  these without me"): reading the head-lakes list in draft #166, picking the privacy sentences in #179, checking the
  gauges search picture in #187, picking the one-bank stripe look in #112. None of the four waits on you for the
  0.9.1 cut any more. The 0.9.1 chat lists what it chose for each in its morning summary, as proposals you can
  change in 0.9.2 (planning chat's ask of it, not your words). The rows below about these four are kept for the record.
- **Read (you): the head-lake walk, draft PR #166 (T1053)**, sent to you by the 0.9.1 chat ~23:55. The list:
  the file head-lakes-before-after.md in that PR's T1053-shots folder (22 head lakes, 42
  rules gain water, none lose). One decision inside it: Dove Creek above Anderson Lake reopens for catch and
  release (the Tsolum River's all-year tributary closure gives way to Anderson Lake's nearer tributary row); the
  0.9.1 chat is asking you. Your ponds answer (Q124) is built as a test in it; that line closes when #166 merges.
  Two things it does not do are tickets now: Technical detail naming the rule that gave way (T1105), and whether
  DFO places and licensing should climb head lakes too (T1106: a list first, then your ruling).
- **Decide (you), before the App Store submission; the iOS chat will bring both up** (its read of build 45, 11-24):
  - DSA trader status (App Store Connect → Business → Compliance): a legal statement about you; only you can make it.
  - Content Rights, "I have the rights": the map's satellite layer is Esri World Imagery used without a key. Whether
    that fits Esri's terms is your call before you tick the box.
  - The privacy page has sentences the app does not match (T1098): you read the before/after list before it goes up.
- Done 2026-11-24 ~23:30: you set the three push keys on the production feeds worker. The proof itself failed:
  your iPhone (0.9.0 (45)) is given no push token by the system, on Wi-Fi and on cellular (T996; the same as build 36
  on 11-16). Signing and the app's push code were checked and no fault found; no real iPhone has registered yet.
- **Do (you), AFTER the iPhone release (your call 2026-11-25 ~00:20: "we should move this to after iphone release
  I think. we could fix with a patch?"): find why the iPhone gets no push token** (T996). Your Console log of 11-25 points to
  the phone, not the app: that spare iPhone's push service holds no token at all. What is left is one step: install
  the same TestFlight build on a second iPhone and try there. The App Store submission no longer waits on this; it waits
  on the new pictures (T1072) and the privacy page words (T1098) only. Until push works, an iPhone learns of new
  data only through iOS's own background check, which can be late.
- Reported 2026-11-24 22:55 (0.9.1 chat): a network outage stopped its lanes 16:15–22:15, all restarted, nothing
  lost. T1087 (the Brunette closure on the whole river) was read against the printed row: right, nothing to change.
- **Do (you): Play Production staged rollout of 0.9.0 (build 44), once Google grants production access** (T1046,
  T240). Build 44 is on Internal and Closed since 2026-11-24 (Google's review of the Closed release pending). Play
  will not let Production be used until the production-access application is approved; the application opens
  2026-12-07 and this does not move that date or the Closed test clock. Then, in Play Console: Test and release →
  Production → Create new release → Add from library → pick build 44 (or the newest build then on Closed) → release
  notes → Next → set the rollout percentage (start at 20%) → Save → Publishing overview → Send for review. Raise the
  percentage from the same Production page once the crash numbers look clean.
- **Do (you): App Store submission of 0.9.0** (T1071), build 0.9.0 (45), uploaded to App Store Connect 2026-11-24
  ~15:30 from release/0.9.0 65f84020c (TestFlight shows it after Apple's processing). Before you click, T1071's checklist has
  to exist (App Privacy, export compliance, age rating, content rights, review notes): it is not written yet, so this
  row waits on the iOS release chat, AND on the new picture set (T1072, store content chat S): you chose "Wait for the
  new set" 2026-11-24 23:06. **Start chat S now** (prompt: roadmap row S). Then, in App Store Connect: Apps → Can I Fish This? → Distribution → the "+" by
  iOS App → version 0.9.0 → Build → pick the build → paste the listing text and pictures (T1072) → App Privacy and
  the other answers → Version Release: "Manually release" and Phased Release on → Add for Review → Submit.
- Answered 2026-11-24: the owner to-do goes markdown only, no artifact (T1080); and a ticket to audit and lean out
  every markdown file and unify the docs (T1081, proposed chat D after 0.9.1).
- Answered 2026-11-24 (multiple choice in chat): next chat after 0.9.1 = "iOS full release first"; approved chats =
  0.9.2 hardening, Store content, iOS full release, Docs lean-out (all four); rc tags (T1062) = "With the repo-move
  chat"; the old owner to-do history (T1080) = "Delete it". The order and each chat's prompt to paste are in
  ROADMAP-after-0.9.0.md.
