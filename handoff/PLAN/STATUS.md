# Where things stand

**Start the review app yourself:** `bash curation-review/run.sh`, then open http://127.0.0.1:5173. It's local only and
spends no credits; Ctrl-C stops it. If Claude's copy is still running on port 8787, either use Claude's copy at
http://127.0.0.1:5183, or run your own beside it with `PORT=8788 CURATION_VITE_PORT=5174 bash curation-review/run.sh`.

Plain summary for the owner, updated as work lands. Newest at the top. Ticket numbers are in brackets for lookup only.

---


## 2026-11-22 13:40: build/CI streamlining chat finished (no release)
- `main` is now the protected trunk: every change is a pull request with four checks, and it merges itself when they pass.
- Version numbers now follow the commits (SemVer). The next app release is 0.9.0; it waits for the next chat's work.
- One release runbook for you: `handoff/PLAN/runbooks/release-by-hand.md`, plus a page explaining every release stop.
- Android builds use one pinned MapLibre commit instead of a patched copy (T978); test suites faster (T866); docs match
  the code (T987).
- Production steps are yours only (you, 2026-11-22). Claude prepares and checks, then gives you the command.
- New tickets from your finds today: T1003 + T1009 (Play's code-optimization score), T1007 + T1008 (website Lighthouse),
  T1010 (region page map link), T1011 (156 waters crossing the border have no web page; old lake names not on pages).
- Your list of 100 pages to submit to Google: `handoff/PLAN/seo/index-requests-top-100.md`.

## 2026-11-21 19:45: 0.8.5 released (data, website, feeds worker, Play Internal + Closed, TestFlight)

- **Production data:** r-741815c6090d. Packed formats only, min app 0.8.4 [T686 T975]. It passed staging smoke 19/0.
  - The `water` size gate was raised to 2.85 MB for the mouth's place [T985 #2].
  - Lost Lagoon is closed under the new local-closures source [T981].
- **Production website:** 0.8.5 on r-741815c6090d. **Production feeds worker:** redeployed, writing the new band words.
- **App 0.8.5:**
  - Android build 43: Play Internal and Closed testing.
  - iPhone build 44: TestFlight internal; external is waiting for Apple's beta review.
  - Emulator walk: T989, T988, T990, T985 #1/#3 and T984 all PASS. T988 was not reproduced.
- **What shipped:**
  - one Sources button at the foot of the regs page [T990];
  - plainer band words and the no-gauge sentence [T982];
  - ended in-season notices no longer come back as New [T983];
  - a web link opened in the app goes back to that water [T989];
  - the guided tour only walks [T984];
  - the mouth label "Flows into …" on the water map [T985];
  - the species switch state [T986];
  - a bigger water map on phones [T979];
  - smaller data downloads [T686].
- **New tickets, next release:**
  - sources display audit [T991];
  - explain the golden pages and release gates [T992];
  - a release, CI and CD runbook and cleanup so a human can release [T993].
  - The competitor list is `handoff/PLAN/COMPETITORS.md`.

## 2026-11-21 10:40: 0.8.4 released (data, Play Internal, TestFlight, website)

- **Production data:** r-fec3fab4c5aa (published 2026-11-20, min app 0.8.0, old formats). It passed staging smoke 18/0.
  Three follow tests were skipped to ship [T980].
- **App 0.8.4:** Android build 42 is on Play Internal. iPhone build 43 is on TestFlight internal; external is waiting
  for Apple's beta review.
- **Website:** canifishthis.ca serves r-fec3fab4c5aa.
- **What shipped:**
  - the Classified Waters carried unit with its "why" fold [T973];
  - dead-end creek licensing [T944];
  - the map rebuild: #1 outside B.C., #6 names, #8 gauge walk [T679];
  - names [T926];
  - salmon region labels [T669];
  - iPhone fixes [T723 T760 T836 T938];
  - review leftovers [LF].
- **New tickets:** T977 to T982. Tester feedback is read by `deploy/staging/testflight-feedback.mjs` [T982].

## 2026-11-19 22:04: the batch landed; staging and production websites live from 1fd3e94f0

- Landed on redesign/stream-sections (pushed): the Conditions card redesign [T920], the website page build 1,136 s →
  470 s [T917], "Join the iPhone test" opens the public TestFlight link [T921], two 0.8.3 leftovers [T919 a, b].
  Android benches 44/44 (same speed as 0.8.3); dark checks pass; the white map box was not reproduced (a fix is noted).
- The full test tier was red, but not from this batch: the live-data tests had not run since the 11-15..11-19 rulings, so
  ~55 still expect the old wording or counts. Ticket [T931]; the one GitHub-tier failure (the TestFlight message test,
  from T918) is fixed.
- Staging website: deployed, smoke 19/19. Production website (owner go "Cna do prod website too"): deployed from the same
  commit on release r-6258ffded676, production smoke 18/18. Production data, workers and phones unchanged.
- Not touched: Play Closed testing (stays until Google approves build 39) and Apple External (build 41 in review).

## 2026-11-19 night (one chat): Conditions redesign approved; a landing batch on lane/t920-batch; tester feedback ticketed

**Owner-approved, waiting on its checks (branch `lane/t920-batch`, not yet on redesign/stream-sections, nothing pushed):**
- T920 Conditions gauge card (round 3, owner "This looks good"): the map under the chart; a taller chart; Flow | Level and
  72 h | Year in one row above it; the legend's forecast toggles as before; no "Data release" line (the provenance code
  removed); the gauge list a closed fold "N gauges behind this estimate" with two lines per gauge; a bordered gauge picker.
  Review fixes in (chart 1:1 on 360-375 dp phones, store-video checks).
- T919 (a) a tributaries row named once, (b) Elaho p.23 not twice. T921 "Join the iPhone test" opens the TestFlight invite.
- T917 merged into the lane: website page build 1,136 s -> 470 s, byte-identical (reviewed, fixes in).
**Running:** full tier (lane 1, started before the review fixes: rerun Tier 1 on the tip after), Android benches + Conditions
checks on the emulator (lane 2), iPhone simulator investigation of T928/T929 (lane 3). Land = fast-forward
redesign/stream-sections to the lane once all pass, then push.
**New tickets (tester via owner):** T922 Gear/Methods run-on lines + "Roe", T923 reserve/private land outline, T924 Layers
wording, T925 Map view for one-section waters, T928 iPhone map 2/3 width after tab changes, T929 water-map boundary labels
stay in screen space on pan/zoom; (owner) T926 search name forms (measured: abbreviations, joined book names, First Nations
names need a source), T927 search finds the 477 placed boundary places.
**Owner decisions open:** T919 (c) map files on first install (default one packed download); T926 (4) First Nations names
source (default BC Geographical Names). Both on the To-Do page.
**Tester reports:** none received yet (production 0, staging only test-bed ones); the tester restarted before reporting.

---

## 2026-11-19 evening: 0.8.3 is on production (web, data, workers, Play closed); iPhone 0.8.3 sent to TestFlight

**What changed for anglers (0.8.3):** smoother pinch-zoom and zoom-out (T903: +61 % / +78 % frames on the stressed
bench); a data update keeps downloading after the app is closed (T912); a simpler trout table, one value per fish, with
a quiet tributary note (T907); gauge pages that answer "how is the river now" (T908); a steady day calendar and a
desktop popup (T915); the rule-book link goes to the water's own page, at the page foot (T916); desktop Conditions keys
as cards (T904); no bell or reload on the web (T909); "Join the test" popup (T911); privacy wording and a faster first
web load (T910, SEO 91 -> 100, 9.4 -> 6.7 MiB).

**Where it is.** Data r-6258ffded676 on staging and production (owner ran `release --prod`; prod smoke 18/0). Feeds
worker and data worker on both (prod: 0a3205f6, 3c598244). Production website canifishthis.ca from 32a56ab20 (clean,
release r-6258ffded676; smoke 18/0); staging website 09985e595 (19/0). Play: Closed = 39 (0.8.3 production, replaced
staging 38 on the owner's pick, so Google reviews 39), Internal = 40 (0.8.3 staging). Android benches before the builds:
44/44 (open 340-360 ms, pinch 55-57 fps). iPhone: 0.8.2 build 39 approved for External; 0.8.3 production build to
TestFlight with T918 (adds itself to the External group, sends itself to Beta App Review). Done 16:26: build 41 is in "testers ext", WAITING_FOR_BETA_REVIEW (run by hand: the build had started before T918 merged; Apple wanted `whatsNew`, fixed).

**New tickets:** T917 (website page build ~17.6 min; baseline measured: 3.4 of 18 cores, 31 % of each render thread in
synchronous compression, 153 s of share PNGs on the main thread), T918 (done here, its first real run is the 0.8.3
upload), T919 (0.8.3 leftovers). Fixed on the way: PRODUCTION.md's production web export lacked T910's font shell.

---

## 2026-11-19 morning (one chat, overnight): 0.8.2 with everything is on staging; production is yours after you test

**What is on staging.** Data release r-379e72fc9524 (LATEST; staging smoke 18 pass, 0 fail), the staging website
(https://staging.canifishthis.ca, app commit 3ab2b1b24, smoke 19 pass, 0 fail), the staging feeds worker (redeployed
with the salmon changes) and Play INTERNAL 0.8.2 build 37 (staging data, patched MapLibre; the tester email went out).
Nothing went to production.

**What an angler sees that is new tonight.**
- Salmon: the yearly line says "10 adult chinook a year from all fresh waters", and where an adult is bigger on that
  river it says so ("(here an adult is over 62 cm, Sep 1–Dec 31)" on the Fraser) [T869]. The card's Source (DFO) lists
  every DFO row behind its answers; each DFO gear line says it is DFO's; DFO's and B.C.'s bait bans are one line; a
  whole-year DFO row reads "(all year)" [T870].
- What you can keep: a trout row whose number comes from another water's rule for its tributaries says so under the
  fish ("Duncan River’s rule for its tributaries"), on the app and the website [T892].
- Gear and Methods: sources once per section, the sturgeon exception once, the banned ways on one line (your 03:25
  approval) [T900].
- The map: a braided river's side channels draw at half the main channel's width (new tiles) [T891 part one].

**Checks.** Full tier green (pnpm check 7,812 tests, pytest 7,724, mini release); Android benches 5/5 (pinch ~57 fps,
nothing over 100 ms); an independent review (no blockers; five fixes made and re-checked); the release's own tests on
the candidate caught the website missing the new trout line, which was fixed with a cheap check added so it fails
earlier next time.

**Did not make it.** The map speed-up (paint only what differs) [T903]: under a stressed phone mode switches were
11–22 % faster but panning on Salmon got worse, so it stays out; numbers in the ticket. Store screenshots [T319]: not
uploaded (permission block). Braid simplification [T891 part two] waits for you.

**What needs you.** OWNER-TODO "2026-11-19 morning": what to test (with links), six questions with defaults, and the
production order (feeds worker FIRST, then data, phones, and your go for the website).

---


## 2026-11-16 morning (chat 14): your labels are on staging, build 33 is on Play internal, the audit is in

**What changed.** Your DFO labels are built into the app data and released to staging (release r-46e4355a7277; the
staging site is rebuilt on it; smoke check 19 pass, 0 fail). The app chat redeployed the staging feed server and put
0.7.11 (build 33, code shrinker on) on Play internal testing at 04:37. Production is untouched.

**What is in the release that an angler sees.** 194 DFO places moved since the last release; 13 new cuts; four closed
areas drawn (Shuswap, Thompson at Deadman, Juniper and Ashcroft); "everything else" places now answer after the
stretches they list, on the days those have no rule of their own (Skeena sockeye above Terrace, Somass, Stamp, Little
Qualicum, Nitinat, Dean) [T761]; the trout "through the year" line says when keeping returns [T762]; Kootenay Lake's
rainbow line says "over 50 cm" [T763].

**What it means.** Everything on staging is for you to check. The saves I made for you overnight are marked "unsure"
(four closed areas and four others stand as "verified" in my name): the list to re-tick, with the questions, is ONE
list in `handoff/PLAN/OWNER-TODO.md`, section "2026-11-16 morning list" (also on the To-Do page).

**The checks you asked for.**
- Skeena cascade, on the built result: 11 of 13 checks pass. To decide: Babine Lake reads open in August inside 13
  closed areas that are not confirmed yet; Gosnell Creek and the Thautil read closed by the Morice row; Gitnadoix coho
  reads open below the powerline; the eulachon line is missing on the Lakelse [T781].
- General audit (five checkers, read-only; `handoff/PLAN/audit-2026-11-16/`): no code defect that changes an answer today (97 commits read); all 309 labelled places
  are stored as your last save says and no place reads open against its own DFO row; 13 of about 150 decisions were
  lost, half-done or recorded wrongly (now ticketed, T786 to T790); the release took 105 minutes over nine attempts
  and about 60 were repeat work (a smaller plan waits for your yes, T791).
- PRODUCTION BLOCKER found by the audit: the production upload will refuse this release until the production feed
  server serves closed areas; that code is on the app chat's side branch, which waits on a merge question you were
  never shown (morning list, question 19).

**What needs you.** The morning list: re-tick the saves, then 28 questions with a recommended answer each (labels
first). The trout page (round 6, mockups only) has four more. The ten-step phone walk of build 33 is on the To-Do
page.

**Trout display.** Six rounds of mockups on one page; nothing is built. Round 6: one line above the table, a fish
opens as a sheet, the app's year bar where dates change, sources under the table and in each sheet.

**The audit's most important finding (answers against the printed rule book, about 98 waters).** No wrong number,
size or date. But seven things the app allows that the sources forbid, none new in this release: wild-trout limits
missing from the keep card on at least 15 lakes and on the Kitimat (first to fix, T793); the Peace just below
W.A.C. Bennett Dam reads open (T794); "No angling from powered boats" is never shown (T797); a stretch the book
closes can still show an open salmon card, and a DFO "Finfish closure" closes salmon only (T796, your call); Coal
Creek's licence class (T798). And Fraser Lake reads closed all year because a reserve touches 0.1 % of it (T795).

**Running.** Nothing. Everything is committed and pushed.

## 2026-11-15 night (chat 14, in flight at 23:26): labels double-checked and fixed, new map promoted, release not yet out
- All 352 DFO places were read against DFO's words (273 clean, 79 flagged, 15 looked wrong); the fixes you asked for are
  saved for your re-tick (mark lines 2770-2794) and landed. One of your Tatshenshini saves had stopped the build; fixed.
- New map promoted locally with 13 new cuts (Skeena confluences and Ferry Island bridge, Nanaimo, Kasiks, Shuswap,
  Thompson incl. Juniper). The rest-of extent was rebuilt (T761: own end points, listed places, "areas and times") and
  every leftover-worded place uses it. Two display errors fixed for this build (T762 year line, T763 Kootenay rainbow).
- Not done yet: the app data build is running; then the staging release, the Android build of 0.7.11 (the app chat),
  Skeena cascade part 2, the general audit. Trout display mockups: https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH
  (round 3 running; exploration only).
- State and next steps for a new or compacted chat: `INFLIGHT-chat14.md`. Your questions: OWNER-TODO.md
  "2026-11-15 night" sections and the To-Do page rows starting `sl-`.

## 2026-11-15 (chat 13 end): DFO labelling done; clean-up, side rebuild, review-app fixes; handed to a new chat
- Owner finished labelling the DFO places (2,769 mark lines). Restart page: `NEXT-SESSION-after-chat13.md`.
- Landed: review-app fixes and follow-ups (T755), marks audit command, closed-area build step (T573), lake loops
  (T753), desktop split map (T752), Kotcho/Seton/dead-end walk fixes, 23 new cuts + cleaned duplicates; atlas, tiles
  and reach promoted locally; bundle and LATEST still r-f749475d0962. Staging: worker serves closed-area pieces and
  the region-default sentences (T757, T758).
- Not done: final audit of the labels, one more rebuild with tonight's cuts, the staging release, Play build 33 (R8,
  0.7.11; six benches passed, merged, build not run), Skeena cascade review, general audit, T759 / T754 / rest-of /
  wrong-mark rule builds.

## 2026-11-14 — build 31 (0.7.9) on Play internal; five phone fixes waiting for the next build; privacy page reworded

- **Out (11-12 to 11-14):** release r-f749475d0962 is live on staging (after r-adc4e248a163 and r-a9914d6b25b7 on 11-13:
  tidal and junction fixes, Whiteswan inlets, Gagnon/Lakelse/Kootenay wording, seven silent-gauge answers). App 0.7.8
  (build 30) and 0.7.9 (build 31, faster tap-to-open) are on Play internal.
- **Fixed after your phone reports, committed, not in a Play build yet:** back from a water's page no longer flashes
  [T717]; a Flow/Level pick in the card no longer changes the Conditions map [T728]; the credits card opens above the
  legend [T729]; search closes with a swipe down, keeps 4 recents, leaves the map where it was moved [T730]; stepping up
  or down a water keeps the bar [T731]. A build lane for 0.7.10 was running: the newest entry in
  `app-integration/T678-progress.md` says whether it uploaded.
- **Privacy page:** reworded 2026-11-14 as you delegated (staging site, smoke 19/19). One owed fix: it names the crash
  switch at More > Settings; the real place is More > Settings > Privacy. The Play form location answer needs your word.
- **Production move is yours:** a clean copy of the site is built in `~/bcfish-clean-staging`; leave it alone until you
  have deployed (`release/PRODUCTION.md`, steps 3-8). Then check https://canifishthis.ca/privacy.
- **Tests and iOS:** the test-making work reached the invented mini release (T667). The iOS chat is working in the
  simulator (T718-T727); your Apple Developer enrolment is waiting for Apple. T732 (backup keeps saved spots first) is
  not merged.
- **Filed:** T717, T728-T731 (above), T732. **Next free ticket: T733.**
- **Next:** 0.7.10 result and your phone check; you run `/code-review ultra`; fixes; final build and one full suite;
  DFO labelling (yours); Play closed testing. Page for a new chat: `NEXT-SESSION-after-chat12.md`.

---

## Orphan lines (T539)

Five lines that sat above this file's title, head of an older entry (T401 measure; test-bed gestures / water-nav), moved here whole.

- **T401 measured (emulator):** drag, fling, and fling close in (Hoy Creek) — 0 % of the new edge missing once the
  camera rests (~0.65 s drag, ~1 s fling). Not reproduced; asked the owner for a screen recording of one pan.
- **Test bed green on the 0.5.0 drift:** gestures (route map is above the gauge list), water-nav (the tap used the small
  water map's probe; now the tallest map's) — T345's last path (close → reopen → Map) confirmed, no crash; T399's
  highlight on the river (r1105d).
