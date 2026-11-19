# Where things stand

**Start the review app yourself:** `bash curation-review/run.sh`, then open http://127.0.0.1:5173. It's local only and
spends no credits; Ctrl-C stops it. If Claude's copy is still running on port 8787, either use Claude's copy at
http://127.0.0.1:5183, or run your own beside it with `PORT=8788 CURATION_VITE_PORT=5174 bash curation-review/run.sh`.

Plain summary for the owner, updated as work lands. Newest at the top. Ticket numbers are in brackets for lookup only.

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
TestFlight with T918 (adds itself to the External group, sends itself to Beta App Review).

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
