# Production-access questionnaire: draft answers (T1046)

The 8 questions of Play Console's "Apply for production" form, in form order (Google's page, read 2026-11-23:
https://support.google.com/googleplay/android-developer/answer/14151465). Each draft answer is built from facts in this
repo; the owner rewrites it in his own words before pasting. **TODO** marks every claim that needs a number or a pick,
with where to get it. No tester is named; the feedback rows are in `feedback-log.md` (F = tester / report / crash rows,
D = the developer's own finds, left out of tester numbers).

Refresh this page right before applying (counts move every day of the test).

---

## Part 1: About your closed test

### Q1. How easy was it to recruit testers for your app? (single choice)

**Draft pick:** TODO (owner): choose the form's option that fits. Facts for the choice:
- Testers came from the owner's own circle (friends and anglers he knows; T863: "a few friends to help me test"), by
  email list on the Closed track.
- The website and web app have a "Join the test" button (an email popup, T911, shipped 0.8.3) and "Join the iPhone
  test" (the public TestFlight link, T921).
- TODO (owner): how many people were asked versus how many opted in, and how long it took to reach 12.

### Q2. Engagement: did testers use all of the app's features? Did their usage match what you expect in production? How did it differ?

**Draft answer:**
Testers used the main parts of the app: the map, the regulations for a water (what you can keep, gear, licence), the
day picker, river conditions (gauge charts and forecasts), and the guided tour. Their feedback touches each of these
(`feedback-log.md`: regulations F02, F15, F16; map F03–F07, F25; conditions and gauges F08–F10, F12, F17, F20;
settings and photos F04, F11; location F13).
TODO (owner): features testers did NOT reach, if any (likely: saved spots with photos, gauge alerts, offline use away from
signal, Runs).

Usage matched what we expect in production in kind: people look up a water before or while fishing, check whether it
is open today and what they may keep, and look at the river's flow. It differed in two ways:
- **Season:** the test runs in late November and December, outside the main open-water season; most anglers fish
  spring to fall, so we expect more use per person, more lake searches and more salmon checks in production.
- **Place:** TODO (owner): where testers live and fish. The waters named in their feedback are mostly in the Lower
  Mainland, the Fraser Valley and the Sea-to-Sky; production users will be spread across B.C.
TODO (numbers): active testers per week and sessions (Play Console > Statistics, filtered to the closed track; TestFlight
> the build's Sessions); the share of testers who sent at least one report.

### Q3. Summarise the feedback you received and how you collected it.

**Draft answer:**
We collected feedback through four channels, each logged with its ticket and the release that fixed it
(`feedback-log.md`):
1. **In the app:** "Report a problem" sends the screen, the water and the tester's words to our server; crash reports
   arrive automatically (no personal data; see `handoff/PLAN/play/data-safety.md`).
2. **TestFlight feedback** from iPhone testers (screenshots with comments).
3. **Messages and in person:** testers sent the owner screenshots and screen recordings, which he passed on.
4. **Email:** the "Join the test" address.

No feedback came through Google Play's own testing-feedback form (owner 2026-11-24: "all was through the app
pathway"; testers did not know how to use the Play form), so the in-app report is the channel we pointed testers to.

What it said, in short (counts as of 2026-11-24; TODO refresh before applying): 16 pieces of tester feedback from at
least 7 testers, 9 in-app reports and 14 crash kinds. Most were about wording that was hard to read (gear lines, gauge
words, a sentence about missing gauges), map display (labels off their place, an iPhone map at two thirds width, a
highlight that did not stay, land closed to fishing hard to see, odd highlighting on a lake, ditches drawn as part of
a river), and a few behaviours (the day picker stopping at December, sub-tabs scrolling, "Flows into" opening the rules
instead of the map, the app not opening at the user's location, a slow first switch to Runs). Crash reports were mostly
early (0.4.0–0.6.x: data updates and map views) and are fixed; the late ones were mostly one search engine's page
renderer running the website. Testers also said the app was clear: one was positive overall on how much fits on a
screen while staying readable (F15). Of the 39 rows, 25 are fixed in a shipped release and 6 more in 0.9.0 (F15, F16,
F21, F22, F24, F25), which shipped on 2026-11-24 (Closed testing, build 44;
data r-0aec88ed3edb).

---

## Part 2: About your app (not shown publicly)

### Q4. Who is the intended audience of your app? Be as specific as possible.

**Draft answer:**
People who fish the fresh waters of British Columbia, Canada: residents and visitors with a B.C. freshwater fishing
licence, from beginners who do not know where to start to experienced anglers who want the rules for one water fast.
Typical uses: checking whether a river, stream or lake is open today, what may be kept (species, size, daily limit),
gear and bait rules, licence needs (Classified Waters), and what the river is doing (flow and level). Adults, mainly
(TODO owner: match the target age group set in Play Console > App content > Target audience).
Not for: tidal or ocean fishing (the app says where tidal water begins) or anglers outside B.C. (the app says plainly
that it is not useful there, T864).

### Q5. What is the value proposition of your app?

**Draft answer:**
B.C.'s freshwater fishing rules are spread over a rule book of more than 100 pages, region rules, water-by-water
exceptions, DFO salmon notices and in-season changes. Can I Fish This? puts them on a map: tap a water and it shows, in
plain words, what the rules allow there on the day you pick, with the rule book's own page one tap away. Beside the
rules it shows live river gauges (flow and level, compared with normal, with forecasts), salmon openings from DFO, and
in-season notices. The rules and maps work offline once downloaded, which matters on most B.C. rivers. The app is free,
with no ads, no purchases and no account (owner 2026-11-19). It is not the official regulation and says so.
TODO (owner): confirm "more than 100 pages" against the current synopsis edition, and the no-ads / no-account wording.

### Q6. How many installs do you expect in the first year? (range)

**Draft pick:** TODO (owner): pick the range. A basis, not a forecast: B.C. sells several hundred thousand freshwater
licences a year (TODO: the exact count from the province's licence statistics, source and year); website visits so far
(TODO: Cloudflare / Search Console numbers for canifishthis.ca). Choose a modest range; Google checks that it is
plausible, not that it is met.

---

## Part 3: Production readiness

### Q7. What changes did you make to your app based on what you learned during the closed test?

**Draft answer:** (each item names its `feedback-log.md` row; releases in `releases.md`)
- **Plainer words:** gear and methods lines rewritten (F02); gauge comparisons in five plain steps ("far lower than
  normal" … "far higher than normal", F10); the "% of this estimate" label and the no-gauge sentence rewritten (F08,
  F12); the Layers sheet reworded (F04).
- **Map display:** labels pinned to the place they name (F07); the iPhone map fills the screen again after tab changes
  (F06); one-section waters get a Map view and the tapped part stays marked (F05); land closed to fishing outlined so
  it shows on satellite and in light mode (F03); gauges drawn as shaded dots with the number (F09); a mouth label
  "Flows into …" at a river's end (F18); the map coloured at once when arriving from Conditions (F19).
- **Behaviour:** a "Show my location" prompt when the first location ask was dismissed (F13); the Trout / Salmon switch
  shows which is on (F14); the chart's date bar can be dragged (F20); regs sub-tabs jump instead of scrolling (F15);
  the day picker runs to March 31, 2027 when the rule book does (F16); "Flows into" opens the receiving water's map
  (F24).
- **Stability:** every crash kind from testing triaged; the early ones (data updates, map views, first install) fixed in
  0.5.0–0.6.9 (F26–F33); error reports that are not defects classed apart (F34); the production triage's app fixes in
  0.9.0 (F35).
- **0.9.0 (fixes for closed-test feedback; Closed testing 2026-11-24, build 44):** the Class II tag sits right under the water's name (F21); ditches no longer drawn
  as part of a river (F22, a data fix); Runs reads ahead so the first switch is quicker (F23, part); a selected slough
  or side channel is outlined once instead of boxed (F25); "Flows into" opens the receiving water's map (F24); the
  day picker runs to March 31, 2027 (F16); regs sub-tabs jump (F15); app fixes from the production crash triage
  (F35, part).

### Q8. How did you decide that your app is ready for production?

**Draft answer:**
- **Every change is checked before it lands:** each change goes through a pull request with four required automated
  checks (app, data pipeline, feeds worker, docs), and a second reviewer reads it before it merges (`AGENTS.md` W47).
- **Every release passes gates:** the release command stops on any failed check; smoke tests run against the live
  site and data after each release (18–19 checks, all passing on the last releases, `STATUS.md`); the phone builds
  are walked on an emulator (fresh install first) and benchmarked (0.8.3: 44 of 44 benches, opening a water in
  340–360 ms, pinch-zoom 55–57 fps).
- **Crashes are read every session:** a single command reads every crash and problem report, TestFlight and Play
  reviews (`python -m pipeline reports`); each new kind becomes a ticket (`feedback-log.md`).
  TODO (numbers): the Android vitals crash rate and ANR rate for the last 28 days (Play Console > Monitor and improve >
  Android vitals), and the crash-free share from our own reports for the build applied with.
- **The rules are checked against the rule book:** every rule shows its rule-book page; the owner reviewed the
  regulations water by water in a review tool before release (TODO owner: one line on how much was reviewed).
- **Testers' feedback is answered:** 25 of 39 tester, report and crash rows fixed in a shipped release, 1 more
  shipped and waiting on the owner's check, 6 fixed in 0.9.0, 3 in progress, 4 open and ticketed (`feedback-log.md`,
  2026-11-24).
- **0.9.0's numbers (2026-11-24, release r-0aec88ed3edb):** the release ran every check on its own data before staging: the
  app suite (9,471 tests passed, 17 skipped), the feeds worker suite (626 passed), the pipeline's no-data tier on a
  fresh copy of the code and its data tier on the release, the rule-by-rule binding diff against the live release
  (16 rules and 3 DFO places, every change the one intended fix), and the golden regulation pages re-cut (only the
  rule-book highlight boxes changed). The staging smoke test after the data and the site: 19 of 19 checks passed; the production
  smoke test after the production data and site: 18 of 18. The device pass (emulator, fresh install first, on the
  release build): two defects found and fixed before shipping (background checks that never ran under the code
  shrinker, T1027; a crash moving a frame to another water, T1034), then 9 of 9 flows and 5 of 5 background
  notifications passed; one known issue shipped and fixed in 0.9.1 (a cold-start tab link, T1049).
- **Policy:** the Data safety answers match what the app sends (`handoff/PLAN/play/data-safety.md`); privacy policy at
  canifishthis.ca (TODO owner: confirm the URL in Play Console); no ads; no account.

---

## Every TODO, in one list (for the owner)

1. Q1: the recruiting-ease pick; asked vs opted-in counts; time to reach 12.
2. Q2: features testers did not reach; where testers live and fish; active testers per week and sessions (Play Console Statistics, TestFlight
   Sessions); share of testers who reported.
3. Q3: recount before applying (owner 2026-11-24: no Play Console testing feedback came in; all of it came through
   the app).
4. Q4: the target age group as set in Play Console > App content.
5. Q5: the rule book's page count; the no-ads / no-account wording.
6. Q6: the installs range; B.C. licence sales (source, year); website visits.
7. Q7: done for 0.9.0 (shipped 2026-11-24).
8. Q8: Android vitals crash and ANR rates (28 days); crash-free share for the build; how much of the regulations
   was reviewed; the privacy policy URL; refresh the fixed counts.
