# Tester feedback log (T1046)

One row per piece of feedback, report or crash kind since testing began (Play internal from 0.4.0, 2026-10-31; Play
Closed from 2026-11-19; TestFlight from 0.7.10). Each row names the ticket that took it and the release that fixed it,
so the production-access form can show the work (`questionnaire-draft.md`). Rules and upkeep: `README.md`.

**Public repo: keep it anonymous.** The tester column is a tag, never a name, email, install id or report id. Summaries
are short and neutral, in our words; no quotes that identify anyone (the tickets hold the detail).

## Columns

- **ID**: `F` + number, never reused.
- **Date**: when the feedback arrived (or the report / crash was read).
- **Channel**: `Play testing feedback` (Play Console, no API: the owner reads it) / `TestFlight` (read by
  `.venv/bin/python -m pipeline reports`) / `in-app report` (Report a problem, via the feeds workers; `prod` or `staging`
  worker) / `crash report` (automatic, via the feeds workers) / `email` / `in person` (told to the owner, or sent to him
  in a message; he relays it).
- **Tester**: an anonymous tag. `T-` = a person who told the owner; `TF-` = a TestFlight tester; `CT-` = a Play closed
  tester; `R-` = in-app reports (reports minutes apart from the same platform and version share a tag);
  `auto` = automatic crash reports (many installs). Two tags may be the same person when the source does not say.
- **Version**: the build the feedback was about, where known.
- **Status**: `open` / `in progress` / `fixed` / `fixed, owner read pending` (shipped, but the ticket's acceptance
  read is the owner's) / `declined` (the owner's call, with why in the ticket). A row whose ticket or follow-up ticket
  is still open counts as `in progress`, even when part of the fix shipped.
- **Fixed in**: the app version (or data release / worker deploy) that shipped the fix; `0.9.0 (pending)` while it is
  on `main` but not released.

## Tester feedback (people)

| ID | Date | Channel | Tester | Version | Summary | Ticket | Status | Fixed in |
|---|---|---|---|---|---|---|---|---|
| F01 | 2026-11-06 | in person | T-1 | 0.6.x Android | Guided walkthrough: a step's card covered the control it described, and a step's words no longer matched the screen | T456 | fixed | 0.6.8 |
| F02 | 2026-11-19 | in person | T-2 | 0.8.x | Gear and Methods lines read like one run-on sentence; the bait "roe" line was worded oddly | T922 | fixed | 0.8.4 |
| F03 | 2026-11-19 | in person | T-2 | 0.8.x | Land where fishing is not allowed (reserves, private land) was hard to see on satellite and in light mode | T923 | fixed | 0.8.4 |
| F04 | 2026-11-19 | in person | T-2 | 0.8.x | Layers sheet wording read oddly; asked whether satellite imagery works offline | T924 | fixed | 0.8.4 |
| F05 | 2026-11-19 | in person | T-2 | 0.8.x | A water with only one section had no Map view, and the tapped part's highlight did not stay | T925 | fixed | 0.8.4 |
| F06 | 2026-11-19 | in person | T-3 | 0.8.3 iPhone (TestFlight) | The main map filled only two thirds of the screen after changing tabs | T928 (follow-up T938) | in progress (fixed in 0.8.4; follow-up T938 open) | 0.8.4 (part) |
| F07 | 2026-11-19 | in person | T-3 | 0.8.3 iPhone (TestFlight) | Water map: boundary labels did not line up with the places they name, and the part looked mis-framed | T929 | fixed | 0.8.4 |
| F08 | 2026-11-20 | TestFlight | TF-1 | 0.8.3 iPhone | "% of this estimate" on a gauge card was unclear | T982 #1 | fixed | 0.8.5 |
| F09 | 2026-11-20 | TestFlight | TF-1 | 0.8.3 iPhone | Gauge percentage boxes on the map: suggested a gauge symbol instead (owner picked a shaded dot with the number) | T982 #2 | fixed | 0.8.5 |
| F10 | 2026-11-20 | TestFlight | TF-1 | 0.8.3 iPhone | "Much below normal" style wording: suggested plainer words (owner picked "far lower / lower / normal / higher / far higher") | T982 #3 | fixed | 0.8.5 |
| F11 | 2026-11-20 | TestFlight | TF-1 | 0.8.3 iPhone | Spot photo buttons: suggested icons (owner picked icon plus one word) | T982 #4 | fixed | 0.8.5 |
| F12 | 2026-11-20 | TestFlight | TF-1 | 0.8.3 iPhone | The sentence shown when no gauge reads a water was hard to understand; asked for plainer language | T982 #5 | fixed | 0.8.5 |
| F13 | 2026-11-20 | TestFlight | TF-1 | 0.8.3 iPhone | The app did not open at the tester's location (a dismissed first location ask was never offered again) | T982 #6 | fixed | 0.8.5 |
| F14 | 2026-11-21 | in person | T-4 | 0.8.x | The Trout / Salmon switch did not show clearly which one was on | T986 | fixed | 0.8.5 |
| F15 | 2026-11-22 | in person | CT-1 | 0.8.4–0.8.5 Android | Switching regulation sub-tabs (keep ↔ licence) scrolled through every section in between. Same message: positive overall on how much fits on a screen while staying readable | T1025 | fixed | 0.9.0 |
| F16 | 2026-11-22 | in person | CT-2 | 0.8.4–0.8.5 | The day picker stopped at December 2026 though the rule book is valid to March 31, 2027 | T1026 | fixed | 0.9.0 |
| F47 | 2026-11-26 | email | CT-3 | 0.9.0 Android | Zoomed out, the map shows almost no lake or river names, so a particular water is hard to find. Same message: positive overall (everything for a river system in one place; the rules looked accurate on the rivers checked; no bugs met) | T1200 #1 | open (options as pictures first) | 0.10.0 (pending) |
| F48 | 2026-11-26 | email | CT-3 | 0.9.0 Android | Asked for a few more main towns on the zoomed-out map, as landmarks | T1200 #2 | open (options as pictures first) | 0.10.0 (pending) |
| F49 | 2026-11-26 | email | CT-3 | 0.9.0 Android | The tapped water does not stand out enough; suggested a colour change to accent it | T1200 #3 | open (options as pictures first) | 0.10.0 (pending) |
| F50 | 2026-11-26 | email | CT-3 | 0.9.0 Android | The many lines on the zoomed-out map (read as roads) made it hard to tell what everything is | T1200 #4, #5 | open (options as pictures first) | 0.10.0 (pending) |
| F51 | 2026-11-26 | email | CT-3 | 0.9.0 Android | A water's regulations page is accurate but a lot at first glance; suggested very distinct headings or a slight colour change between sections (less so on a second look) | T890 #4 | open | 0.10.0 (pending) |
| F52 | 2026-11-26 | email | CT-3 | 0.9.0 Android | Another map app the tester uses is easier on the eyes and shows the water more clearly (but has no labels); the tester finds a lake by scrolling and zooming, not by search | T1200 #6, #7 | open (options as pictures first) | 0.10.0 (pending) |

## In-app reports (Report a problem)

| ID | Date | Channel | Tester | Version | Summary | Ticket | Status | Fixed in |
|---|---|---|---|---|---|---|---|---|
| F17 | 2026-11-21 | in-app report (prod) | R-1 | 0.8.4 Android | The gauge chart flickered when switching forecast model on first load | T985 #1 | fixed | 0.8.5 |
| F18 | 2026-11-21 | in-app report (prod) | R-2 | 0.8.4 iPhone | No downstream label on a river's Map view (now "Flows into …" at the mouth) | T985 #2 | fixed | 0.8.5 |
| F19 | 2026-11-21 | in-app report (prod) | R-2 | 0.8.4 iPhone | Map uncoloured after going to it from Conditions until a pan or zoom | T985 #3 | fixed | 0.8.5 |
| F20 | 2026-11-21 | in-app report (prod) | R-2 | 0.8.4 iPhone | Chart gestures: no way to drag the date bar; zoom and pan got in the way (owner picked a drag handle plus touch-and-hold) | T985 #4 | fixed | 0.8.5 |
| F21 | 2026-11-22 | in-app report (prod) | R-3 | 0.8.x | The Class II tag sat far right of the water's name and was easy to miss | T1004 | fixed, owner read pending | 0.9.0 |
| F22 | 2026-11-22 | in-app report (staging) | R-4 | 0.8.5 Android | Ditches drawn as part of a river's main stem in salmon mode | T1012 | fixed | 0.9.0 data (r-0aec88ed3edb) |
| F23 | 2026-11-22 | in-app report (staging) | R-5 | 0.8.5 Android | Switching to Runs took a very long time the first time | T1013 | in progress (two cuts in 0.9.0; the timing read is left) | 0.9.0 (part) |
| F24 | 2026-11-23 | in-app report (staging) | R-6 | 0.8.5 Android | "Flows into …" should open the receiving water on its map, not its regulations | T1023 | fixed | 0.9.0 |
| F25 | 2026-11-23 | in-app report (staging) | R-7 | 0.8.5 Android | Odd highlighting on a lake (not reproduced on the web yet) | T1024 | fixed, owner read pending | 0.9.0 |
| F54 | 2026-11-26 | in-app report (prod) | R-9 | 0.9.0 | A feature ask: no "Report a problem" on a saved-spot page, and the layout and flow for a spot visited more than once | T1197 | open (design first) | — |

## Crash reports (automatic)

Each row is one crash kind, or one triage of many kinds. Counts and stacks are in the tickets.

| ID | Date | Channel | Tester | Version | Summary | Ticket | Status | Fixed in |
|---|---|---|---|---|---|---|---|---|
| F26 | 2026-10-31 | crash report (staging) | auto | 0.4.0 Android | A tap on the map from a river's "flows into" link crashed the app | T345 | fixed | 0.5.0 |
| F27 | 2026-11-02 | crash report (staging) | auto | 0.4.0 Android | A native crash in the foreground with no stack (memory pressure suspected) | T351 | fixed (none since) | 0.6.9 |
| F28 | 2026-11-04 | crash report (staging) | auto | 0.5.0 web | Every web water page reported a fatal module-load error | T391 | fixed | 0.6.0 |
| F29 | 2026-11-05 | crash report (staging) | auto | 0.5.0 Android | A phone opened an older data release it could not read | T403 | fixed | 0.6.0 |
| F30 | 2026-11-05 | crash report (staging) | auto | 0.5.0 Android | Map style files failed to load after an in-session data update | T418 | fixed | 0.6.2 |
| F31 | 2026-11-06 | crash report (staging) | auto | 0.6.2 Android | Water map crashed when moving to another river's map | T444 | fixed | 0.6.6 |
| F32 | 2026-11-06 | crash report (staging) | auto | 0.6.1–0.6.2 Android | A map call ran after its view was gone | T445 | fixed | 0.6.6 |
| F33 | 2026-11-06 | crash report (staging) | auto | 0.6.3 Android | First install: a data file did not match its manifest | T446 | fixed | 0.6.8 |
| F34 | 2026-11-10 | crash report (staging) | auto | 0.4.0–0.7.0 | Error reports that are not defects (no Play services, test-bed runs, network) now classed apart | T595 | fixed, owner read pending | 0.7.4 + feeds worker 2026-11-12 / 2026-11-18 |
| F35 | 2026-11-22 | crash report (prod) | auto | 0.8.1–0.8.5 | Triage of ~20 untriaged production crash kinds (11 of them from one search engine's page renderer) | T1005 | in progress (app fixes in 0.9.0; the worker's classes left) | 0.9.0 (app fixes) |
| F36 | 2026-11-22 | crash report (prod) | auto | 0.8.4 Android | The Play update check asked for a version file only the staging bucket held | T1014 | open (owner: production file copy) | — |
| F37 | 2026-11-22 | crash report (prod) | auto | web | A search engine's renderer runs the web app on a cached old release | T1015 | open | — |
| F38 | 2026-11-22 | crash report (prod) | auto | web | A tab left open across two releases met missing files | T1016 | open | — |
| F39 | 2026-11-22 | crash report (prod) | auto | web, iPhone | Two single-report defects still to locate | T1017 | open | — |
| F40 | 2026-11-24 | crash report (prod) | auto | 0.8.5 | A new crash kind, two reports: a read of a missing value (stack not yet read) | T1055 | open | — |
| F41 | 2026-11-24 | crash report (prod) | auto | 0.8.5 web | The browser's stored data was cleared while the page was open, and the page reported an error instead of recovering | T1092 | open | — |
| F42 | 2026-11-24 | crash report (prod) | auto | 0.9.0 Android | A location request failed because the phone's Location setting was off, and the error was not handled | T1093 | open | — |
| F43 | 2026-11-24 | crash report (staging) | auto | 0.8.5 Android | The system ended the app for low memory while it was on screen | T1094 | open | — |
| F44 | 2026-11-24 | in-app report (staging) | R-8 | 0.9.0 | Asked for a place to record fish measurements on a saved location, several per location, editable later | T1095 | open (feature; design first) | — |
| F45 | 2026-11-25 | told the owner | T-5 | 0.9.0 | The first download should respect mobile data: ask before downloading on data, and ask again if Wi-Fi is lost | T1109 | open | — |
| F46 | 2026-11-25 | crash report (prod) | auto | 0.9.0 iPhone | No push token arrived within 30 seconds while the developer was testing notifications on TestFlight | T996 | open (the push proof is owed after the App Store release) | — |
| F53 | 2026-11-26 | crash report (prod) | auto | 0.9.0 | A stored-data connection was closing while the app used it (a database "InvalidStateError"; stack and platform still to read) | T1195 | open | — |
| F55 | 2026-11-26 | crash report (staging) | auto | 0.7.3-0.8.0 | Triage: 26 staging error reports from builds 0.7.3 to 0.8.0 (2026-11-13 to 11-18) marked old-build noise, no tickets (owner: "Mark as old-build noise, no tickets"); a kind that reproduces on 0.9.0 or later gets its own row | T1057 | declined (old builds; reopened per kind if it reproduces) | — |
| F56 | 2026-11-26 | crash report (prod) | auto | 0.9.0 | A fresh occurrence of the known "app ended while on screen" native crash kind (about 191 MB in use); still reported on 0.9.0 | T1057 (nearest: T1094) | open | — |

## The developer's own finds on a shipped build (not tester feedback)

The owner uses the same builds. His finds are listed so the record is whole, but the questionnaire's tester numbers
leave them out.

| ID | Date | Channel | Tester | Version | Summary | Ticket | Status | Fixed in |
|---|---|---|---|---|---|---|---|---|
| D01 | 2026-11-10 | crash report (staging) | owner | 0.7.0 Android | A data download failed on a missing partial file | T596 | fixed, owner read pending | 0.7.4 |
| D02 | 2026-11-20 | in-app report (prod) | owner | 0.8.3 iPhone | The water page's Map tab could show a much bigger map | T979 | fixed | 0.8.5 |
| D03 | 2026-11-21 | in person | owner | 0.8.4 | After updating, expired in-season notices showed as New | T983 | fixed | 0.8.5 |
| D04 | 2026-11-21 | in person | owner | 0.8.4 | The Sources menu's Done button did not close it (not reproduced; every sheet pinned by tests) | T988 | fixed | 0.8.5 |
| D05 | 2026-11-21 | in person | owner | 0.8.4 web | Opened from a search result, back went to the default map instead of the water | T989 | fixed | 0.8.5 |
| D06 | 2026-11-23 | in person | owner | 0.8.5 web | The public gauge page's chart was a low-resolution 7-day chart with no forecast | T1039 | fixed, owner read pending | 0.9.0 |
| D07 | 2026-11-23 | in person | owner | 0.8.5 | A gauge with a live level on the Water Office page showed no Level in the app | T1043 | fixed | 0.9.0 |
| D08 | 2026-11-23 | in-app report (staging) | owner | 0.8.5 Android | A lake's lake trout read "hatchery only"; asked why (the book read the agreed way; added to the gotchas) | — (answered; T1046 cites it) | answered, no change | — |
| D09 | 2026-11-24 | in-app report (staging) | owner | 0.9.0 web | A water closed to all fishing said it twice, salmon and game fish under separate headings | T1052 | open | — |
| D10 | 2026-11-24 | in-app report (staging) | owner | 0.9.0 web | A creek closed with its tributaries: the sources did not say "tributaries", and streams above its head lake were left out | T1053 | open (part waits on an owner ruling) | — |
| D11 | 2026-11-24 | in-app report (staging) | owner | 0.9.0 web | Asked whether treble hooks are allowed on a lake: the gear tab did not say | T1051 | open | — |
| D12 | 2026-11-24 | in-app report (staging) | owner | 0.9.0 web | Ice fishing repeated the gear tab's line and hook counts | T1050 | open | — |
| D13 | 2026-11-24 | in person | owner | 0.8.5 → 0.9.0 Android | The app announced an update while the build was on a track the phone could not install from yet | T1073 | open | — |
| D14 | 2026-11-25 | in person | owner | web | The gauges page, found by a visitor through a web search, had no way to search for a gauge by place or river | T1107 | open | — |
| D15 | 2026-11-25 | in person | owner | 0.9.0 | Notifications are off after install and the app never asks to turn on new-data notices | T1110 | open | — |
| D16 | 2026-11-25 | in person | owner | 0.9.0 | The More page does not refresh by pulling down | T1111 | open | — |
| D17 | 2026-11-25 | in person | owner | 0.9.0 | The saved-spot page still feels clunky; a fresh redesign is wanted | T1115 | open | — |
| D18 | 2026-11-25 | found in development | owner | 0.9.0 data | A lake and its creek showed another, same-named lake's stricter rules (a name bound across management units) | T1117 (audit: T1118) | open | — |
| D19 | 2026-11-25 | in person | owner | 0.9.0 web | A river's plain link opened the rules of one unnamed part (an ecological reserve side channel) and read "Closed"; parts were numbered instead of named | T1140 (T1146, T1149) | open | 0.10.0 (names); the landing in the next patch |
| D20 | 2026-11-25 | in person | owner | 0.10.0 candidate | The game-fish status bar showed "open" in a different colour from the salmon bar | T1145 | fixed (#260) | 0.10.0 |
| D21 | 2026-11-26 | seen in a tester's picture | Claude | 0.9.0 Android | At a larger system font the Salmon label of the mode switch wrapped onto two lines and the Conditions tab label was cut short | T1201 (follow-up T1203) | in progress (fixed on `main`; the screens not yet walked: T1203) | 0.10.0 (pending) |
| D22 | 2026-11-26 | seen in a tester's picture | Claude | 0.9.0 Android | A lake with no salmon (Arctic drainage) led with a Salmon status bar (the regional rule applies; the question is whether to show it) | T1202 | answered, no change | — |
| D23 | 2026-11-27 | Play Console warning | owner | 0.9.0 Android (closed testing) | Three Play Console warnings: deprecated edge-to-edge APIs (Android 15), orientation and resizability limits ignored on large screens (Android 16), native libraries not aligned for 16 KB pages (may fail to install or start) | T1261, T1262, T1263 | open | - |

## Counts (refresh before applying)

As of 2026-11-24: 17 tester rows from 8 tags (T-1 … T-5, TF-1, CT-1, CT-2; some may be the same person), 10 in-app
report rows, 18 crash rows (45 in all), plus 18 developer rows. Of the 45: 25 fixed in a shipped release, 1 fixed with
the owner's read pending (F34), 6 fixed in 0.9.0 (shipped 2026-11-24, r-0aec88ed3edb: F15, F16, F22, F24, and F21, F25 with the
owner's read pending), 3 in progress (F06, F23, F35; F23 and F35 partly fixed in 0.9.0), 10 open (F40–F44 added 2026-11-24, F45 on 11-25).
Developer rows: D01 fixed, owner read pending; D02–D05 fixed; D06 (owner read pending) and D07 fixed in 0.9.0; D08
answered with no change; D09–D12 open (0.9.1); D13 open (0.9.2); D14 open (0.9.1); D15–D17 open (0.9.2); D18 open (0.9.1).
2026-11-26: F47–F52 added (tag CT-3, by email; all open, for the 0.10.0 re-cut) and D21, D22; later that day F53 (a crash kind, T1195), F54 (an in-app feature report, T1197), F55 (26 old-build staging items marked noise, no tickets) and F56 (a fresh native-crash occurrence on 0.9.0, T1057); 2026-11-27: D23 (three Play Console warnings); not in the numbers above.
Play Console testing feedback: none came in (owner 2026-11-24: all feedback came through the app; testers did not know how to use the Play form). Recount before applying.
