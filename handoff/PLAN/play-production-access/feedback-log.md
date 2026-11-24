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
- **Status**: `open` / `in progress` / `fixed` / `declined` (the owner's call, with why in the ticket).
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
| F06 | 2026-11-19 | in person | T-3 | 0.8.3 iPhone (TestFlight) | The main map filled only two thirds of the screen after changing tabs | T928 (follow-up T938) | fixed (T938 in progress) | 0.8.4 |
| F07 | 2026-11-19 | in person | T-3 | 0.8.3 iPhone (TestFlight) | Water map: boundary labels did not line up with the places they name, and the part looked mis-framed | T929 | fixed | 0.8.4 |
| F08 | 2026-11-20 | TestFlight | TF-1 | 0.8.3 iPhone | "% of this estimate" on a gauge card was unclear | T982 #1 | fixed | 0.8.5 |
| F09 | 2026-11-20 | TestFlight | TF-1 | 0.8.3 iPhone | Gauge percentage boxes on the map: suggested a gauge symbol instead (owner picked a shaded dot with the number) | T982 #2 | fixed | 0.8.5 |
| F10 | 2026-11-20 | TestFlight | TF-1 | 0.8.3 iPhone | "Much below normal" style wording: suggested plainer words (owner picked "far lower / lower / normal / higher / far higher") | T982 #3 | fixed | 0.8.5 |
| F11 | 2026-11-20 | TestFlight | TF-1 | 0.8.3 iPhone | Spot photo buttons: suggested icons (owner picked icon plus one word) | T982 #4 | fixed | 0.8.5 |
| F12 | 2026-11-20 | TestFlight | TF-1 | 0.8.3 iPhone | The sentence shown when no gauge reads a water was hard to understand; asked for plainer language | T982 #5 | fixed | 0.8.5 |
| F13 | 2026-11-20 | TestFlight | TF-1 | 0.8.3 iPhone | The app did not open at the tester's location (a dismissed first location ask was never offered again) | T982 #6 | fixed | 0.8.5 |
| F14 | 2026-11-21 | in person | T-4 | 0.8.x | The Trout / Salmon switch did not show clearly which one was on | T986 | fixed | 0.8.5 |
| F15 | 2026-11-22 | in person | CT-1 | 0.8.4–0.8.5 Android | Switching regulation sub-tabs (keep ↔ licence) scrolled through every section in between. Same message: positive overall ("information dense but still readable") | T1025 | fixed | 0.9.0 (pending) |
| F16 | 2026-11-22 | in person | CT-2 | 0.8.4–0.8.5 | The day picker stopped at December 2026 though the rule book is valid to March 31, 2027 | T1026 | fixed | 0.9.0 (pending) |

## In-app reports (Report a problem)

| ID | Date | Channel | Tester | Version | Summary | Ticket | Status | Fixed in |
|---|---|---|---|---|---|---|---|---|
| F17 | 2026-11-21 | in-app report (prod) | R-1 | 0.8.4 Android | The gauge chart flickered when switching forecast model on first load | T985 #1 | fixed | 0.8.5 |
| F18 | 2026-11-21 | in-app report (prod) | R-2 | 0.8.4 iPhone | No downstream label on a river's Map view (now "Flows into …" at the mouth) | T985 #2 | fixed | 0.8.5 |
| F19 | 2026-11-21 | in-app report (prod) | R-2 | 0.8.4 iPhone | Map uncoloured after going to it from Conditions until a pan or zoom | T985 #3 | fixed | 0.8.5 |
| F20 | 2026-11-21 | in-app report (prod) | R-2 | 0.8.4 iPhone | Chart gestures: no way to drag the date bar; zoom and pan got in the way (owner picked a drag handle plus touch-and-hold) | T985 #4 | fixed | 0.8.5 |
| F21 | 2026-11-22 | in-app report (prod) | R-3 | 0.8.x | The Class II tag sat far right of the water's name and was easy to miss | T1004 | in progress (owner look) | 0.9.0 (pending) |
| F22 | 2026-11-22 | in-app report (staging) | R-4 | 0.8.5 Android | Ditches drawn as part of a river's main stem in salmon mode | T1012 | in progress | 0.9.0 data (pending) |
| F23 | 2026-11-22 | in-app report (staging) | R-5 | 0.8.5 Android | Switching to Runs took a very long time the first time | T1013 | in progress | 0.9.0 (pending, partial) |
| F24 | 2026-11-23 | in-app report (staging) | R-6 | 0.8.5 Android | "Flows into …" should open the receiving water on its map, not its regulations | T1023 | fixed | 0.9.0 (pending) |
| F25 | 2026-11-23 | in-app report (staging) | R-7 | 0.8.5 Android | Odd highlighting on a lake (not reproduced on the web yet) | T1024 | in progress (PR) | — |

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
| F34 | 2026-11-10 | crash report (staging) | auto | 0.4.0–0.7.0 | Error reports that are not defects (no Play services, test-bed runs, network) now classed apart | T595 | fixed (owner read pending) | 0.7.4 + feeds worker 2026-11-12 / 2026-11-18 |
| F35 | 2026-11-22 | crash report (prod) | auto | 0.8.1–0.8.5 | Triage of ~20 untriaged production crash kinds (11 of them from one search engine's page renderer) | T1005 | in progress | 0.9.0 (pending, app fixes) |
| F36 | 2026-11-22 | crash report (prod) | auto | 0.8.4 Android | The Play update check asked for a version file only the staging bucket held | T1014 | open (owner: production file copy) | — |
| F37 | 2026-11-22 | crash report (prod) | auto | web | A search engine's renderer runs the web app on a cached old release | T1015 | open | — |
| F38 | 2026-11-22 | crash report (prod) | auto | web | A tab left open across two releases met missing files | T1016 | open | — |
| F39 | 2026-11-22 | crash report (prod) | auto | web, iPhone | Two single-report defects still to locate | T1017 | open | — |

## The developer's own finds on a shipped build (not tester feedback)

The owner uses the same builds. His finds are listed so the record is whole, but the questionnaire's tester numbers
leave them out.

| ID | Date | Channel | Tester | Version | Summary | Ticket | Status | Fixed in |
|---|---|---|---|---|---|---|---|---|
| D01 | 2026-11-10 | crash report (staging) | owner | 0.7.0 Android | A data download failed on a missing partial file | T596 | fixed (owner read pending) | 0.7.4 |
| D02 | 2026-11-20 | in-app report (prod) | owner | 0.8.3 iPhone | The water page's Map tab could show a much bigger map | T979 | fixed | 0.8.5 |
| D03 | 2026-11-21 | in person | owner | 0.8.4 | After updating, expired in-season notices showed as New | T983 | fixed | 0.8.5 |
| D04 | 2026-11-21 | in person | owner | 0.8.4 | The Sources menu's Done button did not close it (not reproduced; every sheet pinned by tests) | T988 | fixed | 0.8.5 |
| D05 | 2026-11-21 | in person | owner | 0.8.4 web | Opened from a search result, back went to the default map instead of the water | T989 | fixed | 0.8.5 |
| D06 | 2026-11-23 | in person | owner | 0.8.5 web | The public gauge page's chart was a low-resolution 7-day chart with no forecast | T1039 | in progress (PR) | 0.9.0 (pending) |
| D07 | 2026-11-23 | in person | owner | 0.8.5 | A gauge with a live level on the Water Office page showed no Level in the app | T1043 | in progress (PR) | 0.9.0 (pending) |

## Counts (refresh before applying)

As of 2026-11-23: 16 tester rows from 7 tags (T-1 … T-4, TF-1, CT-1, CT-2; some may be the same person), 9 in-app
report rows, 14 crash rows (39 in all), plus 7 developer rows. Of the 39: 27 fixed in a shipped release, 3 fixed on
`main` for 0.9.0, 5 in progress, 4 open.
TODO (before applying): add every Play Console testing-feedback item (Monitor and improve > Ratings and reviews >
Testing feedback) the owner reads, then recount.
