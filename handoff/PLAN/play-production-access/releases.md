# App releases since testing began (T1046)

One row per app version that reached testers, newest first, with the feedback rows (`feedback-log.md`) it answered.
Testing began with 0.4.0 on Play internal (2026-10-31); 0.1.0–0.3.0 came before Play testing. Upkeep:
`README.md` (a row per release, added at `runbooks/release-by-hand.md` Step 8).

Sources: the version and build history of `app/apps/mobile/app.json` (the date is the day of the first Play upload
commit for that version, or of the version commit before 0.7.0), the "What's new" bullets in
`app/packages/core/src/changelog.ts`, and `handoff/PLAN/STATUS.md` / `STATUS-archive.md` for the tracks. Builds are
Android versionCodes unless marked iPhone. The first tag, `v0.9.0`, sits on the versionCode 44 commit (moved there with the owner's OK, 2026-11-24; `AGENTS.md` W47).

Tracks: **Internal** = Play internal testing; **Closed** = Play closed testing (the production-access test);
**TF int / TF ext** = TestFlight internal / external testers.

| Version | Date | Builds | Tracks | What changed for testers | Feedback answered |
|---|---|---|---|---|---|
| 0.9.0 | 2026-11-24 (cut 2026-11-23; data r-0aec88ed3edb, site and feeds worker on production 2026-11-24) | 44 (production); iPhone: production build uploading | Internal + Closed 2026-11-24 (Google's review of the Closed release pending); TF int + ext once the iPhone build is processed | Each regs tab names its rule-book pages, a fish's sheet has its own Sources tab, the Class II line sits under the water's name; a river with no normal range is charted against its own year and a level-only gauge (Capilano) shows its live level; shaded lake shores, boundary labels that no longer pile up, a selected slough outlined once; the bell shows only news since install; shared links open the same part, tab and day, and the day picker runs past New Year (`changelog.ts` 0.9.0). Also: regs sub-tabs jump instead of scrolling, "Flows into" opens the map, Runs reads ahead, crash fixes; data: ditch pieces leave the Serpentine, Nicomekl and Kootenay (T1012). Known issue: a shared link that names a tab, opened from a cold start on a slow line, usually lands short of that tab (T1049, fixed in 0.9.1) | F15, F16, F21, F22, F23 (part), F24, F25, F35 (part) (and D06, D07) |
| 0.8.5 | 2026-11-21 | 43; iPhone 44 | Internal + Closed; TF int, TF ext (approved 2026-11-22) | One Sources button on the regs page; plainer gauge wording and a no-gauge sentence; ended in-season notices no longer come back as New; back from a web link returns to the water; the guided tour only shows the way | F08–F14, F17–F20 (and D02–D05) |
| 0.8.4 | 2026-11-20 (released 2026-11-21) | 42 (production); iPhone 43 | Internal, then Closed 2026-11-21; TF int, TF ext (approved) | Classified Waters licence carried to creeks; salmon limits name their region; flow estimate follows Flow; iPhone fixes (keyboard, sideways map, photo swipe); smaller data updates | F02–F07 |
| 0.8.3 | 2026-11-19 | 39 (production, Closed), 40 (staging, Internal), 41; iPhone 41 | Closed (first closed-test build), Internal; TF ext (review) | Smoother pinch-zoom; data updates continue after leaving the app; a simpler trout table; steady day calendar; rule-book link to the water's own page | — |
| 0.8.2 | 2026-11-19 | 37–38; iPhone 39 | Internal; TF ext (approved) | Smoother pinch-zoom; one keep table for every fish; adult chinook yearly limit; sunrise/sunset under the date; "what's new" note on update | — |
| 0.8.1 | 2026-11-18 | 36 | Internal | Faster map, most of all the Salmon map zoomed out; route maps show gauges and your location | — |
| 0.8.0 | 2026-11-18 | 35 | Internal | The trout and char redesign: one table per water, tap a fish for identification; Classified Waters shown by the name; licence closures for non-resident aliens | — |
| 0.7.12 | 2026-11-17 | 34 | Internal | Swimming coho while data loads; resumable first download; book closures shown closed on salmon stretches | — |
| 0.7.11 | 2026-11-16 | 33 | Internal | Code shrinker on; search frames the part picked; upstream / downstream along one line | — |
| 0.7.10 | 2026-11-14 | 32; iPhone 33 | Internal; TF int (first iPhone build) | No flash going back; credits above the legend; search swipe-down; a rare crash on opening a water fixed | — |
| 0.7.9 | 2026-11-14 | 31 | Internal | Faster opening on a tap; gauge and wording fixes | — |
| 0.7.8 | 2026-11-13 | 30 | Internal | Notices inbox in the bell; Settings screen; salmon block first on the regs page; better river junctions | — |
| 0.7.7 | 2026-11-12 | 29 | Internal | Tidal boundaries from DFO's written line; small ponds follow their river; search finds former names | — |
| 0.7.6 | 2026-11-11 | 28 | Internal | Keep and Gear tabs follow the data release; tidal parts said plainly; salmon period limits drawn | — |
| 0.7.5 | 2026-11-11 | 27 | Internal | Warning on a closure the map cannot draw; streams open over satellite; legend always matches the map | — |
| 0.7.4 | 2026-11-10 | 26 | Internal | Runs keeps its own day; share a water's page; current notice at the top of the regs page; gauge alerts by the day | F34 (and D01) |
| 0.7.3 | 2026-11-10 | 25 | Internal | Plain-word gauge standings; Flow leads the gauge choice; Report a problem from Conditions and Map | — |
| 0.7.2 | 2026-11-09 | 24 | Internal | Each calendar sits with its own fish; clearer calendar legends | — |
| 0.7.1 | 2026-11-09 | 23 | Internal | Share a river's flow from Conditions | — |
| 0.7.0 | 2026-11-09 | 22 | Internal | Flow alerts; river parts join where rules match; darker-mode map lines; stale DFO rules marked | — |
| 0.6.9 | 2026-11-07 | 19–21 | Internal | Smoother panning over time; daily limit stays at the top; sizes written one way | F27 |
| 0.6.8 | 2026-11-06 | 17–18 | Internal | New walkthrough; pinch-zoom on spot photos; fresh-install file fix | F01, F33 |
| 0.6.7 | 2026-11-06 | 16 | Internal | Smoother flick; no stream flicker going back to the map | — |
| 0.6.6 | 2026-11-06 | 15 | Internal | Smoother panning zoomed out; a crash moving between river maps fixed | F31, F32 |
| 0.6.5 | 2026-11-06 | 14 | Internal | Update notice only once Play has the version | — |
| 0.6.4 | 2026-11-06 | 13 | Internal | Smoother panning; screen readers can accept the first-run notice | — |
| 0.6.3 | 2026-11-06 | 12 | Internal | Map credits in one button; More > Updates syncs; smoother rivers zoomed out | — |
| 0.6.2 | 2026-11-05 | 11 | Internal | New version shown at once; watershed conditions do not blank; labels kept during an update | F30 |
| 0.6.1 | 2026-11-05 | 10 | Internal | Highlighted river part no longer drawn as crossing lines | — |
| 0.6.0 | 2026-11-05 | 9 | Internal | Gauge alerts; new-version notice; tap a legend class to filter; dashed closed streams | F28, F29 |
| 0.5.0 | 2026-11-04 | 8 | Internal | Rule book opens in the app at the water's page; 10-day forecast; B.C. time everywhere; fewer Android crashes | F26 |
| 0.4.0 | 2026-10-31 | 7 | Internal (first Play build) | Offline gauge charts; parking pins on spots; provincial parks; salmon rules by species | — |
