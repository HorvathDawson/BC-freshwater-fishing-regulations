order: 740.625

## 2026-11-24: your four staging reports, ticketed for 0.9.1 (as asked; the answers are in the section above)

- Store checks 2026-11-24 12:10: staging worker, 5 reports since 11-24: your four from Dick Lake, Nelson Creek and Eagle
  Creek are T1050–T1053; the Harrison Lake one (e034e473) was answered 11-23 (gotcha, PR #85), now cited in T1046; no
  staging crash defects (1 crawler). Production, TestFlight and Play not read in this check.
- **T1050 ice fishing lines** (Dick Lake: "Lines: 1." and "Hook, lure or fly per line: 1." under Ice fishing repeat the
  Gear tab). A: show a count under a way only where it differs from the Gear tab (here only the hut and ice-hole duties
  stay). B: one line "Same gear as above: 1 line, 1 lure or fly". C: keep. Recommended / default once you pick: A.
  Picture: `handoff/PLAN/app-integration/T1050-shots/report-now.png`. ANSWERED 11-24 12:33: "Only what differs (Recommended)"; built (T1050).
- **T1051 treble hooks on a lake** (yes, allowed on Dick Lake: p.80 "use of a treble hook is permitted unless single
  hook is specified", p.8 barbed hooks allowed in lakes; the Gear tab says nothing). A: a second Hooks line "Treble and
  barbed hooks allowed ⓘ" wherever nothing restricts hooks. B: the same on lakes only. C: keep silent, note in the ⓘ
  only. Recommended: A. Picture: `handoff/PLAN/app-integration/T1051-shots/report-now.png`. ANSWERED 11-24 12:33: "Say it everywhere (Recommended)"; built (T1051).
- **T1052 fully closed water: one statement** (Nelson Creek). A: keep both calendars; one block "Closed to all fishing
  all year, salmon included" / "The B.C. Synopsis row for Nelson Creek (and its tributaries) reads “No Fishing”." + the
  source. B: smallest change, the banner says "No fishing for any fish, salmon included" and the salmon row's sentence
  goes. C: A with one "All fish" calendar. Recommended: A. Picture: `handoff/PLAN/app-integration/T1052-shots/report-now.png`.
  ANSWERED 12:33: "One block, both calendars (Recommended)". Built (lane L22, 936 waters). Left for you: the line under
  the headline is a proposal ("The B.C. Synopsis row for Nelson Creek reads “No Fishing”." / on a tributary "Little
  Qualicum River’s row covers its tributaries: “…”."), and a water closed to all fishing for PART of the year by one
  rule (130 today) keeps its two statements unless you want a headline like "Closed to all fishing until Apr 30, salmon
  included". Pictures: `handoff/PLAN/app-integration/T1052-shots/nelson-after-light.png` (and -dark, nelson-before-*,
  skagit-two-sources-*).
  THE REAL LINES (936 parts): own row "No Fishing" 122; own row, another line 259 (parks and reserves 144); tributary
  "No Fishing" 163; tributary quoting the mainstem's extent 392 (Brunette 94: "No Fishing upstream of Burnaby Lake or
  from Cariboo Dam to Salamander Creek", Squamish 88, Bulkley 34, Capilano 22, Sustut 16). Pick A (as built: quote the
  line, `handoff/PLAN/app-integration/T1052-shots/wordings-A-light.png`) or B (a tributary says "Inherited from Brunette
  River: its row covers its tributaries.", the line one tap away: `handoff/PLAN/app-integration/T1052-shots/wordings-B-light.png`).
- **T1053 walk through a head lake** (Eagle Creek ✱ flows out of Dick Lake; the walk stops at the lake, so Dick Creek
  and 9 small streams above it stay under the zone rules; no written ruling covers this, it is as built). K: keep (as
  built). L: a whole-water rule's walk climbs through its head lake when that lake has no row of its own (24 such lakes:
  Dick, Cameron, Morice, Meziadin, Tsacha, Sooke …); the lake itself stays uncovered. M: through every head lake (36, incl.
  Cowichan, Pitt, Slocan, Babine: much wider). Recommended: L, after a binding diff you read. Default: K until you rule.
  Also the words: the rule's Sources sheet should say "Covers Eagle Creek and its tributary streams" (A, recommended) or
  only show it in Technical detail (B). Picture: `handoff/PLAN/app-integration/T1053-shots/report-now.png`.
