# T164 — Salmon (DFO) regulations: finish the dossier, place the splits, review the extents, fetch and place current quotas

Status: OPEN

Source: owner 2026-10-23: "make sure we have a ticket to finish the salmon dossier stuff and finish adding the splits
needed for salmon regulations and doing all the curation review for the extents so that we could fetch the current
quotas etc for all extents and be able to place them." Umbrella over T95 (plan) and T132 (review pass); the design and
the measured churn are in pipeline/regs/dfo_salmon/ (README, DESIGN, CURATION, MAPPING, DATAFLOW, PLAN.md).

## Where it stands (measured 2026-10-23 with the tools themselves)
- Matching (`python -m pipeline.regs.dfo_salmon.dossier list`): 1 water unresolved — Sheldens Creek (r6, unmatched).
- Splits (`python -m pipeline.regs.dfo_salmon.splitwork`): 104 waters carry place-naming locators; 93 still need at
  least one cut. ACTIVE locators: 96 need a coordinate (a bridge / sign / road / dam), 7 are confluences (derivable from
  FWA topology), 33 already satisfied; plus 48 DORMANT locators (archived wordings a regulation update can bring back).
  **192 DFO rules resolve to nothing until these cuts exist.** Biggest worklists: Stamp, Campbell, Dean, Kasiks,
  Kitimat, Kitsumkalum, Qualicum, Babine Lake, Puntledge, Nanaimo, Nass, Cowichan …
- Keeping it fresh (PLAN.md): the scraper, parser, untangler, stability harness and churn tool exist; the curated
  bindings, the scheduled re-fetch and the reconciler do not. Churn rule (memory dfo-salmon-churn-asymmetry): waters
  never move, rules turn over ~50 %/yr, scope text drifts — curate the GEOGRAPHY once, re-scrape the RULES every run,
  a vanished reach goes dormant, never deleted; nothing keyed on free text.

## Work (in order)
1. **Dossier:** resolve Sheldens Creek (registry match or a named added stream); re-run the dossier to 0 open.
2. **Splits:**
   a. the 7 confluence locators: derive the cut from FWA topology (no coordinate) — Claude;
   b. the "maybe?" ones: confirm against existing cuts (rewordings the strict label test misses) — Claude, listed;
   c. the 96 coordinate locators: Claude proposes a point for each from public data (OSM bridges/roads/dams, the T154
      dams layer, BC road/rail lines) with the evidence, written as splits stamped "Verify: proposed (DFO)" so they land
      in the review tool's S0 queue; the owner confirms or moves them there (fix mode, T134/T136 — add/move split then
      rebuild, review continues);
   d. the 48 dormant locators: same, after the live ones.
3. **Curation review of the extents:** the salmon queue in the review tool (T132): every DFO water's extents, T/R/P/A
   marks, evidence = the DFO notice text; place (P) marks survive a re-scrape, rule (R) marks go stale on one.
4. **Fetch the current quotas:** a scheduled re-fetch (PLAN.md): fetch → parse → untangle → reconcile against the
   curated geography; a vanished reach goes dormant; a notice when a structural change (like B → B(i)/B(ii)) appears.
   The fetch is public (no credentials); its schedule runs where the gauge feed cron runs (owner's call where).
5. **Place them:** the DFO rules bind through the reach build (`source: "dfo"`, memory dfo-override-tag) onto the
   reviewed extents; deliver; the app shows them (salmon species already in the app's fish list and images). Measured
   binding diff per water; card gate clean.

## Acceptance
Dossier 0 open; splitwork 0 active locators needing a cut (dormant listed); every DFO water reviewed in the salmon
queue; a re-fetch today places every current quota or names why not; the app shows salmon rules on the Skeena, Fraser
and Cowichan with their dates. Each step its own measured diff.

## Owner
- Confirming the proposed coordinate splits in the review tool (step 2c) — the bulk of the human time.
- Q145: where the DFO re-fetch runs and how often (default: with the gauge feed cron, weekly + a notice on change).
Depends: T97–T136 (review tool, done), T154 (dams layer, merged). Size: L. Supersedes T95's "write the plan" (PLAN.md
exists) — T95 closes into this ticket.

## Owner 2026-10-24: a review app for the DFO extents (in the local review app, T184)
"Make the app for this too — basically all extents in the web data scraped should be shown with a map and the splits
will be placed; make sure it can use more than just coord placements (confluence ones etc.)."
- A DFO mode/queue in the LOCAL curation review app (one tool, T184): every scraped DFO water and each of its extents
  (the notice's own wording shown verbatim as the evidence), drawn on the map with the PROPOSED splits placed and
  labelled, the reach each extent resolves to highlighted, and the T/R/P/A checks (P = the extent lands right).
- Split proposals use EVERY anchor type the atlas resolves (`pipeline/atlas/splits/anchors.py` AnchorType): point
  (coordinate → perpendicular cut), line, confluence (a tributary's mouth, straight across — the T67 rule), lake edge,
  MU/zone boundary, area boundary (park/reserve), and offsets ("500 m below X", "5 km above the confluence") — not
  only coordinates. Each proposal says which kind and why (the locator text it came from, the evidence source).
- Placing a split is still a proposal the owner accepts (T185: no tool writes the catalogue) — accepted proposals land
  as reviewed commits, then the rebuild; the review continues on the new build.

## Outcome — the DFO review mode in the local review app (2026-10-25)

DONE (the "DFO review app" part; steps 1, 2, 4, 5 stay OPEN). The app is usable on its own, no credits:
`bash curation-review/run.sh`, then **DFO salmon extents** in the top bar (or `#dfo/<location_id>`).
- **Queue:** every scraped DFO location (346: 247 live, 99 dormant), the notice's words verbatim, its scraped rows,
  its curated extent; filters region / live-dormant / extent state (needs a cut 154, no extent 53, no water 19, bound
  120) / mark / text; progress marked/total overall, live, per region, and new splits proposed. j / k, y n ?, Enter.
- **Map:** the water, every cut on it labelled, what the extent binds now (`build_reach`, magenta), proposals amber.
- **Mark** (scope `dfo`, the same store and the one write `POST /api/marks`): T R P (no A until step 5), an extent fix
  (T191 editor; a point may name a new split), and **new splits** (`marks.NewSplit`: anchor confluence / lake inlet /
  lake outlet / bridge / dam / sign / map point / existing split, name, item, the water it cuts, side, offset, the
  notice's words). Anchors are checked server-side (`GET /api/dfo/locations/{id}/anchor`: a click snaps to the water
  ≤500 m, a tributary's mouth ≤300 m, a lake's edge) and the resolved blk / measure / point is stamped into the mark.
  Nothing writes splits.json or the DFO entry files; tickets carry `- **New splits:**` for T185.
- Prints (`pipeline/regs/review/dfo.py`): text = the notice's words, meaning = the scraped rows (R stale on a
  re-scrape), binding = the curated geography (P stale only when it moves).
Checks: `test_review_dfo.py`, `test_curation_review_screen.py` (queue, anchors, a DFO mark changes only the store),
the safety pins unchanged; frontend `tsc -b`. Screenshots (Campbell River r1, a Quinsam confluence split + extent
fix; a confluence + a sign by map click) were taken on :18918, not committed.

## From T229 (owner 2026-10-28) — Skeena work for this queue
- Q3: a named-water place listed under one lettered section must stay inside it — "Skeena: mainstem waters only"
  (a B(i) row, pink 2/day) is bound `whole` and covers the 119 B(ii) mainstem sections below the CNR bridge. Fix it
  in the extent when the DFO extents are finished after the splits (clip to B(i)).
- Q6: the ~30 Skeena places still without an extent (their waters paint `check`): Bulkley below the Morice and the
  two Bulkley-tributaries rows, Babine Lake ×3, Babine River reaches ×2, Kitwanga, Kitseguecla incl. tributaries,
  Kitsumkalum reaches ×3 + Kitsumkalum Lake, Lakelse reaches ×3, Kispiox below the signs, Sustut and Suskwa incl.
  tributaries, Shegunia, Pinkut, Zymoetz / Zymagotitz / Exchamsiks / Exstew / Kasiks / Khyex / Gitnadoix reaches,
  Redsand Lake, Scotia, Ecstall above the signs, Bear. The review app's DFO → Cascade map shows each (dashed legend
  swatch, dotted water on the map).

## Step 2 splits: every locator has a proposal or a reason (2026-10-29)

Everything below is a PROPOSAL. Nothing was written to data/curated, splits.json, marks.jsonl or a build. Review in the
review app: **DFO salmon extents** → open a location → the violet suggestion + "split hunt" evidence; **accept
suggestion** only pre-fills the editor, and **Save extent** (your mark) is the only write.
Splitwork today: 94 ACTIVE locators need a cut (90 "coordinate" + 4 confluence; the ticket's 96 + 7 have shrunk as
bindings landed) + 49 dormant.
- **Confluence (4 active, 2 dormant):** all 6 are on existing confluence cuts from FWA topology: Bulkley/Morice,
  Dean/Crag ×2, Nass/Meziadin, San Juan/Fleet. Taku "including tributaries" = whole water.
- **Coordinate, active (90):** 17 on existing cuts (13 high, 4 medium) · 28 NEW splits with source + snap distance +
  why (10 high, 9 medium, 9 low) · **39 need no cut at all.** splitwork files every unclassified row as
  "coordinate", including "including tributaries" and "all open portions", which mean the whole water · 6 unplaced.
- **Dormant (47 coordinate):** 15 existing, 21 new (8 high, 8 medium, 5 low), 4 whole, 7 unplaced.
- **"maybe?" vs existing cuts:** 36 confirmed (the suggestion uses the maybe-cut). 18 not used: the cut is a
  different place (Brunette Cariboo Dam is 262 m above the Cariboo Rd bridge; Lakelse CNR is 9 km away), or the cut is
  reused with an offset (Pinkut +25 m). **One conflict:** Puntledge `signs_75_m_downstream_of_…_hatchery_fence` sits
  at FWA measure 4075. That is 74 m ABOVE the OSM lower-hatchery weir (4001), so it cannot be "75 m below the weir".
  It is noted on that location.
- **New today:** Nanaimo Subarea 17-14 ×2 (live wording, new on the 10-28 scrape): downstream of the curated Cedar
  Road bridge (high), and the bridge plus signs 400 m below it (medium); the caveat says it is tidal (T212).
  Corroborated from the FTEN forest-road layer: Shegunia bridge (BABINE SLIDE FSR passes 14 m away) and Lakelse
  Whitebottom bridge (LAKELSE MAIN passes 30 m away).
- **Sheldens Creek (dossier's 1 open), proposed:** the unnamed FWA line blk 360846083 (950-016367) into Sheldens Bay.
  Its roads are all named for it (FTEN R06206 "SP 19(1) TO SHELDONS", SHEL100–103). It is a REVIEW row in the
  **Names** mode (`blk:360846083~sheldens-creek`, medium, never auto-accepted; `handoff/PLAN/curation-review/name-variants/mine.py` `DFO_UNMATCHED_LEADS`).
  Accepting it names the line, and the matcher can then bind the water. The cut is ready: "SP 19(1) TO SHELDONS"
  crosses the line at 53.12968,-131.78224, FWA measure 3402 m (exact road × stream intersection).
- **Unplaced, active (6):** Kasiks ×2 (the "Upper Pool" is named nowhere public; ask DFO Terrace) · Sheldens
  (waits on the name above) · Quesnel Horsefly Bay, Mabel Lake, Osoyoos North Basin (areas of a lake: lake splits are
  deferred by owner rule). Dormant (7): Sakinaw ×2, Babine 400 m radius ×2, Quesnel, Osoyoos (lake areas), and the
  Ruskin spawning channel (a side channel FWA does not draw).
- Regenerated `dfo-suggestions.json/.md`, `dfo-split-hunt.md` and `dfo-coverage.md`. 10 other rows changed
  confidence: the owner's newer tidal proposals (T212) now sit at the same places, e.g. Fraser at the Mission CPR
  bridge, Qualicum Hwy 19A and the Dean mouth, and the matcher reports that as a near-tie. Tests:
  `test_dfo_suggest`, `test_names`, `test_name_sources_t224` pass.

## 2026-11-05: where it stands (measured)
- **Step 1 dossier: DONE** — `dossier list`: 0 unresolved DFO waters (Sheldens Creek resolved).
- **Live feed:** staging `feeds/dfo/unplaced.json` is empty (2026-11-04 run): every current DFO rule lands on a curated
  place. 161 of them still bind to no section until their cuts exist (`splitwork`: 82 waters need a cut — 80
  coordinate + 4 confluence locators active, 45 dormant).
- **Owner review so far:** 79 DFO places marked (68 verified, 7 unsure, 4 wrong; last 11-03).
- **Proposals refreshed** against today's build (`suggest`, 2026-11-05): 352 locations, 173 with a curated extent;
  the other 179 each carry a proposal — 52 on existing cuts, 47 whole water, 65 new splits (24 high / 21 medium / 20
  low), 15 unresolvable with the reason. Versus 10-29: 21 proposals dropped, all on waters the owner has marked since.
  `handoff/PLAN/curation-review/dfo-coverage.md` lists them.
- **Left — owner time:** the 179 in the review app's DFO mode (accept suggestion → Save extent), incl. T229 Q3
  (Skeena "mainstem waters only" clipped to B(i)) and the 3 DFO items T326 waits on. Then Claude: a reach-only rerun
  (memory reach-only-rerun) + the measured binding diff (step 5) and the acceptance check on Skeena/Fraser/Cowichan.

## 2026-11-06 (chat 4): proposals refreshed on r-67b456b5e64b
`suggest` on today's build (T326's 7 cuts in, T449 skips tidal places): 177 proposals (was 181). 4 dropped because
they now bind or resolved (Little Qualicum falls, Stamp inlet + powerline downstream — the T326 cuts — and Sheldens
Creek); 5 re-proposed around the new cuts (Quatse ±100 m, three Stamp places, 5b region section). Counts: 52 on existing
cuts, 47 whole water, 62 new splits (24 high / 20 medium / 18 low), 14 unresolvable. Owner time left as before: the
177 in the review app's DFO mode; then Claude's reach-only rerun + binding diff.
