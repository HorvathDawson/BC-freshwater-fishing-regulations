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
