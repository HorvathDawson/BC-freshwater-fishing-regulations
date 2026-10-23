# T78 — Tidal / backwater disclosure on the Level view (audit N20)

Status: DONE (code) — d312b153 (pipeline: gauge.tidal), dafaeb0d (app: Level-view sentence). Left: the one deliver (below) so the live bundle/release carry the column; owner questions Q-T78a–c with defaults built.

Source: decisions.md "a water's own gauge comes first" (owner): "In the Level view, check and disclose tidal or
backwater influence where the record shows it (Port Coquitlam is tidal, with Fraser backwater)." The own-gauge REPORT:
Level is led by the tidal gauges at Whonock, Mission, Port Mann. No gauge-side tidal flag exists in pipeline/gauges,
core/flow.ts, panel.ts, the feed or the bundle.

## Work
Pipeline flags tidal/backwater-influenced level stations (record evidence + a curated list); the release carries the
flag; core supplies one sentence, shown on Level only.

## Acceptance
08MH035, Whonock, Mission, Port Mann pinned flagged; an unaffected station carries no flag (mutation pin); whole-string
test of the sentence.

Depends: a deliver (ride with T56 or later). Size: M. Owner: no (owner-stated rule).

## Done (2026-10-22, worktree wf_4b4f5207-c7a-2)
- `pipeline/gauges/consume/tidal.py`: HYDAT `STN_REMARKS` naming the TIDE (`\bTID(AL|E|ES)\b`) joined with the authored
  `data/curated/gauges/tidal.json` (new; `CURATED.gauges.tidal`, config.yaml). Episodic "backwater" remarks (ice,
  beavers, the 2024 Chilcotin slide, Comox Lake) are NOT read. Live HYDAT: 6 remark stations (4 outside the roster:
  08MH025, 10LC002, 10LC021, 10MC011); with the list, 7 roster stations flagged: 08MH024 Mission, 08MH126 Port Mann
  (record), 08MH035 Pitt nr Port Coquitlam, 08MH044 Whonock, 08MH028 Steveston, 08MH032 North Arm, 08MH053 Deas Island
  (list). Hope 08MF005 and Vedder Crossing 08MH001 are not.
- Bundle `gauge.tidal INTEGER NOT NULL DEFAULT 0` (last column); release contract additive, still `conditions/1`;
  pack.generated.ts re-emitted. A missing HYDAT or list raises.
- App: core `tidalNote(rows, quantity)` / `TIDAL_NOTE` — "One of these gauges is tidal: its level rises and falls with
  the tide and with water backed up from downstream, not only with what this river carries." Level only, reporting
  members only. Flag rides `PanelRoute.tidal` (from `gaugePlaces`, now `SELECT *` so an older release still answers);
  `donorView` takes `quantity`; DonorPanel shows it in the fold beside the dam note; ConditionsPanel passes the map's
  quantity. Dev fixture rebuilt (gauge rows gain `tidal 0`, nothing else).
- Tests: pipeline/tests/test_tidal_gauges.py 21 passed (+1 skipped live-HYDAT test; run by hand against the main repo's HYDAT:
  flags as above), incl. the acceptance pin and two mutation pins (pattern widened to BACKWATER flags the ice/beaver/
  slide/lake stations; word boundary dropped flags "STATIDAL"). App: core conditions.test 1 new (whole string), data
  bundle/tidal.test 2, ui conditions.render 1, ui-native DonorPanel 2 + ConditionsPanel 1. Manual mutations: removing
  `quantity === "level"` in core and the `quantity` prop in ConditionsPanel each turned a test red; restored.

## Rebuild this needs (merger)
- `pipeline.deliver` (bundle + release) ONLY. No reach, no atlas, no tiles. Paint is unchanged.
- Diff: conditions `gauge` gains column `tidal`; exactly the 7 stations above are 1, every other row 0; no other table
  or file changes. `pipeline/tests/test_release.py::test_a_built_release_is_exactly_the_contract` needs the rebuilt
  bundle (against today's live bundle it fails on the missing column).

## Owner questions (defaults built)
- Q-T78a: the sentence sits in the closed "Notes and sources" fold beside the dam note (one caveat look). Default: as
  built. Alternative: show it open under the headline in Level.
- Q-T78b: other possibly tidal level gauges are NOT flagged without a recorded reason (e.g. 08MH026 Brunette at
  Sapperton, 08MH002 Coquitlam at Port Coquitlam, coastal river mouths). Default: add a row to tidal.json only with a
  reason a reader could check.
- Q-T78c: standing non-tidal backwater (08HB074, "variable backwater from Comox Lake") is not flagged; the sentence is
  about the tide. Default: as built.
- A saved spot does not record its quantity, so it says nothing of the tide. Default: as built.
