# T227 — A water closed to game fish but open for salmon says so at the top (the salmon year bar)

Status: DONE — 4f2e22a3, 6af1ffa5 (DFO proposals), follow-ups in the ledger row

Source: owner 2026-10-28 ("Cowichan has no fishing generally but salmon are open on certain sections, but when you view
the regs this isn't very clear"), with add-ons the same day: the salmon bar at the top; region notes beside their
species; a closed-all-year salmon answer is a plain row; one timeline once every DFO place is mapped; the whole glance
row is the press.

## Diagnosis (Cowichan, Oct 28, part "mouth → CNR Mile 66 Trestle")
- The only status on the page was the provincial one: "✕ Closed today · Opens Nov 16", "No fishing for any game fish
  Jul 4–Nov 15". The book's closures are game fish only (salmon are DFO's) but nothing near the top said so.
- The salmon answer was folded into "Species and limits" → Salmon → "Details" (mixed rows: no word), and on a closed
  water that table was drawn ABOVE the year bar (`KeepTab` put it first when no row held it).
- DFO: 66 Mile Trestle → Sandy Pool is catch-and-release Sep 1–Jul 14 (coho 1/day Nov 1–Dec 31); Sandy Pool → tidal
  boundary is not drawn on the map (PARTIAL COVERAGE), so every row read "check DFO".

## Built
1. **The salmon year bar** (`@app/core` `salmonCard().year`, `salmonStripOf`; `ui-native` `SalmonYearStrip`), right under
   the provincial bar at the top of the Keep tab. The two are named: "Game fish (B.C. rules)" / "Salmon (DFO)". Same
   visual language: open solid, release-only stripes, closed hatch, plus "check DFO" (checker) and "differs along this
   part" (half open, half hatch). Each day's status is the pipeline's `map_status` read from the five species'
   verdicts (`salmonStatusFromSpecies`, pinned to the release's map runs: 323/323 chains without partial coverage
   agree). A day no DFO rule speaks for is CLOSED (the pipeline's `NOT_LISTED`).
   - Every DFO place mapped: ONE bar for the viewed part/section, `varies` on days its sections differ.
   - Partial coverage: see Follow-ups 1 (one bar, one line listing the undrawn places).
   - No bar where DFO has no salmon rules for the page (the unlisted water / "not opened" only), and none where salmon
     and the provincial bar are both closed all year.
2. **Region-wide closure** (owner): a page whose DFO answer is only its region's own rows and whose region header says
   "no fishing for salmon … until further notice" (Region 4) reads that closure: one closed row in DFO's words, no bar,
   no partial line. Region 7's header says the same but the Nechako has its own DFO openings: the override applies only
   when the page has no water-specific DFO place.
3. **Plain closed glance row** (owner): a card that is one closed-all-year statement (`closedOnly`: the region closure,
   an unlisted water, every place closed every day) → "Salmon · closed", no Details, not a button.
4. **Species notes beside the species** (owner): a region header note with species (`sp`: adult chinook, annual
   chinook limit, jack coho …) sits on that species' row (any row that may be fished) in DFO's words; one the row's
   "Sizes (DFO)" line already says (min size / adult / jack) is not repeated; the region block keeps only what no row
   took (a closed chinook's note stays there).
5. **The glance table** on a closed water now draws after the leading blocks (bar, banner), never above the bar. The
   WHOLE folded row opens/closes it (it already was the Pressable; now it shows pressed / hover / focus).
6. **For the merge lane** (not touched here): `salmonTimelineKey(f, sid)` — equal for two sections exactly when DFO's
   year reads the same on both (species verdicts, gear, map status incl. partial).

## Follow-ups (owner 2026-10-28, second round)
1. **Partial coverage, one bar.** The stacked place bars are gone: ONE bar for the viewed part; a day an undrawn DFO place
   answers otherwise is "check DFO" (it may lie on this part), a day every place agrees reads that answer. Under it one
   line, "N DFO places here aren't on the map yet — check DFO ⌄", opens the list, each with its own day. The card rows
   follow the same rule per species (a row keeps its answer only where every undrawn place says the same for each of its
   species), so the bar and the card agree.
2. **Catch-and-release shapes.** Every DFO verdict whose words say release ("Open for salmon catch and release",
   "Non-retention (…)", "No retention of salmon / of coho (…)") already reads `release` (take 0, may target) in the
   release file, bar, glance, card and map. One reads otherwise: Region 4's "Non-retention (as printed: Apr 1 until
   further notice)" is `check` (the date is unreadable) — the region header's "no fishing for salmon in Region 4 until
   further notice" now answers it as CLOSED on the card AND the map (`regionClosedChain`, owner's earlier call). The
   synopsis has no salmon catch-and-release line (its salmon mentions are game-fish closures near viewing pools, a
   licence record line and pointers to DFO). Not changed: "N hatchery-marked per day" reads open (wild ones must go back).
3. **Gear / Methods tabs.** On a closed day with salmon not closed: "No fishing for game fish here until Nov 16." then
   "Salmon (DFO): release only today — see What you can keep." (`closedLinesWithSalmon`; per part and day, from the bar).
4. **Unplaced DFO places** (6af1ffa5): proposals for all 8 Cowichan / Capilano / Kitimat places are pre-filled in the
   review app's DFO mode (dfo-split-hunt.json → dfo-suggestions.json); OWNER-TODO lists "accept in review app → DFO".
   No splits written.

## Region defaults (owner question)
No DFO region-wide/default rule opens an unlisted period: the header notes are limits, sizes, daylight, facility and
closures ("daily limit 4 … unless otherwise noted" is a limit, not an opening); openings come only from listed
locations, and the pipeline already folds a region's own listed location (`6:a:region`, `4:region:section`) into each
chain. So no-rule days stay closed.

## Shots (`../T227-shots/`)
`before-*` / `after-*` × `{cowichan, courtenay, kitimat, capilano, kootenay-nosalmon}` × `{desktop, phone}`;
`after-{cowichan,courtenay}-*-salmon-open.png` (the card, species notes on the rows); `before-cowichan-desktop.png` is
the original (table above the bar).

## Not done / open
- Cowichan stays "Check DFO" until the Sandy Pool → tidal boundary place is drawn (curation, not this ticket).
