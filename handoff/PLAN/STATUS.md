# Where things stand

Plain summary for the owner, updated as work lands. Newest at the top. Ticket numbers are in brackets for lookup only.

---

## 2026-10-30 evening

### Finished today

**DFO salmon limits per species** [T320]
The app now shows each species' own daily limit under the region's overall salmon limit. For example:
"Up to 4 salmon a day in total · 8 in possession", then "Chinook: 4 a day · only 2 over 62 cm".
- Owner spot-checked 5 places; they read correctly.
- Regions 5A and 7: DFO's "no salmon fishing" header now wins over 2025 table rows that were left on the page.
- Live on the staging feed now. Reaches phones with the 0.4.0 rebuild and APK.

**DFO wording safety net** [T321]
Every DFO wording we have ever seen (85 phrasings across 75 saved copies of DFO's pages) is in a list,
`data/curated/dfo_salmon/wording.json`.
- If DFO uses a new phrasing (for example, a notice that changes a quota in new words):
  - the owner gets an URGENT email with the exact words, place and notice number;
  - that row shows "DFO changed the wording here — read their text", with DFO's own words and no number.
- Three conditions that were being silently dropped are now kept:
  - hours written in words ("one hour before sunrise…");
  - "in the aggregate";
  - "(see exception below)".
- The list is read from storage on every run. After editing it, run `deploy/staging/sync-inputs.sh` (or ask Claude); no redeploy is needed.

**Feeds worker deployed to staging**
It now carries the per-species limits, the wording alerts, the offline gauge history (`obs.json`) and the crash-report changes.

**Sheldens Creek release + map/rules agreement** [T306]
- Staging is on release r-038e6818e80f, and the staging smoke check passes 18/18.
- The map colours and the rules pane now use the same tidal logic. Before, 99 tidal sections showed tidal in the rules but were coloured as normal water.
- Three sloughs (Maria, Hogranch, Nicomen) now draw at the right zoom.

**Data flow documented** [T324]
`handoff/PLAN/data-flow.md` traces every data stream from its source to the app, lists the problems found, and sets out the agreed end state:
- one final curated file per data type;
- review marks flow into those files automatically.

**Cleanup done so far**
- Ponds review closed [T329]. A pond call only ever confirms "lake", so nothing needs it.
- 16 machine-made review files moved out of `handoff/` into `data/generated/review/` [T330].
- Slough files are now in the right places [T322]:
  - the owner's slough verdicts are in `data/curated/sloughs/`;
  - the 811 confirmed calls are in `data/curated/waters/slough_calls.json`.

### Running now

**Rebuild → staging → Android checks → APK 0.4.0 (Play internal testing)**
The lane stops and reports if any check fails.
1. Rebuild with the DFO changes and the 08MH044 gauge fix.
2. Compare against the live release. Only those changes are allowed.
3. Upload to staging, then run the smoke check.
4. Emulator checks:
   - offline gauges and spots;
   - panning with and without spots;
   - the 15 s offline "Save spot" [T323].
5. Full test suite, then the APK to Play internal testing.

**Sloughs join their streams** [T322]: CODE DONE, switched off until its own side build.
Of the 811 confirmed calls, the dry run against today's map gives:
- 749 join one of 236 named streams.
- 31 Okanagan oxbows become general streams in place.
- 29 are skipped because they're isolated.
- 1 is held: wbk 329708509 touches only unnamed lines and belongs to no registry water, and the registry has no way to mark that as a stream yet.
- 1 was already a stream.

An automatic check replaces a review. It stops the promote if any water other than these, or the creeks that now flow into them, changes its rules.

What the plan said for this item:
- Confirmed sloughs join the stream that flows through them, including unnamed streams and streams that only enter or only leave.
- The 31 Okanagan oxbows become general streams.
- Only polygons no stream line touches are skipped.
- After the rebuild above, this gets its own side build. Claude checks that only those waters change, then promotes it. There is no owner review step (owner's call).

**Reviews save straight into curated files** [T325]: DONE (1a403a926).
Saving a mark now updates the curated file in the same save:
- Sloughs and names: instantly.
- DFO and regulation extents: a moment later, in the background. The app shows "saving to curated…".
- Tidal: still uses its current path until it gets its own file [T326].
- If a save is refused, the mark is kept and an amber "Mark saved, NOT in the curated file" list says why. The next save retries.
- There is no lock step. An empty queue reads "done". Ponds shows "Closed — feeds nothing".
- Catch-up command if marks were made while the app was off: `python -m pipeline.regs.review.apply`.
- Heads-up: one of your entry decisions is waiting to apply on your next entry mark. Hatzic, with tributaries included, widens rules r2/r3 from 4 to 92 sections.

What the plan said for this item:
Saving a mark in the review app will update that data type's final curated file in the same save: tidal, sloughs, DFO extents, regulation extents, splits and names.
- There is no lock step and nothing to run.
- If a save is refused, the mark is kept and the app shows why.

**Config tidy-up** [T327, T331]: BUILT on branch `cleanup/t327-t331`, waiting to merge after the rebuild.
- Owner name decisions now live in `data/curated/waters/name_calls.json`, not in code. The registry is unchanged.
- One upload list feeds both the staging script and the GitHub job; the GitHub job now uploads `wording.json`.
- Scraped and parsed data moved out of curated into `data/source/`:
  - gauge water types → `data/source/bc_station_waterbody_type.json`;
  - run timing → `data/source/runtiming/timing.json`.
- One unused file deleted (`cross_references.json`).
- The 7 retired marks are closed. Delivery reruns only when a file it actually reads changes.
- Left for later: four test-only path overrides need a small design of their own.

What the plan said for this item:
This runs on a separate branch so the rebuild isn't disturbed.
- Name decisions move out of Python code into a curated file.
- Every curated file gets registered in the config.
- Old files with no reader get removed.
- Merged after the rebuild is promoted. Old releases get pruned to the live one plus the previous one.

**iOS** [T313]
- The app builds and loads on the iPhone simulator; the download fix worked.
- Map, satellite and location pass.
- Search, offline toggle, walkthrough and PDF failures are being sorted into test-script issues and real bugs.
- Performance timings get recorded next to Android's. iOS goes after the APK.

### Needs the owner

- **Sheldens Creek:** accept the suggested cut at the Spur 19 bridge (53.12968, -131.78224) in the review app. Until then the app shows it as "not yet mapped".
- **DFO wordings:** review the 85 in `data/curated/dfo_salmon/wording.json`. Mark each `accepted` or `rejected`, especially the 15 "notes" shown as DFO's words.
- **Tidal:** 7 open marks plus Mamquam. After that, tidal becomes one file.
- **Existing items:** see `handoff/PLAN/OWNER-TODO.md` (salmon extents to review, gestures hand-check).
