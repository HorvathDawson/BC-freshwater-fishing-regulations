# T199 — run timing in the app: implement and refine

Status: PARTIAL — feed + Run timing mode built and on staging (owner 2026-10-28); data refinement (step 2) open

Source: owner 2026-10-25 — "implementing and refining the run timings into the app. will be at end, alongside the
stocked and bathy maps." Follows T162 (data + plan, PARTIAL).

## Inputs
- `data/curated/runtiming/timing.json` (329 PSF curves, 4,944 DFO NuSEDS records), parsed by
  `python -m pipeline.runtiming.parse`; yearly refresh = `fetch.explorer`, `fetch.boundaries`, `fetch.nuseds`, then parse.
- `handoff/PLAN/runtiming/PLAN.md` (binding runs to sections at delivery, the `runs` release file, the Fish runs layer
  and tap sheet, credits) and `integration-prototype.patch` beside it (reverted prototype; a starting point only).

## Work
1. Owner answers first: Q144 (steelhead first, passage on a paler ramp, own layer). (Go given by the Source line,
   owner 2026-10-25; the release budget is no longer a blocker — T182 replaced the 15 MB total with per-file gates.)
2. Refine the data before shipping: check curves against DFO escapement where both exist, flag low-confidence CUs,
   decide how passage vs spawning reaches show.
3. Implement through the bundle only (no runtime reads of curated files): deliver step, release file, app layer +
   sheet, credits "Pacific Salmon Foundation (Salmon Watersheds Program)" and DFO (Open Government Licence – Canada).
   PSF's permission reply is never stored or quoted.
4. Tests + a binding/size diff; owner look on the phone.

Band: last ("Data feeds at the end"), with T200 and T201.

## Outcome (2026-10-28, 318a07dd) — PARTIAL: its own stream + the Run timing mode; step 2 (refining the data) not done

Owner 2026-10-28: start T199 as ITS OWN STREAM, not the regs / bundle / release; Q144 answered with the defaults
(steelhead; spawning solid, passage dashed; its own mode with its own paint index). The release plan of T162
(`runs/1` in the release, PLAN.md §2) is superseded.

### Data contract (`pipeline/runtiming/feed/__init__.py` is the source)
R2 `runtiming/` on the data origin (staging: https://data-staging.canifishthis.ca/runtiming/index.json):
- `index.json` (`runtiming-index/1`, no-cache, uploaded LAST): `section_handles`, `tile_version`, sources (credit,
  updated, fetched), `files.{runs,paint,waters}` = `{url, bytes, sha256}` of the DECODED file (files are gzipped,
  immutable, content-named `<name>-<sha12>.json.gz`; the app's `fetchVerified`).
- `runs` (`runtiming-runs/1`, 97 kB gz): every bound run (312 PSF CUs + 4,574 DFO stream records): species, slot (pink
  by brood line), CU / stream name, label, peak, window, PSF quality / DFO survey years, `months` (12 bits) and
  `pentads` (73 bits) in the water (PSF >= 10 % of peak, DFO >= 25 % of years), source; sources with credit + terms
  (PSF "used with the written permission", DFO OGL-Canada); slots; default = Steelhead; relation words.
- `paint` (`runtiming-paint/1`, 821 kB gz): THE PAINT INDEX, bound to ONE atlas: 4,673 profiles (run x relation set +
  species bitmask + per slot [local months, passing months]) and per section (435,405, sid-delta coded) its profile and
  tens of km to the sea. No join in the app; refused when its handles are not the tiles'.
- `waters` (`runtiming-waters/1`, 2.5 MB gz): the same binding with NO section ids — reaches as `item_id` (null for an
  unnamed stream) + FWA blue line + measure range (a lake: its waterbody key) + profile; `run_ids`. Durable across
  rebuilds; not read by the app.
Binding (`feed/bind.py`, the T162 prototype's rules on a base): spawning 262,220 sections, passage 179,295, DFO
observed 9,333 — the prototype's numbers. 17 PSF CUs bind nothing (no order >= 3 water or draining lake in BC).

### Workflow (`.github/workflows/feeds-runtiming.yml`)
Manual (`workflow_dispatch`, `upload` default false); a monthly schedule written and commented out; staging only (the
exact bucket guard of feeds-sync-inputs.yml; secrets CLOUDFLARE_ACCOUNT_ID / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY).
Steps: the live release's handles (bucket `release/manifest.json`) → the BINDING BASE for them
(`private/runtiming/base/<handles>.json.gz`, never served) → fetch PSF page + CU tiles + NuSEDS → parse → build
(+ checks) → upload files then `index.json`. The atlas cannot run on a runner: the base (`feed/base.py`: the atlas
reduced to kept sections — order >= 3, draining lakes, named waters and everything below them: 715,197 of 1,957,984,
16 MB gz) is built on the Mac. `deploy/staging/upload.sh` now spares `runtiming/` when it clears the bucket.
Run locally once 2026-10-28 (fetch → parse → base → build → `feed.publish --via wrangler --yes`): live on staging.

### App
Map tab corner: Standard · Salmon · **Run timing** (`RegsLayerToggle`, the CornerStrip beside the day strip); under it
a species row (steelhead first). The feed is read on the first press (Loading… meanwhile), from `${base}/runtiming`
(`httpRunTiming`; the last capture kept: Cache API on the web, a file on a phone). The colouring engine's `runs`
channel labels each section `rt` = spawn / pass / off for the species and the month of the map's day (a change
relabels; no style change); mode `runs` on stream / lake / lake_flowing / wetland_flowing; passage dashed by the
runtime layer `stream-runs-pass`; tokens `color.run.{spawn,pass,off,none}` + `dash.run.pass` per theme. Legend: species
+ month, the states, no run recorded, the credit; "made for another map version" when the handles differ. A tap opens
the runs sheet (`RunsSheet`, words in core `runSheet`: every run here, the chosen species first, relation, dates in
its source's terms, in the water on the day or not, DFO survey years, river-entry + km line, credit, "Open this water").
Shots: `T199-shots/` (desktop + phone, Chilliwack / Fraser, Dec 15, against staging).

### Tests
`pipeline/tests/test_runtiming_feed.py` (bind → files, section-free waters, checks refuse a mismatch);
`core/runTiming.test.ts` (states, species row, pentads, the sheet's whole strings); `data/feed/runtiming.test.ts`
(verified fetch, capture offline, unchanged index fetches nothing, a bad file refused); engine / native-style pins.

### Owner steps
1. `gh workflow run feeds-runtiming.yml --ref redesign/stream-sections -f upload=true` once; read its report.
2. After every atlas promote: `python -m pipeline.runtiming.feed.base --upload --yes` (Mac), then step 1.
3. When happy, uncomment the monthly `schedule:`; production stays commented out (and `feed.publish` names staging only).

### Not done
Step 2 of Work (curves vs DFO escapement, low-confidence CUs); a native device run (web + phone-web only); pruning old
content-named files on R2 (a few MB a refresh).

