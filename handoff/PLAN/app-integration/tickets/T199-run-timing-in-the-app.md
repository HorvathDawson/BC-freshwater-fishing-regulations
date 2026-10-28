# T199 — run timing in the app: implement and refine

Status: OPEN

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
