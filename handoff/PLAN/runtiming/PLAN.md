# Run timing — integration plan (T162; owner 2026-10-24: data + plan only, the app waits for the owner)

What exists: `python -m pipeline.runtiming.parse` → `data/curated/runtiming/timing.json` (tracked), from the PSF
Explorer page (gitignored) and DFO NuSEDS (`fetch/nuseds.py`, gitignored). Nothing reads it yet. This is how it would
be fed. Every number below was measured on 2026-10-24 against the live atlas (`a1944923732ccbfc`) with a prototype
that was then reverted; it is not in the repo.

## Sources and credit

| | what | dated at | terms | credit (owner's words) |
|---|---|---|---|---|
| PSF | dataset #90, 329 CU curves (of 463 CUs) | river entry | PSF's written permission (2026-10-24; the reply is not stored) | Pacific Salmon Foundation (Salmon Watersheds Program) |
| DFO | NuSEDS, 4,944 stream × species × run records (≥3 dated survey years, the latest ≤25) | in the stream | Open Government Licence – Canada | Fisheries and Oceans Canada |

Best source per species: on a stream DFO surveyed, DFO replaces that species' PSF *spawning* curve; passing PSF runs
stay; steelhead (not in NuSEDS) and unsurveyed water take PSF. Credit on every surface that shows a timing (the
sheet's foot) and a "Fish run timing" block on Sources and credits.

## 1. Binding (a delivery step, never a frozen index)

`pipeline/deliver/runs.py`, a step of `deliver all` after `status_index`, reading `timing.json`, the CU polygons
(`parse` stitches them into `data/generated/runtiming/cu_polygons.geojson`), `graph.pkl` and the tiles' own
`layers/{stream,lake}.geojsonl`. Bound at every delivery, so section ids can never go stale (the 2026-09-08 index did).
Peak ~graph load; ~2 min.

| relation | rule | measured |
|---|---|---|
| spawning (PSF) | section midpoint in the CU polygon (simplified 0.001°), AND a stream of Strahler order ≥ 3 or a lake with an outflow | 262,256 sections; unfiltered it was 948k (509k first-order, 189k ponds) and 2 MB gz |
| passage (PSF) | flow graph walked DOWN from the spawning sections; each step's FWA code an ancestor of a spawning one (stops the Nass walking up the Skeena); drainage roots < 1 % and < 100 sections dropped first (polygon spill) | 178,802 sections |
| observed (DFO) | sections of the site's FWA code that carry its name (any of the atlas's names); one code level down by name if none (NuSEDS's Chilliwack = the FWA's Vedder code; the FWA's Chilliwack is `…-998041`); a water over 150 km binds within 50 km of the site; several sites: nearest, then most recent | 4,735 records on 9,336 sections |

Without the name rule a slough's site landed on its river (Esker Slough coho on 71 Skeena sections). km to the sea
must follow the section's own blue line, not the shortest way out (a braid's dead end read 0.8 km 48 km up the Skeena).
Spot checks: Skeena summer + winter steelhead; Babine DFO sections 1-3 / 4 / 5; Chilliwack DFO fall + summer chinook
(closes review.json's "no PSF chinook" gap — on the Chilliwack's pieces, not the Vedder's).

## 2. Release file `runs/1` (beside `lakes/1`, optional in the contract)

`BCRN`, u8 1, 8 bytes section_handles; LEB128 header length + UTF-8 JSON {slots, sources (credit, title, updated,
licence), runs [{s, sp, slot, n, l, pk, w, q, y}], profiles [[run × 3 + relation]]}; runs × 73 curve bytes (0..100);
count; per section (sid ascending): sid delta, profile, tens of km to the sea + 1. 435,173 sections, 4,822 profiles,
**865,961 B gzipped** — under a 900 kB per-file gate, but the release total would be ~14.92 MB of the 15 MB gate
(owner call: raise the gate, drop km on spawning-only sections (~180 kB), or bin coarser).

## 3. App surfaces

- Layers → Colour the water by → **Fish runs** (needs the release's `runs`, read off the manifest; fetched only when
  chosen, so first paint is untouched). Species row under it (steelhead default, Q144), saved with the layer choice.
- The colouring engine's `runs` channel, feature-state `run`: 1..100 a local run at that share of its peak (solid
  ramp), 101..200 only a passing run (paler ramp — a dash cannot vary per feature), 0 recorded but not in the water on
  the date, no key = "No steelhead run recorded" (never "no fish"). The date pill drives the pentad; pink picks the
  year's brood line. Web map only (native has no per-tile event, as the lake modes); 8 new `color.run.*` tokens per
  theme (two first picks failed the ΔE ≥ 12 gate: dark `off` vs `mask`, light `r2` vs `temp.warm`).
- Under Fish runs a tap opens a **runs sheet**: per run, species · run label, CU / stream name, relation ("Spawns in
  this watershed" / "Passes through here" / "Seen in this stream (DFO spawner surveys)"), dates in its source's terms,
  "In the water now" or not, DFO survey years, "PSF dates are when fish enter fresh water. This water is about N km
  from the sea, so they reach it later.", the credit; "Open this water" continues to the water screen.
- Gate snag found: `tools/check-colours.mjs` reads any line holding "Pink" as a colour context (`ink`); its context
  needs `(?<![a-z])ink`.

## Open questions (owner)

- **Q144**: default species (steelhead?), passage shown at all (paler ramp, not dashed), own layer vs inside Conditions.
- The 15 MB total gate (above).
- Native colouring waits on N2's per-tile labelling.
- Yearly refresh, owner-run: `fetch.explorer`, `fetch.boundaries`, `fetch.nuseds`, then `parse`; commit `timing.json`.

Prototype (reverted, not applied): `integration-prototype.patch` beside this file — the app/deliver wiring the numbers above came from; a starting point if the owner says go.
