# T160 — Disclose a gauge's standing backwater (08HB074, Comox Lake) with its own Level sentence

Status: DONE — cb55b47a. Rebuild: deliver (bundle → release) for the flag to ship; no atlas or reach run.

Source: Q49 decided by Claude 2026-10-23 (owner's T78 rule: disclose tidal OR backwater influence where the record shows
it). 08HB074's HYDAT record names a standing "variable backwater from Comox Lake"; the tidal sentence is the wrong one.
Add a backwater row (checkable reason) and its own Level sentence; episodic ice/beaver/slide remarks stay unread.
Ships: deliver + app words. Size: S.

## As built (cb55b47a)
- AUTHORED row, checkable: `data/curated/gauges/backwater.json` (config `curated.gauges.backwater`, AGENTS 36) — one
  station, 08HB074 Cruickshank River near the mouth: `from` "Comox Lake", `remark` = HYDAT's standing station remark
  verbatim ("STATION SUBJECT TO VARIABLE BACKWATER FROM COMOX LAKE, NECESSITATING EXTENSIVE ESTIMATED PERIODS", type 4,
  repeated in the annual type-2 remarks), `why`.
- `pipeline/gauges/consume/tidal.py::load_backwater` / `backwater_stations`: raises on a missing file or HYDAT, on a row
  without from/remark/why, a `from` that is not a lake, a quote that does not say BACKWATER and name the lake, a quote
  that is not one of the station's own `STN_REMARKS` (case/spacing aside), and a station also flagged tidal (one Level
  sentence per gauge). Never read from the remarks by pattern — the ice/beaver/landslide backwater stays unread.
- Bundle: `gauge.backwater INTEGER NOT NULL DEFAULT 0`, added LAST after `tidal` (still conditions/1; an older release
  reads 0); release contract + `pack.generated.ts` carry the column; dev fixture regenerated from schema.sql.
- App: `PanelRoute.backwater?: true` (`placesOf`/`routesOf`, absent column = not flagged); core
  `BACKWATER_NOTE`/`backwaterNote` — Level view only, only about a reporting member: "One of these gauges sits where a
  lake backs up into the river: its level rises and falls with the lake as well as with what this river carries.";
  `donorView.backwater` → a `Caveat` in `DonorPanel` beside the tide's. Whole strings pinned in core, ui (donorView)
  and ui-native (mounted panel, Level shows it, Discharge does not).

## Measured (scratch deliver: `pipeline.deliver bundle` → scratchpad T160-deliver/, against atlas/full + reaches/full)
- gauge: 2,324 rows, `tidal` 7 (unchanged), `backwater` 1 — exactly 08HB074 (gnis:36224, sid 57475). Every other
  column of the gauge table is byte-identical to the live bundle (md5 of all rows/columns but `backwater`).
- 08HB074 is real-time and active (levels through 2025); it gauges 584 sections and sits in the panel of 373 sections,
  so the sentence can appear on those sections in the Level view while it reports.
- HYDAT check: of every BC station remark naming backwater (ice/beaver/tide excluded), 08HB074 is the only STANDING
  lake backwater; the rest are the 2024 Chilcotin landslide (08MA001, 08MF073–076, episodic) and "max instantaneous
  discharge not valid — backwater" (08HF012/014). None added.

## Adversarial review (2026-10-23)
- Mutations: dropping the check against the record, the tidal refusal, the quote check, `donorCounted`, and the
  `routesOf` flag each fail a test. Dropping the "`from` must be a lake" check was NOT caught (the bad case also failed
  the quote check) — fixed the case so only the lake check refuses it; now caught.
- Second copies: the sentence lives once (core `BACKWATER_NOTE`); the flag is read once (`placesOf`); every surface
  that says the tide (`donorView` → `DonorPanel`) now says the backwater beside it — no other surface says the tide.
- `pnpm check` (REGS_STORE_REQUIRED=1, via heavy.sh): every gate through typecheck passes; tests 8,030 pass, 4 fail —
  all x4p_1010 (Shuswap) regs conformance (caveats/keep/sheets parity), unrelated to this change and pre-existing on the
  lane's base ef829635: the same three files pass with 1596dd19 (T100 conformance rewrite, already on the dev branch)
  applied. The dev fixture (`packages/data/dev/bundle.sqlite`) is regenerated for the new column (deterministic).

## Owner questions (built with the default; say if not)
- None new. Q49 was decided 2026-10-23 under the owner's T78 rule; the sentence's words are the only judgement call
  (default: the words above, parallel to the tide's).
