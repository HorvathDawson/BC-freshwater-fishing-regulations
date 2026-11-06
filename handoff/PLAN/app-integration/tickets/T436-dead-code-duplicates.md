# T436 — Dead code and duplicate helpers (tidy-up)

Status: OPEN — from the audit (T333–T335, 2026-11-05). Severity: **low**. Verified by a second reader.

Source: `handoff/PLAN/audit-2026-11-05/` (findings: pipeline 2, 7, 17, 18; app 9, 10, 11, 16, 18; `*.verified.md` has the evidence per finding).

## What happens
Dead: `runtiming/generate/{index,passage,profiles,indicators,crosswalk}.py` + `consume/bundle.py` (no importer;
`runtiming/__init__.py` describes the retired design); two unreferenced audit modules; `generated.scratch` has no
readers; `tile-ids.native.ts` (only a test reads it); a dead name-match branch. Duplicates: section-id parsing in ~7
places (they agree today); the fly/bait marks listed 3× in `map` (`paint.ts:150`, `contract.ts:205`,
`runtime-style.ts:172`) instead of core's `GEAR_LEGEND`; the report "platform line" built in 5 places;
`PanelMember`/`BasinMember` in core vs data types. Nothing stops shared code importing `map/src/web|native/`.

## Fix
Delete the dead (git keeps them); one section-id parser; map reads `GEAR_LEGEND`; one platform-line helper; one
member type; check-platform refuses imports from `map/src/web|native/` outside the pair files.

## Acceptance
Each listed defect fixed with a test that fails without the fix (or a recorded "no" with the reason). Owner picks
when; no fix in the audit run.

Size: S. Owner: no (picks the order).
