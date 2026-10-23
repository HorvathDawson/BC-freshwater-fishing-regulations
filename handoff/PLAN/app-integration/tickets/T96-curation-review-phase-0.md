# T96 — Curation review, phase 0: make the tool safe; the 5 entries that fail the gate

Status: OPEN.

Source: handoff/PLAN/curation-review/PLAN.md §4 "Phase 0" and §6 Q1 (a3f66b7c, T66). Defect found there: 5 entries
fail the gate and `validate_catalogue` — Shuswap Lake `min_cm` 50/60 "not in own verbatim"; `zp:set_lining`,
`zp:spear_fishing`, `zp:bait`, `zp:white_sturgeon_licence` "exempts share no ground" — so ingest for region 3 and
prov is broken today.

## Work
- Delete `backend/rebuild.py`, `/api/rebuild*`, `RebuildButton` (no endpoint starts a subprocess).
- Vite `host: "127.0.0.1"`; CORS limited to the Vite origin.
- Send each split's `_note` and auto-proposal grade to the frontend; move the Hwy 19 bridge note off
  `adam_river__eve_river_into_adam_river`.
- Fix or remove `AnswerPanel`; correct 117 → 132 (`QueueList.tsx:28`, `reuse.py:852,869`, README), the splits path
  in `API.md`, `graph.gpkg` → `.pkl` in the README; show path-less errors at the top of the entry.
- Curation API, writes, fingerprint and ingest tests into `pipeline/tests/smoke.txt`.
- The 5 gate failures: check Shuswap against the book (printed pages) and fix the data; for the four `zp` exemptions,
  loosen the gate only if the book really exempts across ground they don't share. Never run the parser.

## Acceptance
PLAN.md Phase 0 "Accept when": no endpoint starts a subprocess; the server is not reachable from another machine; split
notes show in `SplitEditor`; all 5 entries pass the gate and `validate_catalogue`; smoke tier green (run unfiltered).

Depends: none. Size: S. Owner: the Shuswap/zp data calls (Q33). Default: as above (fix Shuswap data; loosen the gate only
where the book exempts across unshared ground).
