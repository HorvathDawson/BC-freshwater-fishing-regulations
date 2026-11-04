# T357 — DFO release / feed: silent fallbacks that hide missing data

Status: DONE (Python side) 2026-11-03; worker's dfo-feed/1 upgrade waits on a prod R2 check (found by T350 lean pass, 2026-11-02).

- **Header notes missing → no header closure** (`pipeline/deliver/salmon.py:518`): when no cached page matches the
  feed's `date_modified`, the release ships NO header notes for that region, said only on stdout. With no notes,
  `quota.header_closure` finds nothing, so a region whose header closes salmon (5A, 7) answers from the table's 2025
  rows (open days). Fix: fail the build (or keep the header with the typed feed so it cannot go missing).
  **DONE in T366 (2026-11-03):** the typed feed carries `notes`; `load_headers` reads them, else the dated page, else raises.
- **Corrupt previous feed read as none** (`pipeline/regs/dfo_salmon/live.py:234` `_read`, broad except): a truncated
  index.json drops every carried answer silently. Only "missing" should mean none; unreadable should stop the run.
- **Leftover format upgrade** (`live.py:291` `_upgrade`, dfo-feed/1 → /2; the worker has the same): remove once no
  dfo-feed/1 is in R2.
- **Stale `item_ids` cache** read by deliver (`salmon.py:394, :405`) and the review backend
  (`curation-review/backend/dfo.py:184, :216, :363, :449`) — already T337; do T337 first.

Size: S.

## Done 2026-11-03 (chat 5)
- Header notes: T366 (b71bbcd11) — typed files carry the page's notes; `load_headers` raises when neither they nor a
  same-dated page exist.
- `live._read`: only a MISSING file is "none"; an unreadable one stops the run (names the file).
- `live._upgrade` removed: `_same_format` refuses a last feed that is not dfo-feed/2 (local and staging are /2). The
  worker's twin (app/workers/feeds) stays until prod R2 is checked to hold no dfo-feed/1 (prod is the owner's).
- Stale `item_ids` cache: T337 (done in batch 1).
- Note: `test_dfo_live.py::test_a_dry_run_writes_nothing…` reads the curated region-2 entries; with the queued (uncommitted)
  Fraser change it fails until the owner confirms the places (then re-pin).
