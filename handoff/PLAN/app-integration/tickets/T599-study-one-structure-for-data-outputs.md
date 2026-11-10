# T599 — Study: one structure for the pipeline's outputs, side files and intermediates (Zarr or other)

Status: OPEN

Source: owner 2026-11-10: "if we should restructure the data outputs and side cars and intermediates etc to all live in
a nicely structured ZAR or similar maybe? We should see if it makes sense. Would it give us benefit … other options
should be considered too because we want it to be easy to fit and manage and easy to expand."

## Work (study only; no data or code moves)
1. Inventory what the pipeline writes today: every output, side file and intermediate under `data/` (and anything
   written elsewhere), with format, size, writer, readers, how often it is rewritten, and whether it is curated, built
   or cached. Note where the same fact lives twice and where a reader guesses a path or a vintage.
2. Name the real pains from the repo's own history (STATUS.md, memory notes, tickets such as T547, T549, T555, T560):
   stale vintage pairs, silent fallbacks to curated files, hand-kept side files, slow rebuilds, hard diffs.
3. Compare options against those pains and against "easy to manage, easy to expand":
   - Zarr (what kind of data it suits; whether any of ours is that kind).
   - Parquet / GeoParquet with DuckDB as the reader.
   - One SQLite or GeoPackage per stage.
   - Keep today's formats but put them under one layout with a manifest per build (inputs, hashes, vintage, schema
     version), one reader/writer module, and schema checks.
   - A mix (say which data goes where and why).
   For each: what gets better, what gets worse, what it costs to move, what it does to git diffs of curated data, to
   the app release files (those stay as the app reads them), and to the tools the owner already uses.
4. Recommend one, with a staged plan (smallest first step that pays for itself) and what not to move.

## Acceptance
- One findings file, `handoff/PLAN/app-integration/T599-findings.md`: the inventory table, the pains, the comparison,
  one recommendation, the first step. Plain words; the owner decides from it.

Depends: —. Size: M (study). Owner: decides after the findings.
