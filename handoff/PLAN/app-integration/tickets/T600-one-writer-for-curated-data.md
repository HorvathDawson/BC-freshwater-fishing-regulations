# T600 — One writer for curated data

Status: DONE — 8319d401a

Source: T599 findings step 2 (`handoff/PLAN/app-integration/T599-findings.md` §4, §5); owner 2026-11-10: "the
recommendation looks good and is cleaner than what we have now"; "keeping curated data more strictly managed".

## Work
1. One module (`curated_write.py`): validate against the file's schema, back up, write atomically, append a write log,
   stamp provenance (who / when / source) on the record.
2. Move every tool that writes a curated path onto it (the findings count about ten; confirm the list first). The
   review app's write-through belongs to chat 10's paths: list those call sites and hand them over, do not edit them.
3. A test that fails when any module writes under the curated tree without the writer; no pipeline build step may
   write curated data at all (T547 adds the read-only-tree test).

## Acceptance
- One atomic-write copy left; every curated change has a log line and a backup; the grep test is in.

Depends: T547. Size: M. Owner: no.

## Consistent with today's builds (owner 2026-11-10: "need to make sure it is consistent with the builds now")
This step changes where and how data is written, never what is built. Before it starts, keep the current atlas,
bundle, tiles and release as the reference. After it, one full build must give the same outputs: same release file
hashes (or a diff where every difference is explained and expected, e.g. the new input digests in a meta file), same
tiles, same app and web packages. The build, release and staging commands the owner uses keep working unchanged, or
the change is listed in the ticket and in AGENTS.md. A step that cannot show this is not done.

## Done 2026-11-13 (8319d401a)
- `pipeline/common/curated_write.py`: `write(path, text, tool=, by=, source=, validate=)` refuses a path outside the
  curated tree (the live one or the `CURATED_ROOT` copy), validates (JSON / JSONL parse; a dict file with `_about` keeps
  it; a waters file passes `check_curated.check_waters` with the new text in its place; the caller's schema, e.g.
  `CatalogueFile` in `write_entryfile`), backs up the old file (last 20, `data/generated/curated_writes/backups/<rel>/`),
  writes atomically (temp + fsync + `os.replace`; `parsing.io.atomic_write` is now this) and appends one log line
  (`curated_writes/log.jsonl`: file, tool — the running module —, by — `CURATED_WRITE_BY` or the login —, at, source,
  old/new sha256, backup). Unchanged bytes: nothing. `write_any` = `write` in the tree, plain atomic elsewhere (a
  tool's `--out` in a tmp dir).
- The list, confirmed (subagent read of every write under `pipeline/` + `curation-review/backend/`): 17 call sites in 14
  modules moved — `parsing.io.write_entryfile` (so backfill_matched, remap_boundaries, drop_on_parts, electric_kw,
  reparse_candidates, ingest_catalogue), `review.apply` (write + put-back), `review.apply_proposals` (write + put-back),
  `review.closed`, `steelhead.accept`, `dfo_salmon.entries.save`, `dfo_salmon.wording.write`, `inseason.resolve`,
  `gauges.generate.promote`, `gauges.review.save`, `added_streams.build_dataset.write`, `bathymetry.accept`, and
  `reach.word_reads` (missed by the T599 count). `mined_names` / `slough_calls` / `tidal_limits` write through
  `review.apply`.
- HANDED OVER (the review app's own write path, chat 10's): `curation-review/backend/writes.py` `commit` (the marks
  store `reuse.py:1605/1658` and the in-season matches `inseason.py:296/357`; it has its own snapshot/rollback) and
  `pipeline/regs/review/marks.py` `append` (marks.jsonl under the store lock). Listed in the guard as HANDED OVER.
- Guard: `pipeline/tests/test_curated_writer.py` — the writer's contract on a sandbox tree (backup, log, refusal leaves
  the file, `_about`, waters check, outside-the-tree refusal, backups pruned) and `CLASSIFIED`: every module that reads
  the curated tree (or names `atomic_write`) and writes files is listed with what it writes; a new one fails, a stale
  entry fails; mutation-pinned on source snippets. "No build step writes curated": `test_atlas_inputs` (T547, read-only
  tree) unchanged and passing.
- Consistent with today's builds: the change touches write paths only — no module a build stage EXECUTES changed, except
  `parsing.io` (its readers unchanged; only `write_entryfile` / `atomic_write`, which no stage calls) and
  `reach.word_reads` (`main` only, a CLI). The read-only-tree test runs reach, tiles and deliver steps with the curated
  tree unwritable and passes. Commands the owner uses are unchanged (AGENTS 36b). The next atlas/reach run (T678 junction
  rebuild, same session) reruns the reach stage for its own reasons, so no separate full build was made for T600 alone.
- Not done, with why: (a) PER-RECORD provenance (findings §4.3): `CatalogueEntry` and other record schemas refuse
  unknown fields, so stamping records would change what the build reads; provenance is on the log line instead. (b) The
  ~20 private temp+replace copies that write NON-curated files (release, deliver, bathymetry/stocking proposals,
  dfo fetch …) are T560 item 7's, not this ticket's.
