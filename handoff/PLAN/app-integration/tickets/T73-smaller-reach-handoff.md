# T73 — Smaller reach hand-off (audit N14)

Status: PARTIAL — code + tests in (see Outcome); the full-run conversion and its numbers are the owner's heavy run.

Source: `data/generated/reaches/full/rule_section.jsonl` is ~20 GB (20,100,603,190 B on 2026-10-22); the reach step
writes plain JSONL (`atlas/reach/io.py`); full_twin doubles it (~95 GB with copies); the disk was 89% full (103 GiB free)
during the T56 rerun.

## Work
Reach writes SQLite or interned gzip JSONL; the bundle and the binding diff (`e398ef77`, test_binding_diff.py) read it.
Record disk and time before/after. Run through scripts/heavy.sh, alone.

## Acceptance
Binding diff old vs new format: identical bindings. Size and time before/after recorded.

Depends: T56 (swap full_twin in first); not at the same time as T61's name normaliser (that moves bindings).
Size: M (heavy). Owner: no.

## Outcome

PARTIAL 2026-10-24. Chosen: interned gzip JSONL (no new dependency, still a readable diff after `zcat`).

- `atlas/reach/io.py`: the reach step writes `rule_section.jsonl.gz` — one line per bound RULE,
  `{"entry_id","rule_id","sections":[sorted],"trib":[subset]}`, gzip mtime 0 (byte-identical reruns) — generated rule by
  rule, no longer 149 M row dicts in memory. `rule_section_file(run)` (gz, else a pre-T73 `rule_section.jsonl`; neither
  or both = refused) + `iter_rule_sections(path)` (same (entry, rule, section, scope) rows, same order, out-of-order
  refused) are the one read. Readers moved: bundle (`bundle/rules/tables.py`), `binding_diff`, the parity gate
  (`regs/review/parity.py`), `io.read_run` (reach `--against` / diff), `pipeline/build.py` outputs. A rerun into an old
  run's directory removes the old file.
- `python -m pipeline.atlas.reach.io convert <run> [--to DIR]`: rewrites an old run, streams both forms side by side
  (any differing row → the new file is removed), then drops the old one (`--to`: leaves the source, copies the rest).
- Tests (`test_binding_diff.py`, no data): round trip + byte-identical; old form vs converted = "no binding changed",
  with mutations (a lost row, a rescope) seen; write_run writes only the gz and every reader takes it; out of order /
  both files / neither / a bad scope refused. `test_reach_builder` byte-identity moved to the gz.
- Measured on the first 3,000,000 rows of `reaches/full/rule_section.jsonl` (in a scratch copy): 357,410,465 →
  10,845,191 B (33×), write 2.2 s + verify 1.7 s, 144 MB RSS; binding_diff old vs converted: no binding changed.
  Extrapolated to the full 19.17 GB: ≈ 0.6 GB, ≈ 2–3 min each way.

Left (the owner, alone, through heavy.sh — reads 19 GB, writes ≈ 0.6 GB):
```
scripts/heavy.sh t73-convert 30 -- python -m pipeline.atlas.reach.io convert data/generated/reaches/full --to /tmp/full_t73
python -m pipeline.atlas.reach.binding_diff data/generated/reaches/full /tmp/full_t73     # expect: no binding changed
scripts/heavy.sh t73-convert 30 -- python -m pipeline.atlas.reach.io convert data/generated/reaches/full data/generated/reaches/full_prev
du -sh data/generated/reaches/*                                                           # record before/after here
```
(`convert` prints rows, old/new bytes and seconds per run; the reach step's own time is unchanged in kind — measure it
on the next rerun.) Then the ticket is DONE.
