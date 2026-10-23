# T73 — Smaller reach hand-off (audit N14)

Status: OPEN. Pressing: disk.

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
