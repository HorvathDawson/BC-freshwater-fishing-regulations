"""THE REGS STORE — the answers and the export words a client reads, as SQLite (S0). It is the release step's input
and is not kept (P20): the release's water / regs-<region> / regs-sid files hold its tables, and `release.py`
(`province_store`) puts them back together for every Python reader. `regs.sqlite` is written only in staging, or to
`DEBUG_OUT` on request.

    python -m pipeline.deliver store [--bundle FILE] [--export-dir DIR] [--out FILE] [--blobs CODEC]
    python -m pipeline.deliver.store build|check|measure …

A SIDECAR (plan APP-REGS-INTEGRATION, "Data architecture", phase S0): built in Python from the
answers file (`ui-rules-answers.json`, answers/2), the export pair it pairs with and the bundle's
part partition, with integer keys — one `akey` row per answers key, `cell` (akey x segment -> one
frame id per section), one table per deduplicated frame family (the answers' own interned indexes),
interned strings, a columnar `part` table replacing `display.waters`, `section_akey` (section handle ->
akey in u16 blocks), `section_place` (the region set every section is in), the export words the answers lack and `meta` digests. Nothing the page or the
app reads changes: the store is ADDED beside them.

EXACTNESS IS THE GATE: `decode.answers_bytes(store)` rebuilds `ui-rules-answers.json` BYTE-IDENTICAL
(the answers CLI's serializer), `decode.export_subset(store)` equals `common.export_subset` of the
export pair by value, and `meta.store_digest` covers every table. `build` refuses to write a store
that does not decode back to its inputs.

  schema.sql  the format (one definition)
  common.py   serialisation, blob codecs, digests, the export subset, StoreError
  build.py    the stage
  decode.py   the reference decoder (answers bytes, export subset, section_akey lookups)
  measure.py  sizes per table and compressed (S0's report)
"""
from __future__ import annotations

from pipeline.common.curated import GENERATED

#: NO CANONICAL STORE (P20, T61 2.4): the store is the release step's input, made in `deliver all`'s staging directory
#: and dropped with it; the release ships its tables (water, regs-<region>, regs-sid) and every reader reads those
#: (`pipeline.deliver.store.release.province_store`). A copy is kept only on request — `deliver all --debug-json`, or
#: `deliver store` run alone — and goes here, never beside the bundle.
DEBUG_OUT = GENERATED.base / "debug" / "regs.sqlite"

#: 2 (S1): `place`, `akey.place`, and every section in B.C. keyed (`section_akey`).
#: 3 (S1b): keys by content (no `akey.place`); `place` is a region set; `section_place`. MUs are
#: not shipped (the bundle's `mu_set` / `section_ruleset.mus`; the tiles' MU layer).
#: 4 (answers 2.7): `part.short_s`, the part strip's short name.
FORMAT = "regs-store/4"
