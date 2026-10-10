"""THE REGS STORE — `regs.sqlite`, the answers and the export words a client reads, as SQLite (S0).

    python -m pipeline.deliver store [--bundle FILE] [--export-dir DIR] [--out FILE] [--blobs CODEC]
    python -m pipeline.deliver.store build|check|measure …

A SIDECAR (plan APP-REGS-INTEGRATION, "Data architecture", phase S0): built in Python from the
answers file (`ui-rules-answers.json`, answers/2), the export pair it pairs with and the bundle's
part partition, with integer keys — one `akey` row per answers key, `cell` (akey x segment -> one
frame id per section), one table per deduplicated frame family (the answers' own interned indexes),
interned strings, a columnar `part` table replacing `display.waters`, `section_akey` (section handle ->
akey in u16 blocks), the export words the answers lack and `meta` digests. Nothing the page or the
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

#: The store's canonical path: beside the bundle it is cut from (like `status_index.bin`).
OUT = GENERATED.bundle / "regs.sqlite"

FORMAT = "regs-store/1"
