# T350 — Quick audit: DFO salmon pipeline unified with the freshwater (trout) model, no fallbacks

Status: OPEN, starts after T349 lands (owner 2026-11-02: "a quick audit of the salmon DFO pipeline to make sure we don't
have bad datatypes or tech debt deviating from other datatypes like the trout freshwater ones … unified … solid … no
weird fallbacks").

**Scope:** pipeline/regs/dfo_salmon/*, pipeline/deliver/salmon.py, app/workers/feeds/src/dfo/*, app/packages/core
salmon*.ts, ui-native SalmonCard / RegionRules, and the review app's DFO / cascade backend.

**Look for:**
- **Data types that differ from the freshwater ones for the same idea:** quota / size / season (`when`) / gear /
  species codes / extents / water binding / tributaries / notes / provenance.
- **Two code paths for one answer:** Python vs TS beyond the tested parity, duplicate parsers, a second cascade.
- **Silent fallbacks:** defaults that hide missing data, broad excepts, "if missing use X", a stale cache read
  (e.g. the `item_ids` cache, T337), env overrides.
- **Dead fields and code, leftover versions,** tests that can't fail.

**Output:** a ranked findings list (file:line, why, fix), each checked by a second read. Small, safe fixes are applied
with targeted tests. Bigger ones become tickets.

Size: S–M, kept lean.
