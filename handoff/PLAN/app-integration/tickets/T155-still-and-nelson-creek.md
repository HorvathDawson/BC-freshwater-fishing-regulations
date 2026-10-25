# T155 — Still Creek and Nelson Creek: book entries with no water (ditch/canal lines that are no item)

Status: OPEN

Source: T148 census table C, 2026-10-23.

## Problem
r2:still_creek@2-8 (Still Creek: 5.2 km of FWA canal + ditch lines) and r2:nelson_creek@2-8 (1.1 km of ditch) bind
nothing: their named lines are not stream items and have no sections. Also Deep Creek (15.7 km of ditch) is a stream
item that r6:deep_creek names but does not reach through the item.

## Work
Make the named ditch/canal lines stream items (registry; a ditch or canal with a GNIS name is a stream by the book),
check each entry binds after a rebuild, and explain Deep Creek. Measured binding diff.

Rebuild: atlas registry + reach + deliver.

## Outcome
No code change: measured 2026-10-24 on the live bundle + atlas, all three entries bind, and the census's "no water" was
its FWA-line view only.
- `r2:still_creek@2-8` binds **99 sections** (4 rule sets): its water is the municipal Burnaby Still Creek
  `wsc:100-019698-999999` (added streams). The FWA canal/ditch blue line 356328804 is absent from the graph BY DESIGN —
  `added_streams.build_dataset.FWA_EXCLUDE_BY_SOURCE["burnaby"]` drops every FWA reach upstream of the Brunette
  (`100-019698-…`) because the municipal layer supersedes it.
- `r2:nelson_creek@2-8` binds **6 sections**: `gnis:20408`, West Vancouver's Nelson Creek (blk 360679993, MU 2-8). The
  1.1 km ditch (blk 356013125, WSC 100-019698-194371) is a different Nelson Creek inside that same excluded Brunette
  watershed in Burnaby; the book row is not about it.
- `r6:deep_creek@6-15` binds **9 sections**: `gnis:3983`, Region 6's Deep Creek. The 15.7 km ditch Deep Creek is
  `gnis:20175` in Region 8 (MUs 8-22/8-26, the Armstrong/Spallumcheen valley) — a different water with no row; it is
  already a stream item.
- Every other named ditch/canal blue line (110) is already a stream item (census table C); the only two that are not
  are the two Burnaby ones above. Nothing to make an item, nothing to rebuild for this ticket.
