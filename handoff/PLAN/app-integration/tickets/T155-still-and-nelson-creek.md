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
