order: 10

## 2026-11-20 — lane L7 (T678 / T679 build stages): queued while the owner slept
- T679 #12: should the bundle carry each rule's own tributary scope (today `includes_tributaries: false` is dropped, so
  "inherit the entry's" and "explicitly not" look the same) and the designations' `tributary_excludes` (kept out of the
  bundle on purpose, `bundle/rules/row.py:84`)? Yes lets the export derive `UNDRAWN_PART_TRIBUTARIES` and
  `EXCEPTED_RIVERS_CREEKS_CLOSED` instead of keeping two entry-id lists in code; it changes the bundle and export
  output (lane L8's files, a release). Default if no answer: leave the two lists in code (the rulings moved already).
- T679 #1 / #6 (wave 4): flip both switches with the atlas rebuild? #1 takes the border slivers no region claims off
  the map (they are ruleless and say "outside B.C." on a tap); #6 drops the name the map shows on a section no water
  holds. Counts in the T679 ticket (lane L7 section). Default: flip both, diffs shown before the release.
- Disk: the Mac's data volume ran down to 0.2 GB free about 10:30 (several lanes' delivers at once); L7 deleted
  superseded backups (reaches/full_next, full_prev, full.pre_t743, full.pre_rf, full.pre_ri3, full.pre_deadend,
  bundle.pre_t743, tiles.pre_t743, side/dfo-cuts23b, side/rebuild-input-3; about 10 GB). Wave 4's atlas build needs
  about 4 GB free, and L7 could not run its side atlas build for that reason (the proof ran on the live atlas instead).
