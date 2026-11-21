# T980 — Three follow tests skipped to ship release r-fec3fab4c5aa: fix them

Status: IN PROGRESS — fixed on lane/085-t983; left (Claude): merge + combined review.

## Measured and done (2026-11-21, lane/085-t983)
1. The +5 page calls on both waters are all T973's `cw-through`: opened (1 meta read by the loader, 1 by the store's
   atlas check) and read whole (`cw_note`, `cw_year`, `section_cw`) by the FIRST tap, once a store — the prewarm did
   not ask it (r-6258ffded676 → r-fec3fab4c5aa: Norrish 26 → 31, Cultus 29 → 34; nothing else moved). The page's
   whole-once reads are now one list, data `warmRegsStore` (statics, rules, licensing, section notes, carried units;
   regs-sid / core open): the phone's prewarm (`prewarmRegs`) asks it and the T286 count warms with it. Page 24 / 28 /
   27, whole tap 38 / 39 / 41 on both releases. Budgets unchanged. (The section notes, whole-once since T796, left the
   page count with it: 2 calls of headroom the budget did not have at 0.8.0.)
2. `taps.py` keeps an unnamed stream a tributary rule closes all year on purpose (`trib_closed`: every segment's display
   closed, every closer a tributary rule; it finds r-6258's 844024). r-fec3fab4c5aa's even sample held none (844054 is
   open part of the year): 137340 (Region 2) takes the last non-base stream slot (1067388). A pool with none stops the
   re-cut. Fixtures re-cut on r-fec3fab4c5aa (`sync.mjs`): only taps.json `unnamed`, one section-lines row and the
   fixture release (r-1af85a5d9eb1 → r-30be58c1fe32) moved; page-oracle, VM fixtures and trout golden byte-equal.

Source: 2026-11-20. The owner was away and wanted the release out ("just promote the releases now"). The 0.8.4 data
release r-fec3fab4c5aa stopped at [5/7] follows on 3 failures. None is a wrong rule shown to an angler. They were skipped
with `--no-follow-tests`:
1. `openCost.release.test.ts` T286: Norrish Creek's page took 31 SQLite calls against a budget of 30; Cultus Lake's took
   34 against 30. The whole tap stayed under budget (45 and 48 of 50). The extra reads are likely T973 cw-through or LF's
   salmon `named` lookup. Fold them into existing reads, the way T286 did. The budgets stand.
2. `WaterScreen.unnamed.test.tsx`: "the regs fixture holds no unnamed stream that a tributary rule closes all year". The
   re-cut picked a different unnamed example (edge_unnamed handle 772845 → 917553). Make the fixture keep one such
   stream on purpose.

## Acceptance
All three pass on the next release, with no budget raised.

Depends: —. Size: S. Owner: no.
