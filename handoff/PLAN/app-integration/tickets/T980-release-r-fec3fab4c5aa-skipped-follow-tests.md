# T980 — Three follow tests skipped to ship release r-fec3fab4c5aa: fix them

Status: OPEN

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
