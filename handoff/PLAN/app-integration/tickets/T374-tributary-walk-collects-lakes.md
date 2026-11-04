# T374 — The Skeena tributary walk collects 11 lake sections

Status: DONE (e23994f5a) 2026-11-04 — NOT A BUG, the test was stale (found 2026-11-03 re-pinning test_dfo_salmon; failing before this session — not caused by T343/T366).

`test_dfo_salmon::test_a_watershed_walk_passes_through_its_lakes_but_collects_streams` fails on the current atlas: the
walk of the Skeena collects 11 `lake:` sections (lake:328974096, lake:329030585, lake:329030607, lake:329031154,
lake:329066047, lake:329066410, …). The owner's ruling (2026-09-24): a tributary walk passes THROUGH lakes and collects
none ("tributaries" are streams, synopsis p.80). The same walk (`atlas.reach.build.build_reach`, includes_tributaries)
binds freshwater "[Includes Tributaries]" rules, so trout rules may be on lakes they should not be.
Work: find which atlas change let lakes in (the split alias on lake boundaries? braids?), diff rule bindings before and
after the fix (memory: split fix changes the trib walk), then a reach-only rerun + bundle.
Size: M.

## Done 2026-11-04: not a bug
All 11 are polygons the owner called `stream` on 2026-10-29 in the T322 slough review (`data/curated/waters/
slough_calls.json`): each was folded into the one creek through it (Deep, Pinkut, Station, Darling, Quill Creeks, the
Telkwa and Kitnayakwa Rivers) by `registry.flowing.fold_confirmed_sloughs`. The graph node keeps its polygon kind
(`lake`), so the test — written before T322 — read them as lakes. The walk is right to collect them: they are part of
the stream. The test now subtracts the owner's stream calls and checks the folded sloughs ARE carried. No rule moves;
no rerun.
