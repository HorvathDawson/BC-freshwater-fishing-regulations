# T833 — a cut on a line's end drops every line touching that node, not just the lines meeting at the cut

Status: DONE — 20ccb1cd8, dad1dea26, 5debac3ef, a5638c5e7. an end cut takes across only the lines meeting at its point (junctions within STUB_M), POLICY 46; the Gitnadoix powerline cut and label restored.

Closed by the 2026-11-20 audit: fix and STUB_M tolerance landed on land/rebuild-1116, province binding diff vs rebuild2 empty, merged a5638c5e7 and shipped with the 0.8.0 data.

Source: the rebuild follow-up worker, 2026-11-17 04:15 (land/rebuild-1116, T803 item 11 Gitnadoix).

`reach.extent._half`, for an upstream_of / downstream_of cut sitting on a line's end (m 0), calls `_across_the_junction`.
That marks every other blue line touching the end NODE as outside, not only the lines that meet at m 0. On the
Gitnadoix the lowest node `360887399:0` spans 0 to 23.6 km, so the owner's "upstream of the powerline crossing near
the mouth" place (cut picked up as an alias at the mouth, m 0) lost 9 lines: six dead-end side channels (360230202,
360236295, 360240397, 360243603, 360244909, 360245609), the mouth distributary 360215450 with 360215474, and the creek
360742973 (its coho went from open Sep 1 to Oct 31 to closed). The tributary walk only recovers braids that flow back.

Stopgap on land/rebuild-1116 (df08a776a): the place is `{op: whole}` again and the cut is removed from cuts.json, so the
mouth lost the powerline label the owner asked for in T803 item 11.

Fix: `_across_the_junction` excludes only the lines that meet the cut's own point (measure 0 of the cut line), not the
whole node; pin with the Gitnadoix case (the cut back, the place binds the same 1,000 sections as `whole`) and a
synthetic node test; bump POLICY_VERSION; diff rule bindings across the whole province for every other cut on a line
end before promoting (memory: split-fix-changes-trib-walk). Then restore the cut and the T803 label.

## Progress 2026-11-17 05:47 (worker, land/rebuild-1116)

- 20ccb1cd8: `_across_the_junction` takes an edge only when its junction is the cut's point (incoming by measure,
  `at_cut` 1 m; outgoing by coordinate against the line's end point), `A_CUT_ON_A_LINE_END_IS_ACROSS_ONLY_AT_ITS_POINT`,
  POLICY_VERSION 46. 5debac3ef: the Gitnadoix cut and the upstream_of place are back (Sheldens part of df08a776a kept).
- Rebuild (scratch rebuild3, r-ffb9d7191e4f): Gitnadoix coho place binds 1,000 = `6:gitnadoix-river:whole`; the
  label is back in the bundle. BUT the province diff vs rebuild2 moved the Chilliwack-Vedder row: the Vedder Crossing
  Bridge cut sits on the Vedder line's top (356363467, m 12950.7) and the Chilliwack joins 25.4 m below it (m 12925.3),
  so the 1 m rule finds nothing across: r3 (between Tamihi and Vedder Crossing) 284 -> unresolved `parallel_branches`,
  r4-r9 (downstream of Vedder Crossing) 32 -> 1,816. NOT shippable. Candidate: a junction within `tolerances.STUB_M`
  (100 m, the continuation rule's "B's line may go on this far above A's mouth") counts as at the cut's point;
  the Gitnadoix's nearest non-mouth junction is 520 m.
- 2026-11-17 06:02 (main chat call, dad1dea26): STUB_M adopted — a junction within `tolerances.STUB_M` (100 m, as
  `bundle.continuation`) of an end cut is at its point; POLICY_VERSION stays 46. Reach-only rerun on rebuild3's atlas
  (scratch rebuild3b): province binding diff vs rebuild2 is EMPTY (rules, DFO places, licensing; unresolved identical);
  Chilliwack-Vedder r3-r9 back to 284 / 32; the Gitnadoix coho place binds 1,000 = its `whole` place.
  Deliver r-e5b7b1527bbf: card check 0, vintage agrees, paint sync + name check pass, size gates clean. Answers diff vs
  rebuild2 r-101acd988438: 0 groups, one release-wide wording (gnis:3508 part name now reads from the powerline
  crossing); vs live and vs salmon: the same 966 / 168 groups as rebuild2. Not promoted.
