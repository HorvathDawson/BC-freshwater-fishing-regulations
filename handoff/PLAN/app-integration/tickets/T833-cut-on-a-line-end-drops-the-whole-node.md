# T833 — a cut on a line's end drops every line touching that node, not just the lines meeting at the cut

Status: OPEN (main chat; reach code, needs a POLICY_VERSION bump and a rebuild).

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
