# T1121 — Web gauges page search: four leftovers from the T1107 review

Status: OPEN.

Source: the 0.9.1 chat's review of T1107 (PR #187), sent to the planning chat 2026-11-25. Not checked by the
planning chat. Related: T1107, T1060 (failures that hide), T333.

1. The page's failure messages land in the feeds worker's defect class: "gauge search file out of date (HTTP 404)"
   is not in `EXPECTED` in `app/workers/feeds/src/crashes.ts` (about line 179). Decide per message: expected (a
   release changed under an open tab) or a defect, and list it.
2. The static `/gauges` list and each gauge page do not show whether a gauge has current data; only the search
   results do (the owner asked for with and without current data to be shown, T1107).
3. Place results stop at two places: "Smithers" also lists "Smithers Landing" and nothing more. Decide the cap and
   say when more exist.
4. "1 river gauges": the plural is wrong for one (seen possible in a tiny build only).

Acceptance: each item fixed with a test that fails without it, or closed with the reason. Severity: low (2: medium,
it is half of an owner ask). Release: 0.9.2 (item 2 in 0.9.1 if the T1107 lane's fix round takes it). Model: Sonnet.
Size: S. Owner: no.
