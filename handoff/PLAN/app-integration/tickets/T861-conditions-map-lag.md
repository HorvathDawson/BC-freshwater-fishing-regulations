# T861 — The Conditions map feels laggy like the Salmon map did (same cause as T848/T855?)

Status: PARTIAL — fixed by 4c005321e (T848/T855 split, already covers Conditions + Run timing); left (Claude): lands with the 0.8.1 merge (app chat).

Owner 2026-11-18 ~17:10 (to the main chat, relayed 17:12), verbatim: "conditions seems like it was laggy like salmon map
was maybe it had same issue?"

T848/T855's layer split (lane A, in the 0.8.1 integration branch app/integrate-081) moved the lit/closed widths to a
runtime `stream-heavy` layer so MapLibre stops re-evaluating a big line-width expression on every feature-state change.
The staging site the owner is testing runs 0.8.0, which does not have that split. Work: check whether the Conditions
tab's map layers get the same split, measure its zoom-out and pan the same way (long tasks, staging 0.8.0 vs the 0.8.1
web build), and fix it if it is left out.

Owner 2026-11-18 ~17:15 (relayed), verbatim: "also check run timing to while at it". So the Run timing map is measured
and checked the same way; any fix rides the 0.8.1 web release.

## 2026-11-18: measured: same cause, already fixed in 0.8.1 (no new change)
The T848/T855 split covers both maps: Conditions draws streams in the `standing` mode and Run timing in `runs`, and
0.8.1's `streamWidths`/`splitStream` are mode-independent (`app/tools/stream-heavy.test.ts` covers both). In 0.8.0 the
`stream` width read feature-state in every mode (1,729 B on Conditions/Runs, 2,650 B Trout, 21,397 B Salmon).
Chrome (Playwright), 4x CPU, 390x844; long tasks / total ms; staging 0.8.0 vs a 0.8.1 web build of app/integrate-081:

| Fraser Valley (-121.95, 49.15) | zoom-out z12->z7, 0.8.0 | 0.8.1 | pan 600 px, 0.8.0 | 0.8.1 |
|---|---|---|---|---|
| Trout | 24/2678, 21/2257 | 5/301, 5/334 | 4/353, 4/308 | 1/53, 1/59 |
| Salmon | 25/4701, 25/4885 | 13/1068, 14/1195 | 5/828, 5/785 | 1/101, 2/161 |
| Conditions | 10/1072, 11/1042, 10/964 | 0/0, 0/0 | 2/158, 1/108, 1/105 | 0/0, 0/0 |
| Run timing | 9/1069, 13/1309, 10/1008 | 0/0, 0/0 | 1/157, 1/103, 2/159 | 0/0, 0/0 |

Intata Reach shows the same (Conditions zoom-out 4-7 tasks / 500-845 ms -> 0/0; Run timing 5-7 / 587-745 -> 0/0). Left:
Salmon at Fraser Valley still ~13 long tasks / ~1.1 s on zoom-out (the large `sp` colour/opacity lists), ~4x better than
0.8.0. Ships with 0.8.1 (web and phones). Scripts: session scratchpad `t861/measure.js`.
