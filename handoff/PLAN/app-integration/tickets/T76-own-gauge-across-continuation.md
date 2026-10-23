# T76 — Own gauge across a continuation: the Vedder takes the Chilliwack's gauge (audit N19, owner call 10)

Status: PARTIAL — ee624350 (code + tests), 8ea1cf8b (decisions.md: rule, five constants, real-graph diff). Built and
reviewed on the live graph: 41 sections change, all on continuation waters (Chilliwack 15, Vedder 2, Atnarko 20, Bella
Coola 4), 0 elsewhere, lead ranks only. Vedder 313838: 08MH047 (retired) rank 1, 08MH001 rank 2 → 08MH001 leads at read
time. Left: the merger's batched `pipeline.deliver` (no reach, no atlas, no tiles) — then diff `panel_member.lead` vs
the live bundle (expect only those 41 sections) and see `test_water_continue.py::test_the_chilliwack_continues_as_the_vedder_in_the_bundle`
go green (fails on the live bundle until T59's Atnarko row ships). Review a51b6d45: catalogue pairings now read before the pickles load (peak memory); acceptance pinned on shipped data — `test_own_gauge_continuation.py::test_the_vedder_313838_is_led_by_08mh001_in_the_bundle` (fails as 'predates' on the current live bundle; must pass after the deliver). Catalogue heads == live `entry` rows (1525, 0 differ), so the `built_with` guard will not trip.

Source: own-gauge REPORT.md: "Vedder (sid 313838): nothing leads" — its own gauge 08MH047 is retired and the Chilliwack
gauges belong to another item, though `water_continue` links them. Owner 2026-10-22: Vedder/Chilliwack is "one system"
(87ba267c). Call 10 kept the five constants, but they are not recorded as values in decisions.md.

## Work
- `build_panels` treats a `water_continue` predecessor's/successor's own gauge as own, same quantity, same far/area gates.
- Record the constants in decisions.md: lead 0.7, ratio 10, far 0.2, far min 10 km, area trust 10.

## Acceptance
A test: Vedder 313838 is led by 08MH001. The before/after panel-lead diff on the real graph is reviewed — only
continuation waters change. A deliver ships it.

Depends: T56; after T59 (new `water_continue` rows such as Atnarko change the pairs). Pairs with T93.
Size: S–M. Owner: delegated (call 11). Default: yes, across `water_continue` only.
