# T76 — Own gauge across a continuation: the Vedder takes the Chilliwack's gauge (audit N19, owner call 10)

Status: OPEN.

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
