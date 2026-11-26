# T1177 — Audit data build, inside the 0.10.0 re-cut: the rulings P5-P11 and O1-O17

Status: OPEN — inside the 0.10.0 re-cut (owner 2026-11-26); owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1177 | Audit data build, inside the 0.10.0 re-cut: the rulings P5-P11 and O1-O17 | OPEN — inside the 0.10.0 re-cut (owner 2026-11-26); owner: see ## Owner | — | a build lane (Opus): apply the rulings below with an answers diff per change; the re-cut waits for it; five asks are open |

Source: the owner's audit rulings of 2026-11-26, verbatim in `docs/OWNER-RULINGS.md` ("Audit rulings, batch 2"); the
planning chat's notes `AUDIT-REVIEW-for-the-owner` (the items), `T1106-T1088-T1084-case-lists`. Timing, his pick: "Inside
the 0.10.0 re-cut". Nothing is built before this ticket; every change below needs an **answers diff** (and a binding
diff where a binding moves), listed for his look before the build ships. Related: T1053, T1088, T1106, T1118, T1146, T1149.

## Work
1. **Tributary logic (P1, P5, P6, DFO):** P5 a boundary tributary X is left out of "upstream / downstream of X" unless the
   row says "including" (16 cases); P6a facilities and "please note" lines stop inheriting the tributaries mark; P6b the 4
   spot closures and the Bulkley method rule stay inside the spot, boat rules keep walking. **P5 and P6b are built as
   extent-level ops, never a special case.** One tributary logic for the book and DFO: DFO places climb head lakes too,
   unless DFO's own written words define "tributaries" otherwise (find them first; T1106).
2. **P7a:** Stellako opens May 15 (both closures lift; verify). **P7c:** Hevenor, Station, Two Mile keep the Nov 1-Jun 30
   release (no change; the gotcha is written).
3. **P8:** the steelhead lake copies bind to the lakes whose rows mention steelhead, the lakes a region's steelhead box
   names, and Tenas. **P9:** marshes and swamps are lakes (7A's 16 first; measure the other regions' lake-only and
   stream-only rules on marshes).
4. **P10b:** the cross-unit binding check, per row-and-water pair, an exceptions file, REPORT ONLY (T1118 item 3); the
   seven P10 overrides' notes are already rewritten.
5. **The one-offs:** O1 Little River: both rules bind the whole river, row dates replace the spring closure. O2 Upper
   Chehalis River: close it; **a lake-tributary closure must override** (check the pattern). O3 Lost Lake near Whistler:
   move the electric-motor rule to the Whistler lake. O4 Hatton Creek: add fly-only (Sep 1-Jul 14). O5 Gwillim and
   Muncho: the zone's limit is kept. O8 Robb Lake: the zone's dates are kept. **O7: a "X Lake's tributaries" row binds
   ALL the lake's inflows**; sweep every such row province-wide (Connor Lake's tributaries bind to Forsyth Creek above
   the lakes). **O13: the closure below Healy Lake, as a GLOBAL extent on the water, not rule-level.** O16 Shuswap above
   Sugar Lake: the exemption limited to Mara Bridge-Sugar Lake (his reading is unconfirmed, see Owner).
6. Kept, no change (recorded in the rulings page): O6, O9, O12, O14, O15, O17; their errors are on
   `pipeline/docs/BOOK-ERRORS-FOR-THE-PROVINCE.md`.
7. The D6 rebinds of T1118 ride the same build (Mosquito, Bear, Twin, Hall, McDonnel, Panther, McLennan).
8. Not in this build: P7b, O10, O11 (open), lake zones (T1180), the August 2026 book (T1179), the Nation Arm reach (T1178).

## Acceptance
Each change has its answers diff and every moved binding is explained; unchanged items are proved unchanged; he has seen
the lists before the re-cut ships. Severity: medium (several are "A": an angler could break the law). Model: Opus. Size: L.

## Owner

- [ ] 2026-11-26 — P7b, Region 5 "other streams listed in the tables": your rule (a listed row lifts the closure as its name and locator scope it) against only rows that say open all year or exempt. Not ruled; the audit note `P7b-region5-spring-closure` has the numbers. Default: as built.
- [ ] 2026-11-26 — O10 Goat River: an agent checks whether Leadville and Cameron creeks join the Goat so "the main stem between the two confluences" fits; then you decide the reading. Default: as built.
- [ ] 2026-11-26 — O11 Mine Lake: you asked to see the map first; the picture is sent to you. Default: as built.
- [ ] 2026-11-26 — O16 Shuswap above Sugar Lake: confirm "it is a better more cautious reading to limit it so that it is not exempoting the whole river so exempt here" means the exemption only, with catch and release on the whole river. Default: that reading.
- [ ] 2026-11-26 — The Okanagan River Oxbows label: you said "Other" with no text; what should it say? Default: the plain "side channels" label of the label rule.
