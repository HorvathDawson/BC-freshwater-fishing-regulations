# T1177 — Audit data build, inside the 0.10.0 re-cut: the rulings P5-P11 and O1-O17

Status: OPEN — inside the 0.10.0 re-cut (owner 2026-11-26); owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1177 | Audit data build, inside the 0.10.0 re-cut: the rulings P5-P11 and O1-O17 | OPEN — inside the 0.10.0 re-cut (owner 2026-11-26); owner: see ## Owner | — | a build lane (Opus): apply the rulings below with an answers diff per change; the re-cut waits for it; one ask is open |

Source: the owner's audit rulings of 2026-11-26, verbatim in `docs/OWNER-RULINGS.md` ("Audit rulings, batch 2"); the
planning chat's notes `AUDIT-REVIEW-for-the-owner` (the items), `T1106-T1088-T1084-case-lists`. Timing, his pick: "Inside
the 0.10.0 re-cut". Nothing is built before this ticket; every change below needs an **answers diff** (and a binding
diff where a binding moves), listed for his look before the build ships. Related: T1053, T1088, T1106, T1118, T1146, T1149.

## Work
1. **Tributary logic (P1, P5, P6, DFO):** P5 a boundary tributary X is left out of "upstream / downstream of X" unless the
   row says "including" (16 cases, the audit's count); P6a facilities and "please note" lines stop inheriting the tributaries mark; P6b the 4
   spot closures and the Bulkley method rule stay inside the spot, boat rules keep walking. **P5 and P6b are built as
   extent-level ops, never a special case.** One tributary logic for the book and DFO: DFO places climb head lakes too,
   unless DFO's own written words define "tributaries" otherwise (find them first; T1106).
2. **P7a:** Stellako: the row's Nov 15-May 14 replaces the spring closures; it opens May 15 (the owner's words; verify the lift). **P7c:** Hevenor, Station, Two Mile keep the Nov 1-Jun 30
   release (no change; the gotcha is written).
3. **P8:** the steelhead lake copies bind to the lakes whose rows mention steelhead, the lakes a region's steelhead box
   names, and Tenas. **P9:** marshes and swamps are lakes (7A's 16 first; measure the other regions' lake-only and
   stream-only rules on marshes).
4. **P10b:** the cross-unit binding check, per row-and-water pair, an exceptions file, REPORT ONLY (T1118 item 3); the
   seven P10 overrides' notes are already rewritten.
5. **The one-offs:** O1 Little River: both rules bind the whole river, row dates replace the spring closure. O2 Upper
   Chehalis River: close it; **a lake-tributary closure must override** (check the pattern). O3 Lost Lake near Whistler:
   move the electric-motor rule to the Whistler lake. O4 Hatton Creek: fly only ALL YEAR on Hatton Creek and the Caycuse main stem above the Hatton confluence (final ruling, replaces Sep 1-Jul 14; no other tributary; the Caycuse Jul 15-Aug 31 closure stays on the whole Caycuse). O5 Gwillim and
   Muncho: the zone's limit is kept. O8 Robb Lake: the zone's dates are kept. **O7: a "X Lake's tributaries" row binds
   ALL the lake's inflows**; sweep every such row province-wide (Connor Lake's tributaries bind to Forsyth Creek above
   the lakes). **O10: catch and release only on the Goat main stem between Leadville Creek (km 33.1) and Cameron Creek (km 42.6); the rest takes the regional quota (GOTCHAS written). O13: the closure below Healy Lake, as a GLOBAL extent on the water, not rule-level.** O16 Shuswap above
   Sugar Lake: the exemption limited to Mara Bridge-Sugar Lake (confirmed: "Yes, that is right"; no data change).
6. Kept, no change (recorded in the rulings page): O6, O9, O11, O12, O14, O15 (the book prints two Tamihi lines, closed all year below the falls and May 1-June 30 above; the data
   matches both, only the distance differs: "Our cut is the falls" stands), O17; their errors are on
   `pipeline/docs/BOOK-ERRORS-FOR-THE-PROVINCE.md`.
7. The D6 rebinds of T1118 ride the same build (Mosquito, Bear, McLennan, and the four lake rebinds he approved: McDonell 6-4, the Twin Lakes 4-34 versus Yankee and Canuck, Hall Lake 4-20 versus the 4-34 Hall lakes, the Malcolm Knapp Lost Lake versus the Whistler Lost Lake; Panther is only the name-variant pointer fix).
8. **P7b (ruled, the strictest reading):** the Region 5 spring closure is lifted only where a row prints its own closure or "open
   all year" (whole or part), on that stretch; gear or quota rows keep it. Closes about 430.8 km in spring (Cariboo 143.7, Quesnel below
   the Cariboo 99.9, Moffat 88.1, McKinley 38.5, Blue Lead 18.2, Little Horsefly 17.8, Mitchell 15.4, Canim 9.2); opens nothing. GOTCHAS
   entry written; the Region 5 question is BOOK-ERRORS entry 19. The lower Quesnel (owner, batch 3): "Yes, close it too (Recommended)".
9. O16 is confirmed (no change). The Oxbows label follows the general label rule (the final table goes to him).
10. **The tributary walk (own work item; a walk change, needs a reach rerun and a rule-bindings diff):** "everything in is a tributary and only shouldnt be if an exception is listed
    saying otherwise". A lake's tributaries row covers every inflow, climbing through every lake, even a river with its own row; a stream is not a tributary of itself (a row "stream A
    from x to y, incl. tributaries" does not include A above x); tributary rules stack and on a conflict about the same item the MOST UPSTREAM row wins. Root cause and gap list: the note
    `one-offs-evidence/C-lake-tributaries-sweep.md`: `_continuing()` in `pipeline/atlas/graph/tributaries.py` (about L884-917) refuses the inflow on the lake's outlet blue line, then
    `_through_river_with_the_lake()` in `pipeline/atlas/reach/build.py` (about L1325-1372) puts it back only when the river has no row of its own. Gaps: Chehalis 180 km, Whitetail inlets
    47.9 km, Connor 50.5 km, Buttle 2.4 km (and the Campbell and Arrow chains). Covers O2 and O7.
11. The year-bar fix (partial-closure line type) is wanted in the re-cut but waits on the design chat's look (T1180).
12. Not in this build: O11 (ruled, no change), the Oxbows label, lake zones (T1180; nothing is built), the August 2026 book (T1179), the Nation Arm reach (T1178).
13. Not in this build, cross-link only: the owner's ~17:35 rulings on #312's final side-channel table (Snehumption: "Plain \"2 side
    channels\" (Recommended)"; Klinaklini / MacMillan: "Plain \"Side channel\" / \"3 side channels\" (Recommended)"; "Shorten Ecological
    Reserve to \"Eco Reserve\"") are PR #312's words (T1172, round 4); the area in the app subtitle is T1181.

## Acceptance
Each change has its answers diff and every moved binding is explained; unchanged items are proved unchanged; he has seen
the lists before the re-cut ships. Severity: medium (several are "A": an angler could break the law). Model: Opus. Size: L.

## Batch 3 answers (the owner, 2026-11-26 night, verbatim; the lower Quesnel's is in item 8)
- Stellako (opens 16.5 km May 15-Jun 30; Region 6 steelhead only Jun 16-30): "Yes, open May 15 as built (Recommended)".
- Little River (3.6 km; lift for rainbow trout and char only; catch and release Jul 1-Dec 31): "yes we want the strictest here."

## Owner

- [ ] 2026-11-26 — The Oxbows label follows the general label rule: look at it in the final label table before #312 merges ("Follow the rule, show me in the final table (Recommended)"). Default: the rule's words.
