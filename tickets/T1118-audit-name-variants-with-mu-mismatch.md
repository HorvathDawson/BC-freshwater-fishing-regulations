# T1118 — Audit every name variant whose target sits in a different management unit than the book's row

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1118 | Audit every name variant whose target sits in a different management unit than the book's row; a build check | OPEN — 0.10.1, first Opus lane (HIGH until the list exists; a first pass of 98 bindings is in #166) | | 0.9.1 chat 2026-11-25, behind T1117 |

Source: 0.9.1 chat 2026-11-25 ~00:50 MST, the wider question behind T1117: one name variant bound a book row from
MU 1-3 to a same-named lake in MU 1-6, and its own note recorded the mismatch.

## Work
1. List every entry of `data/curated/waters/name_variants.json` (and any other curated name-to-water binding) where
   the book row's MU differs from the bound water's MU, or where the note mentions a mismatch, another candidate or
   doubt. Mechanical: the MU of the row against the MU polygon of the bound water.
2. For each: the row as printed (page), the bound water, the other same-named candidates with their MUs, and a
   proposed verdict. Every change lands as a proposal for the owner (`everything-added-is-a-proposal`); nothing is
   rebound unreviewed.
3. A build check from now on: a row bound to a water outside its printed MU fails unless an explicit, reviewed
   exception says why (rows on MU boundaries exist).
4. Answers diff for whatever the owner accepts.

## Acceptance
The list exists with a verdict per row; the check fails on the T1117 case; accepted fixes ship with their diff.

Depends: T1117. Severity: high until the list exists (each hit is wrong rules on a real water; count unknown).
Release: 0.9.2, first Opus lane (the list); fixes as the owner accepts them. Model: Opus. Size: M. Owner: yes
(reviews the proposals).

## First pass 2026-11-25 (0.9.1 chat, in PR #166)
98 bindings whose note or target names another management unit; the list is the file mu-mismatch-bindings.md (deleted; git history) in the
T1118-shots folder of that PR. No verdict per row yet: that is this ticket's first lane.

## 2026-11-26 night: the list, the research and the owner's rulings
- The list (planning chat notes `T1118-name-variants-list`): 55 of 1,393 book rows (60 row-water pairs) are bound
  to a water outside the row's printed unit; 6 probably wrong, 4 cannot tell, 45 probably right. All 60 come from an
  override in `data/curated/regulations/book_name_links.json` (58) or a hand-stamped list (2); none from a name
  variant (the name lookup refuses other units, `matcher.py:321-333`; override ids are never checked,
  `matcher.py:387-398`). All seven questioned overrides date from that file's first commit (2026-03-14).
- The research (notes `T1118-better-matches`, all 55 rows, old stocking and survey records): the rulings below hold;
  three "cannot tell" rows are kept (Emerald, Nation River, Ellison: the only water of the name).
- The owner's rulings, his words in `docs/OWNER-RULINGS.md`: Mosquito Creek = the Arrow Park one; Bear (Mahood)
  Creek = the Surrey creek only; Twin Lakes 4-34 = the pair inside 4-34 only, with a note; Hall Lake = the 4-34
  lakes; McDonnel = the 6-9 lake only; Panther Lake points at Healy Lake (and is its name variant); McLennan Creek
  bound to both, a question to the province. Each override's note says why.
- Work from here: a lane applies the rebinds with an answers diff, in the ONE future data build with the audit
  fixes, after the owner's audit review; the build check (item 3) starts report-only, its exceptions carrying the
  note's reason.

## Owner

- [x] 2026-11-26 — The 14 newly examined "book's unit is off" rows → "Keep all 14 and add each to the Province list with its evidence (Recommended)" (2026-11-26); McLennan Creek → "Both creeks closed, ask the Province (Recommended)" (2026-11-26)
- [ ] 2026-11-25 — Review the proposals of the cross-unit name-binding audit when its first lane lands them (0.10.1).
- [x] 2026-11-26 — The seven probably-wrong bindings → his rulings (2026-11-26), verbatim in `docs/OWNER-RULINGS.md`; the first: "miquot creek yes should be the arrow park one."

## Still open (from the deleted mu-mismatch-bindings.md, 2026-11-25)
- The first-pass list (98 rows: file, printed name, bound water, printed MU, the target's MUs, why listed, note) is in git history (T1118-shots/mu-mismatch-bindings.md); regenerate it from the curated name files and the registry's item MUs instead of trusting it.
- Every row still needs a verdict (a water straddling MUs, or an MU line drawn differently from the book's, can be listed and be right).
- The build check (item 3) is not built.

## 2026-11-26 afternoon: the seven out-of-unit rows stay (P10)
Owner: "i agree with the 7 items can you add all the info to the notes? so we dont have to refined it." Lynx, Chubb, Bridal, Hiawatha, Emerald, Nation River and Ellison keep their bindings; each override's note in `book_name_links.json` now carries its evidence (federal vessel schedule item and coordinates, the stocking record, the Province's proposal page, the Kootenay 2009-10 review); the errors are entries 1-7 of `pipeline/docs/BOOK-ERRORS-FOR-THE-PROVINCE.md`. The build check (item 3) is report-only first (P10b: "Yes, report only (Recommended)"), built in T1177. The Nation Arm reach is T1178.

## 2026-11-26 evening: all 60 pairs audited (planning chat; notes `out-of-unit-full-audit`)
The owner asked: "what ever happen to the audit a long time ago looking at all waterbodies that were matched to items in a
different mu you said there were like 50 but we only looked at 7". True gap: only 4 of the 49 "probably right" pairs and
the 7 hand overrides had a real look. A full look now covers **all 60 pairs over 55 rows** (the same ids as
`T1118-name-variants-list`); each has evidence and a verdict, and it found **no new wrong water**:
- FINE-LINE 19 (on the printed unit's edge, or within 2 km, the only water of that name near); FINE-NAMED 8 (the row
  names the part); FINE-GROUP 3 (one polygon of a gazetted group); **BOOK-UNIT-OFF 21** (7 already in P10 or ruled, **14
  new**); D6 7 (wrong water, fixed by D6); AMBIGUOUS 1 (McLennan Creek 2-8); RULED 1 (Mackenzie Creek 4-21 to 4-31).
- Federal vessel-schedule points tie the bound water of 16 more rows to the right lake or stream within about 1 km, and
  settle the D6 rebinds of Hall Lake and Twin Lakes west of Brisco (4-34; 109 to 126 km from today's binding).
- All 60 bind by an override id (58) or a hand-stamped list (2); none by name or a tributary walk.
- **His picks:** the 14 new rows: "Keep all 14 and add each to the Province list with its evidence (Recommended)" (Prior,
  Prospect, Chilliwack Lake, Hope Slough, Lucille, Stawamus, Bridge, Jack of Clubs, Dunalter, Lakelse River, Wahla,
  La Salle, Tsitniz, Swan 8-26: entries 25-38 of `pipeline/docs/BOOK-ERRORS-FOR-THE-PROVINCE.md`). McLennan Creek: "Both
  creeks closed, ask the Province (Recommended)". The audit's other three recommended defaults were taken by the planning
  chat and sent to the data chat: seed the build check's exceptions with the 51 non-D6 pairs that are not ambiguous or
  ruled, each with its class, distance and evidence line (`out/pairs.csv`); Mackenzie Creek stays 4-31 and goes to the
  Province; the six wrong override notes (Square Lake, Langford, Mahood, Hall and Twin 4-34, Arrow Park) are rewritten inside
  the D6 change.
- The check (item 3) stays report-only and is built in T1177. Maps: `out-of-unit-full-audit/maps/`.
