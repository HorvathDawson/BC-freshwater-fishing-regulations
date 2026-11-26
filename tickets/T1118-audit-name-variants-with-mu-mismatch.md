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

- [ ] 2026-11-25 — Review the proposals of the cross-unit name-binding audit when its first lane lands them (0.10.1).
- [x] 2026-11-26 — The seven probably-wrong bindings → his rulings (2026-11-26), verbatim in `docs/OWNER-RULINGS.md`; the first: "miquot creek yes should be the arrow park one."

## Still open (from the deleted mu-mismatch-bindings.md, 2026-11-25)
- The first-pass list (98 rows: file, printed name, bound water, printed MU, the target's MUs, why listed, note) is in git history (T1118-shots/mu-mismatch-bindings.md); regenerate it from the curated name files and the registry's item MUs instead of trusting it.
- Every row still needs a verdict (a water straddling MUs, or an MU line drawn differently from the book's, can be listed and be right).
- The build check (item 3) is not built.

## 2026-11-26 afternoon: the seven out-of-unit rows stay (P10)
Owner: "i agree with the 7 items can you add all the info to the notes? so we dont have to refined it." Lynx, Chubb, Bridal, Hiawatha, Emerald, Nation River and Ellison keep their bindings; each override's note in `book_name_links.json` now carries its evidence (federal vessel schedule item and coordinates, the stocking record, the Province's proposal page, the Kootenay 2009-10 review); the errors are entries 1-7 of `pipeline/docs/BOOK-ERRORS-FOR-THE-PROVINCE.md`. The build check (item 3) is report-only first (P10b: "Yes, report only (Recommended)"), built in T1177. The Nation Arm reach is T1178.
