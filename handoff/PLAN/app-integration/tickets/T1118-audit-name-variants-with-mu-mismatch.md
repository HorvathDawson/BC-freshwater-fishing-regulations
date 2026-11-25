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
98 bindings whose note or target names another management unit; the list is the file mu-mismatch-bindings.md in the
T1118-shots folder of that PR. No verdict per row yet: that is this ticket's first lane.
