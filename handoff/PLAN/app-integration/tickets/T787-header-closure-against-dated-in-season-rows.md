# T787 — Regions 7 and 5A: the page header "no fishing for salmon until further notice" overrides the dated in-season openings in the table

Status: OPEN (owner: OWNER-TODO "2026-11-16 morning list" question 15; `pipeline/deliver/salmon.py` `location_rules`, and the feeds worker must read the same way).

Source: general audit 2026-11-16, labels checker finding 2 (`handoff/PLAN/audit-2026-11-16/labels.md`).

## What happens
`location_rules` replaces a place's rows with the header closure when the page header carries one
(`quota.header_closure`, owner 2026-10-31: "the header closure wins"). So the Nechako (sockeye Aug 22-Sep 21, pink;
FN0851), the Quesnel River (sockeye Aug 22-Sep 15, pink to Sep 30; FN0846) and Quesnel Lake Horsefly Bay read closed
every day. DFO's page carries both: the header, and above the table "There is no fishing for salmon in Region 7
except for the opportunities listed in the table below"; the page cached Aug 29 shows the header and the open rows
together, inside the opening's dates. No effect until next Aug 22.

## What to build (after the owner's answer)
Recommended: a dated table row that carries a fishery notice answers on its own dates; the header closure answers
every other day and fish. Python and the worker's reading change together (parity vectors).
