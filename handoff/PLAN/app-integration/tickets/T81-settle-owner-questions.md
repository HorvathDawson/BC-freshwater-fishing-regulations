# T81 — Settle the pending small owner questions and record them (audit N15, owner call 11)

Status: DONE — 32e3772d (decisions.md "CLAUDE 2026-10-22 (T81, owner delegated: audit call 11)"). The README table edits below are the merger's (this lane may not edit README).

Source: owner call 11: "Claude makes judgement calls on the ~30 smaller pending questions and records them". There is
no consolidated record; two threads of questions have already been lost. The README "Owner questions" table now lists
every open call with its default; this ticket turns each delegated default into a recorded decision.

## Work
For each row of README "Owner questions" marked delegated, plus the stragglers below: the call, why, how to undo it —
recorded in decisions.md under one dated block. A row that needs code becomes a line in a ticket. Answered rows move to
"Answered" in the README table. Stragglers: T44 (name size on Conditions/Map, desktop column, Map full-bleed, wheel
outside the column), T45/T46, T38/T42/T43 and map-chrome questions, T48 best-match preview (default no) and "Add a spot
here" words (likely closed: removed by the owner), T49 `dayRange` (default: calendar year), D18 bait `also_allowed`
(hidden), the selected-tab cue, "Level may move into the layers menu" (done 476b06d8: close), dark water names near
status.base / flow f5, (p15 is ANSWERED by the owner, decisions.md "OWNER 2026-10-21 (late)": keep the 10 municipal
continuations; the button says where it goes, 88478f44; reversed continuations refused by rule, 9defa9a5 — close it),
prototype bugs (closed-all-year "."; `pinUpdate` throwing on 25/134 states — default: close, the prototype is a
reference).

## Acceptance
A grep of decisions.md and tickets/ for "OWNER QUESTION" / "no owner answer" finds only answered or delegated-decided
items. The list sent to the owner is one short block.

Depends: none. Size: S–M. Owner: delegated; the owner reads the record afterwards.

## Result (2026-10-22)
Decided and recorded in decisions.md under one dated block: Q8, Q11, Q12, Q14, Q15, Q16, Q17, Q18, Q19, Q20, Q21, Q22,
Q23, Q25 (Q24 was already answered), plus the stragglers (T38, T42, T43, T45/T46, T39/T40, T49, the Level-menu item, the
"Owner questions this spec could not settle" list). Older open-question lines in decisions.md (T49 header, T48
"OWNER QUESTIONS", D18 row, the prototype-bug / image-permission / `code: null` bullets) are marked DECIDED/CLOSED with a
pointer. No code changed; nothing needs a rerun.

### README changes the merger makes (this lane did not edit README)
- Ledger row T81: `DONE | 32e3772d, <this ticket's commit> | —`.
- "Owner questions": delete rows Q8, Q11, Q12, Q14, Q15, Q16, Q17, Q18, Q19, Q20, Q21, Q22, Q23, Q25.
- "Answered": add one line — "Delegated calls Q8, Q11, Q12, Q14–Q23, Q25 — decided by Claude (owner call 11), decisions.md
  'CLAUDE 2026-10-22 (T81)'; the owner reads the record."
- Everything left in the table is owner-only (Q1–Q7, Q9, Q10, Q13, Q26–Q44); none is delegated.

### Lines for other tickets (code or text this record implies; not done here)
1. T70 (cleanup wave 1, app side): Q17 "no" leaves the best-match-follows-typing branch dead in production —
   `useSearchView(..., follow = true)` and `subjectOf(..., follow = true)` in `app/packages/ui/src/search.ts` are only
   ever called with `false` (search.ts:387); the `true` path lives only in `search.test.ts`. Drop the parameter and the
   branch (or keep it if the owner ever reverses Q17).
2. T83: its Source line and T47:78 say "no owner answer" for boundaries-under-water (Q14). It is now decided (under the
   status lines, above the fills); append "(decided T81, Q14)" to both so the acceptance grep reads clean. Q15 and Q16
   are decided too (own hue; lift-or-listed-exception) — T83 builds them.
3. T76 builds Q8 (`water_continue` only), T80 item 3 builds Q11 (pipeline `group`), T82 builds Q12 (O3 caveat box, O5
   size-to-content with the prototype's cap). Their tickets already carry the work; no new ticket.

### Acceptance check
`grep -rn "OWNER QUESTION\|no owner answer" decisions.md tickets/` → decisions.md:163 and :252 now say DECIDED (T81);
T47:78 and T83:5 name Q14, decided here (line 2 above makes their text say so); this ticket's own acceptance line.

### The short block for the owner
Delegated calls settled (decisions.md "CLAUDE 2026-10-22 (T81)"): Q8 own gauge across `water_continue` only · Q11 D19
pooled row gets a pipeline `group` · Q12 timed caveat in the same box, sheets sized to content · Q14 boundaries under
the status lines · Q15 own hue for the warm dot · Q16 lift sub-20 pairs or list them · Q17 no best-match preview ·
Q18 closed (removed) · Q19 this calendar year · Q20 `also_allowed` hidden · Q21–Q23 as built · Q25 closed (the app
already fixes both prototype bugs). T38/T42/T43/T45/T46/T39–40/T49 stragglers as built. Say any you want reversed.
