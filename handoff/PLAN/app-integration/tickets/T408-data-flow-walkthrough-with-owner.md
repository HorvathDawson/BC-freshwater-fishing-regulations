# T408 — Walk the owner through the data flow, one question at a time, then simplify it to match

Status: PARTIAL (2026-11-05) — walkthrough done, every stream decided (`handoff/PLAN/data-flow-decisions.md`); T324, T325, T266, T295, T297, T337 re-scoped, T412 + T413 filed. Left: data-flow.md rewritten to the pipeline as built (with T324). Was: OPEN — FIRST task of the next chat.

Source: owner 2026-11-05: wants an agent that asks them about the pipeline cleanup "to really understand what we want in
the data flow and how we want it simplified … then implements", and "at least i will learn then too". T324's design
(data-flow.md, approved 10-30) was a written plan; this is the conversation it skipped.

## Work
1. Read `handoff/PLAN/data-flow.md`, the T324–T337 tickets and the pipeline as it is today (sources → raw/cache →
   curated → generated → release → R2 / feeds worker → app). Refresh data-flow.md where it has drifted.
2. **Walk the owner through it, one stream at a time, ONE question per message** (AskUserQuestion where the choices
   are clear). For each stream: explain in plain words what it is, where the data comes from, what touches it, and
   where the owner's own review time goes; then ask what to keep, merge, drop or simplify, and where human work
   should live. Teach as you go — the owner wants to understand the flow, not just approve it. No jargon without a
   one-line meaning.
3. Write the answers down as they come (`handoff/PLAN/data-flow-decisions.md`: stream → decision → why → ticket).
4. Re-scope T324, T325, T326, T266, T295, T297, T337 to the decisions (new tickets for anything new), then implement
   them in that order. Heavy jobs one at a time (scripts/heavy.sh); parser runs stay human-only.

## Acceptance
Every stream in data-flow.md has an owner decision recorded; the implementation tickets cite it; data-flow.md matches
the pipeline as built at the end.

Links: T324 (data-flow map), T325 (write-through), T326 (tidal), T266 (marks vs data), T295/T297 (build), T337 (DFO
match in build). Size: M (talk) + the linked tickets. Owner: yes — the walkthrough is theirs.
