# T216 — "How this was decided", better: explain the interpretation and what overrode what

Status: OPEN

Source: owner 2026-10-27. The prototype's "How this was decided" ladder (app/packages/core/src/regs/decided.ts; T190 grouped
two-region headings) explains a day's answer. Make it better and clearer for an angler: for the water + date, show
which rules applied (region base → water row → exceptions), what each one overrode or was overridden by (stricter wins,
closures beat openings, the water's own row beats the region, "on parts", tidal, DFO), and why — in plain words, each
step citing its source (printed page). Collapsed by default, one tap to expand. Tests on real cases (Fraser two
regions, a spring-closure override, a sub-cap, a closed water).
