# T1149 — A park rule reaches far outside the park on sections that straddle its boundary (Strathcona: about 478 km)

Status: OPEN — HIGH; the hardening release, scoped then (owner 2026-11-25)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1149 | A park rule reaches far outside the park on sections that straddle its boundary (Strathcona: about 478 km) | OPEN — HIGH; the hardening release, scoped then (owner 2026-11-25) | — | 0.10.0 chat 2026-11-25 (T1146's finding); a read-only scoping pass first; the owner chooses cut or mark |

Source: the 0.10.0 chat's read-only finding for T1146, 2026-11-25, on the candidate r-71d09ad82e81. Area and region
membership is decided by overlap and parks are not cut, so a section that straddles a park boundary carries the
park rule on its whole length. For Strathcona Park: 365 park-straddling sections carry the park rule over roughly
478 km OUTSIDE the park; Rees Creek carries it on 9.6 km from a 31 m overlap. The side channels that drew
attention to it (Burman, Bedwell, Rees, probably Comox; T1140's nameless parts) lie 0.26 to 5 km outside the park
polygon and correctly do NOT take the park rule; with the park cut at its boundary they would merge into their
lower main stems. Related: the memory note `straddling-sections-mark-not-cut` (the owner's earlier decision: 177
straddlers are polygons the cutter never touches; the 55 stream cases get side marking, not more cuts), T1146,
T1140, T1118 (the same-name audit).

Owner, 2026-11-25 (planning chat dialog), to "next patch, scoped first (recommended) / hold 0.10.0 / hardening
release", verbatim: "what is the issue here if they are straddling they can get the rule? but if it needs fix (we
can scoper the issue when we do it) with hardening release".

Work (scoping first, read-only, no curated row and no mark by an agent):
1. Answer his question with cases: what the Strathcona rule says, and for the 365 sections the length inside and
   outside the park, sorted by the share outside; ten pictures of the worst (Rees Creek first). Is the rule shown
   outside the park stricter or looser than what applies there?
2. The same count for every other park, reserve and closure area whose rule is bound by overlap (is Strathcona
   the worst or the first one found?).
3. Options to him with cost: a minimum-overlap threshold (share or metres) below which a section does not take
   the area rule; a cut at the boundary for stream sections (curation plus an atlas rebuild); a side mark as he
   decided before. Each with the binding diff it would cause.
4. Then build what he picks; diff every rule binding; tiles and bundle of one vintage.

Acceptance: no section shows an area's rule on more than an agreed length outside the area, proven by a check
that runs at build time and refuses a regression. Severity: high (a wrong rule shown on real water, though it has
been so in every release). Model: Opus. Size: M for the scoping; the build depends on his pick. Owner: the pick,
and any mark.

## Owner

- [x] 2026-11-25 — When to fix it: next patch scoped first / hold 0.10.0 / the hardening release → "what is the issue here if they are straddling they can get the rule? but if it needs fix (we can scoper the issue when we do it) with hardening release" (2026-11-25)
