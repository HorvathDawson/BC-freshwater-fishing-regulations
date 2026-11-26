# T1149 — A park rule reaches far outside the park on sections that straddle its boundary (Strathcona: about 478 km)

Status: OPEN — HIGH; Creston Valley WMA first, in the first patch after 0.10.0 (owner 2026-11-25); the other areas with it or in the hardening release

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1149 | A park rule reaches far outside the park on sections that straddle its boundary (Strathcona: about 478 km) | OPEN — HIGH; Creston Valley WMA first, in the first patch after 0.10.0 (owner 2026-11-25); the other areas with it or in the hardening release | — | 0.10.0 chat 2026-11-25 (T1146's finding); a read-only scoping pass first; the owner chooses cut or mark |

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

## Scoped 2026-11-25 night (read-only; every bound section measured from the tile lines)
Notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `T1149-scoping.md` (with `straddlers.csv`).
- **The worst case is the Creston Valley Wildlife Management Area, not Strathcona.** Its rule "bass and yellow
  perch, no limit" and its permit requirement show on about 190 km of creeks OUTSIDE the area (68% of the length
  the rule is on): Goat River 23.8 km outside from 145 m inside; Corn Creek and Summit Creek alike. The rule lifts
  Region 4's bass and perch closure, so the app says "keep any number" where bass fishing is closed. Confirmed
  with the app's own reader on Goat River and Corn Creek. It is on production today (0.9.0).
- Strathcona's only rule is "No powered boats": 384 straddling sections, 478 km outside against 377 km inside;
  it never changes open / closed, quotas or gear. Bowron Lake Park: 135 km outside (quota 1, bait ban: stricter).
  Liard: 15.6 km, almost all outside B.C. The rest are small or zero.
- Cause: membership needs only 1 m of overlap (`border.py:275`, `429-438`); parks and WMAs are never cut.
- A minimum overlap or a majority rule barely helps (a 1,000 m minimum still leaves Strathcona 212 km outside;
  majority leaves 99 km outside and strips the rule from 102 km inside). A side mark has no cut to mark for a
  park. Only the clean cut national parks already get brings the outside length near zero (5.5 km for
  Strathcona): about 480 new sections and one atlas rebuild; it also merges the Burman, Bedwell, Rees, Comox and
  Thelwood side-channel parts (T1140's leftovers).

Owner, 2026-11-25 (planning chat dialog), on Creston, to "first patch (recommended) / hold 0.10.0 / hardening":
"first patch so that it has hopefully improved cycle time from the audit". The 0.10.0 chat's advice: one fix for
all areas, not a Creston special; the smallest shape without an atlas rebuild is in the reach step (bind an
area's rules only to sections mostly inside, reusing T1140's line-in-polygon measure). A build-ready plan for the
first patch is being written (same archive, `T1149-first-patch-plan.md`).

Not this ticket, by the owner's ruling of the same night: a river that is the LINE between two management units
or regions is in both and takes either's closure (Campbell River: "campbell river is correct it straddles so is
included in mu 1-10 and 1-6 so is in closure"). That data is right.

## Owner

- [x] 2026-11-25 — When to fix it: next patch scoped first / hold 0.10.0 / the hardening release → "what is the issue here if they are straddling they can get the rule? but if it needs fix (we can scoper the issue when we do it) with hardening release" (2026-11-25)
- [x] 2026-11-25 — The Creston Valley WMA rule shows on 190 km outside the area (illegal bass retention shown as allowed): when → "first patch so that it has hopefully improved cycle time from the audit" (2026-11-25)
- [ ] 2026-11-25 — After the first-patch plan: pick the fix (a clean cut of Creston, Bowron and Strathcona with a build check, recommended; or mostly-inside binding with no atlas rebuild) and the length allowed outside an area (default 1,000 m); Liard is left as it is unless you say.
