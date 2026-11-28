# T1149 — A park rule reaches far outside the park on sections that straddle its boundary (Strathcona: about 478 km)

Status: OPEN — HIGH; Strathcona's cut is IN the 0.10.0 re-cut (T1240 class 2; owner 2026-11-26/27, ## Owner); Creston Valley WMA in the first patch after 0.10.0 (owner 2026-11-25); Bowron later

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1149 | A park rule reaches far outside the park on sections that straddle its boundary (Strathcona: about 478 km) | OPEN — HIGH; Strathcona cut in the 0.10.0 re-cut (T1240); Creston in the first patch; Bowron later | — | the Strathcona cut with T1240 in the re-cut; Creston's cut in the first patch; the build check is an open pick (## Owner) |

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

## First-patch plan 2026-11-25 night (read-only; build-ready)
Notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `T1149-first-patch-plan.md` (pictures and scripts beside it in the planning chat's scratchpad).
- **Fix Creston with the clean cut national parks already use, not with "mostly inside" binding.** Majority
  binding moves the error inside the parks: Bowron, 26.4 km inside would show the looser regional quota and allow
  bait; Strathcona, 102.5 km inside would lose its rule and 19 sections would still reach more than 1 km outside;
  Creston alone, 9.5 km would still say "bass: no limit" outside (the Kootenay River's 3.4 km is named by the
  book as included) and 16.3 km inside would lose the permit line. PR #265 has no "inside length" measure to
  reuse (`part.inside_area` tests only where a part's runs start and end); `regions.region_shares` is the one
  that exists.
- The cut is small: ONE curated edit, a proposal for the owner: Creston gets its own WMA entry with
  `cut: "clean"`, rejoin 1,000 m; the area id stays; no new cutter code. About 66 new sections; section ids
  change, item ids do not (no entry resync); 0 owner marks on the Creston entry. One atlas side build (32 min,
  20 GB peak) and the tiles; the release can reuse the proof's atlas. A bug the lane must fix:
  `lake_loops.py:94-99` treats every `area:wma:` area as uncut. Cutting ALL parks and WMAs is not worth it
  (about 18,000 new sections that no rule reads).
- The build check: the longest UNBROKEN run outside an area is at most 1,000 m, counted only inside B.C. (a
  total-length limit would refuse Yoho's Beaverfoot River, which is the park's boundary; today's national parks
  pass, longest run 976 m). It starts with a list of known exceptions that may only shrink: Strathcona's 114 and
  Bowron's 17 sections (the hardening release).
- Estimated: about 26-28 h on the clock, 11-12 h of work.

## Owner

- [x] 2026-11-25 — When to fix it: next patch scoped first / hold 0.10.0 / the hardening release → "what is the issue here if they are straddling they can get the rule? but if it needs fix (we can scoper the issue when we do it) with hardening release" (2026-11-25)
- [x] 2026-11-25 — The Creston Valley WMA rule shows on 190 km outside the area (illegal bass retention shown as allowed): when → "first patch so that it has hopefully improved cycle time from the audit" (2026-11-25)
- [x] 2026-11-25 — After the first-patch plan: pick the fix (a clean cut of Creston, Bowron and Strathcona with a build check, recommended; or mostly-inside binding with no atlas rebuild) and the length allowed outside an area (default 1,000 m); Liard is left as it is unless you say. → "Yes, cut Creston; others later (Recommended)" (2026-11-25 23:27)
- [x] 2026-11-25 — Picks for the first patch (defaults first): Creston by the cut / majority for WMAs only / majority for every area; Strathcona and Bowron in the hardening release / in this patch too; the four new 10-14 m pieces at the WMA edge: accept / pictures first. → Creston and the others: "Yes, cut Creston; others later (Recommended)" (2026-11-25 23:27); then, for the folds the re-cut needs (the Strathcona cut among them): "Hold the re-cut for all of them" (2026-11-26) and Strathcona-Westmin: "Yes, treat it as Strathcona Park (Recommended)" (2026-11-27). Reading: the Strathcona cut is in the 0.10.0 re-cut (T1240 class 2), Bowron stays later. The four edge pieces: superseded by T1199's "no piece under 100 m" (#372; "Merge with the list; close both edge stretches (Recommended)", 2026-11-27).
- [ ] 2026-11-25 — The build check: 1,000 m unbroken run / 1,000 m total / 250 m, starting with a shrinking exceptions list / strict from day one (T1199's gates G1-G6 now cover cut areas; whether membership-only areas still need this check is not settled). The curated edit comes to you as a proposal with before / after pictures of Goat River and Corn Creek.
