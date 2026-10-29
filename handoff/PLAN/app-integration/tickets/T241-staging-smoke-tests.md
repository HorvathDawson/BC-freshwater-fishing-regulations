# T241 — staging smoke tests after every deploy

Status: OPEN — not started.

Source: owner 2026-10-28 — approved proposal P7 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
`app/tools/smoke-staging.mjs` (Playwright): against https://staging.canifishthis.ca — map paints (canvas non-blank,
tiles 200), tap a known river → regs page, Conditions view shows a gauge, Salmon mode colours, the synopsis PDF opens,
a `/water/<slug>` page 200 with canonical, a legacy `/waterbody/...` → one 301, the in-map attribution names
OpenStreetMap; desktop + phone viewport; exits non-zero with a short failure list and shots. `deploy/staging/site.sh`
and `upload.sh` run it at the end (opt-out flag).
## Acceptance
Green on the live staging; a deliberately broken check (wrong slug) fails loudly; < 2 min.
Depends: none. Size: S. Owner: no.
