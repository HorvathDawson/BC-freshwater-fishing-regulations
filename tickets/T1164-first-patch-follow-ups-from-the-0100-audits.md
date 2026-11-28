# T1164 — First patch: the low findings of the two independent 0.10.0 audits (walk outflow guard, distance words, privacy list, an error boundary)

Status: PARTIAL — item 1 built (0e63889ef, ec748430d, 1c35257ee, 351987f66; PR open for review); left (Claude): items 2-5

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1164 | First patch: the low findings of the two independent 0.10.0 audits (walk outflow guard, distance words, privacy list, an error boundary) | PARTIAL — item 1 built, in review | 0e63889ef, ec748430d, 1c35257ee, 351987f66 | items 2-5 (walk guard, distance words, in-season diff, privacy list); item 1 follow-up: the gate misses throws in core presenters; S–M |

Source: the two independent read-only audits of 0.10.0 on 2026-11-25 night (notes: `tickets/archive/night-notes-2026-11-25.tar.gz`
`audit-0100-app.md`, `audit-0100-pipeline.md`). Their HIGH and MED findings were handled in the release that
night (the tidal-stretch crash in the part-names change, fixed in its lane before any push; the privacy page's
"0.9.1", PR #270; the in-season comparison run before production). Verdicts: the pipeline side "ship, no HIGH"
(all 56 sections T1146 gained were hand-checked: each channel ends with exactly its river's rule set; `--prod` /
`--final` cannot ship a tree other than the tested rc); the app side "ship after the crash fix"; a 0.9.0 app
reads the 0.10.0 formats and the reverse. These are what was left:

## Work
1. BUILT 0e63889ef, ec748430d, 351987f66 (`ScreenBoundary`, `Shell.tsx` `screen(…)`, a boundary per menu page, one for the sheets, search and the map key, a root one; `check-no-render-throw.mjs` with 7 listed throws in 6 functions, each with a count and reason). The app had no React error boundary, so any throw in a render path is a full crash (the tidal-stretch case
   showed it): a boundary per screen with a shown message and a report; a lint or test that new render-path code
   does not `throw`.
2. T1146's walk has no outflow guard (`tributaries.py:1154-1159`): a channel that drains into another basin can
   take the river's rules (MacMillan Creek's channel into Diana Lake; no visible change today, it already carries
   the Skeena bait ban). The owner, 2026-11-27/28 (OWNER-UNFILED "T1146 follow-ups"): the channel is NOT Skeena water and
   does not take the Skeena River's own row; built here, after 0.10.0, unless he pulls it into the re-cut.
3. T1140's distance words (`side_channels.py:273-275`, `164-170`): a channel that leaves the river and flows into
   another named water is measured from its far end; the "nearest" wording has no distance cap; "into <Lake>" can
   name a lake the channel flows OUT of; the orientation uses the nearest section below by distance, not the one
   directly below. No instance known in 0.10.0's data.
4. In-season notices go through T1146's walk (`pipeline/regs/inseason/points.py:241`) and are not in the binding
   diff: the comparer written that night (`inseason_diff.py`, in the notes archive) becomes part of the diff tool.
5. The privacy page's list of website scripts leaves out the /gauges search (`app/tools/seo/html.ts:302`; with
   T1148); the owner's list in `T1098-shots` still reads 0.9.1.

Item 1, left (Claude), added after the build (root boundary: ec748430d):
- The throw gate (`app/tools/check-no-render-throw.mjs`) sees only a `throw` written straight into a component, hook or
  render-time callback. It does not see a throw inside a core presenter (or any helper) that a component calls: that still
  reaches the boundary at run time, and no gate names it. A follow-up would walk calls from components into `packages/core`.
- The real Shell IS rendered with a throwing screen now (`ScreenBoundary.shell.test.tsx`, the Spots tab, on NavRail.shell's setup:
  fixture source and regs loader, map stub). Not covered: a throwing screen inside a water's page (WaterModeHost) or a spot.

Finds from the independent review, not built (left, Claude):
- A held water pane hides its fallback for up to 1.2 s (`paneReveal` PANE_HOLD_MS): a water that throws while its pane is held shows nothing until the hold ends.
- On the web, keyboard focus is lost when the faulted subtree unmounts (the fallback's Try again takes none).
- Whether remounting `App` on the root boundary's Try again registers its listeners twice (the module-level installs run once; the effects re-run).
- The report cannot say which boundary caught a fault: `reportFailure(error)` carries only the error (the payload is kind, name, scrubbed message and stack, and the route's screen kind). Adding the screen name needs a place in the crash payload and its scrubbing rules.

## Acceptance
Each item has a test that fails without it. Severity: low each; item 1 medium. Model: Opus (1-4). Owner: no.
