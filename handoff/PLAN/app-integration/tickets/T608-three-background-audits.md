# T608 — Three read-only audits before closed testing

Status: OPEN

Source: owner 2026-11-10: "we could run more audits in background with fable maybe?" Aim: the T495 gates (no wrong
regs, no crashes, clean first run) and the owner's rule that one water never shows differently by platform or path.

Each audit is read-only, writes one findings file, and files nothing; findings become tickets after the owner's look.

## A — Rules as shown vs the printed book and the DFO feed
Sample about 40 waters across all regions (big rivers, lakes with exceptions, waters with in-season or DFO rules,
waters with no exceptions). For each: what the live release says (what the app would show for today and one summer
day) against the printed synopsis page (cite printed page) and the live DFO feed. Report every disagreement with both
texts, and every rule the reader would see that the source does not support.
Findings: `handoff/PLAN/app-integration/T608A-findings.md`.

## B — One logic, two shells
Every place where `app/packages/ui` (web) and `app/packages/ui-native` (phone) each hold their own copy of a rule,
a wording, a number format, a default, a sort or a state restore, instead of calling `core`/`data`. For each: file and
line of both copies, whether they already differ, a state that shows it, the fix. Follows T580; do not re-report
T580's F1–F15 unless still open.
Findings: `handoff/PLAN/app-integration/T608B-findings.md`.

## C — The feeds worker from the outside
Every public and authed route: what an anonymous caller can read, write or cost us; token handling; rate limits; what
is stored about a person (reports, crashes, IP hashes, retention); failure paths that answer 200 with wrong content or
serve stale data without saying so; what a bad upstream (ECCC, DFO, B.C. page) can do. Read the code and the staging
worker's public answers only; no load, no fuzzing, no writes except none.
Findings: `handoff/PLAN/app-integration/T608C-findings.md`.

## Acceptance
- Three findings files, each with a ranked list (severity, evidence, proposed fix, size); tickets filed from them.

Depends: —. Size: M (read-only). Owner: reads the three summaries.

## D — Bathymetry maps and stocked lakes: where things stand (added 2026-11-10)
Owner: "look at the bathy map / digital bathymap and stocked lakes matching and tickets etc and see the state and what
needs to be done, and how the feeds for stocked lakes would work to make sure they get as up to date as possible, also
where to display the data (maybe a fourth tab in the regulation pane with more or something?). This is all audit stuff
for now."
1. State: every ticket, doc, memory note, script and data file about lake bathymetry (PDF sheets and any digital
   depth data) and fish stocking; what is built, what is matched to waters (counts), what is half done, what is only
   an idea. The check step today prints "stocking waterbody match: no roster" and "bathymetry sheet match: not built
   (2,265 in the roster)".
2. Sources: where B.C. publishes stocking records and bathymetry (formats, licence/terms, how often they change,
   whether there is an API or dated files); the self-host rule (data only from our servers).
3. Matching: how a stocked lake or a bathymetry sheet would be tied to our water ids (the gauge-match lesson: one
   frozen, reviewed match file; everything added is a proposal); how many match cleanly today and how many need a look.
4. Freshness: how a stocking feed could run on the worker (cadence, change detection, what the app shows as "as of",
   staleness words), with the cost per run; bathymetry as release data or on-demand files (sizes).
5. Display: options for where it goes (a fourth tab in the water pane, a section under Regulations, the map sheet,
   a map layer of stocked lakes, the public water page), each with what it costs and what it crowds; one recommendation.
6. A staged plan: the smallest first step that is useful to an angler.
Findings: `handoff/PLAN/app-integration/T608D-findings.md`.

## D: parked by the owner 2026-11-10 ("we can handle them later… I just wanted to look at the scale")
Findings `T608D-findings.md` (1b1cfd8a9) and `T608D-followup.md` (07e20df52: why 697 vs 28; matching rules A0-A8, B0-B7).
Owner page: https://claude.ai/artifact/7Dn9FwZu3N1aMGxsCR4ea3 (source kept as `handoff/PLAN/app-integration/T608D-page.html`;
update that artifact with `url`, do not make a new one). Ten owner decisions are listed on the page. No tickets filed
for D1-D9 until he takes it up.
