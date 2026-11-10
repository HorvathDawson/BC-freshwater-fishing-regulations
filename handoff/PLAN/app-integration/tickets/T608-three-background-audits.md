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
