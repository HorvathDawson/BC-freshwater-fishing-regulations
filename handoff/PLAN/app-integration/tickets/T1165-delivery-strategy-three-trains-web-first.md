# T1165 — Delivery strategy: data, web and native trains; web first behind build-time flags; a data contract that never strands a phone

Status: OPEN — the owner's main priority (2026-11-25); the first patch after 0.10.0 is the "speed release" and its trial

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1165 | Delivery strategy: data, web and native trains; web first behind build-time flags; a data contract that never strands a phone | OPEN — the owner's main priority (2026-11-25); the first patch after 0.10.0 is the "speed release" and its trial | — | planning chat: roadmap order + the speed release scope; build lanes: a web-only release path, build-time flags, the data-contract floor + old readers in CI; then measure native churn before any OTA; with T1133, T1144, T1159, T1161 |

Source: owner 2026-11-25 night (planning chat), verbatim: "i think our main priority will be improving
development speed and how much small releases are effecting that. we can put more features into each release so it
has less ci/cd? plus try to speed it up. we need to figure out the best strat here right now we are being limited
by our builds and release etc". To "two trains / one bigger train / keep small releases, sped up": "could we do
option 1 but also allow for option 3 like right now we will need a lot of changes before app release getting all
our features out but doing some stuff in web first will help with this as long as datatypes dont mismatch forcing
app releases?? either way speed it up. then things that can be devloped in web should be ... make sense? what is
industry standard for fast rolling developement and release". First patch: "you can choose best option" (the
planning chat chose the speed release, with the Creston fix as its one data change). And: "scope the ci/cd changhes
including speeding it up too". Notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `delivery-strategy.md`, `release-timeline.md`,
`ci-merge-queue.md`, `ci-speed-build-plan.md`, `ship-workflow-design.md`, `fewer-questions.md`.

## What was measured
- The machine work of a release is small (about 4 h of 17.1 h for 0.9.0); late scope, late answers and waiting
  cost the most (T1159). A code PR takes 40 min because the same tests run three times (T1144). The 37 code PRs of
  0.9.0 took a median 24.7 h from merge to production data.
- **The web cannot ship without the phones today:** any change under `app/` bumps the app version and the runbook
  then builds both phones (`release-by-hand.md:80-81`). A data-only release exists; a web-only one does not.
- **Over-the-air phone updates fit less than hoped:** 20 of the 32 app versions since 0.4.0 changed native code or
  config, and every version since 0.8.3 did; an update is about 2.5 MB. Apple allows downloaded interpreted code
  within the app's purpose, and guideline 2.5.2 bars code that adds features; Google's rule clearly allows it.
- **The data contract has two blind spots:** the release gate does not check the answers-section versions or the
  list of required files, though the app does; nothing stops a data release from cutting off the phones in use.

## The strategy (the industry pattern, fitted to one owner and agents)
Trunk-based merges behind fast CI; unfinished features merged switched off; the web deploys often; phones ride a
slower train; versioned data with a test that the oldest supported app reads the newest data.
1. Three trains: **data** (any day, one approval once T1133 exists), **web** (as often as daily, one click),
   **native store builds** (batched every three to four weeks). Small releases stay allowed on the data and web
   trains; the store train is where more per release pays off.
2. A web-only release path: `release --cut --web` (S), so a web change does not force phone builds.
3. Build-time release flags only (S–M; no third-party service): a half-built feature merges off, ships to the
   web first, and reaches the phones at their next train; a flag is removed when the feature is on everywhere.
4. The data contract: a "floor" (the oldest app version in use that a data release must keep working) that stops
   a release; an additive-only test for the formats; the old app readers since the floor run in CI against the
   committed mini release; the gate checks the answers-section versions and the required-files list.
5. OTA: measure how often native code changes for two weeks after the trains exist, then decide; if yes, for
   fixes only, from the release branch, self-hosted and signed.
6. Public PR previews of the web app on staging data.
The first patch after 0.10.0 is the speed release: T1144's CI changes, T1159, T1161, the first steps of T1133
and of this ticket, T1160's key scoping, and T1149's Creston fix as the one data change; its cut-to-production
time and question count are compared with 0.10.0's.

## Acceptance
A web-only change reaches production without a phone build; a data release that would strand the floor app
version is refused by a check (mutation); the speed release's numbers are in T1159. Severity: the owner's main
priority. Model: Opus for the contract and release tooling. Size: L across lanes. Owner: the picks below.

## Owner

- [x] 2026-11-25 — Release shape → "could we do option 1 but also allow for option 3 ... doing some stuff in web first will help with this as long as datatypes dont mismatch forcing app releases?? either way speed it up" (2026-11-25); the first patch → "you can choose best option" (2026-11-25; the speed release with the Creston fix)
- [ ] 2026-11-25 — Picks (defaults first): data, web and native trains now, over-the-air updates only after two weeks of measuring; web production: one click a day / automatic for web-only changes; public PR previews on staging data: yes / no; flags: build-time only / also remote; the floor rule stops a release / only warns; old readers kept since the floor; OTA, if ever, for fixes only.
