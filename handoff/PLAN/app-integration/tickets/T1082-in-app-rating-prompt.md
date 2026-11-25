# T1082 — An in-app star-rating prompt that does not get in the way (design first)

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1082 | An in-app star-rating prompt that does not get in the way (design first) | OPEN — 0.10 or 0.11; design page for the owner first | | owner 2026-11-24, via the 0.9.1 chat |

Source: owner 2026-11-24 ~14:25, relayed by the 0.9.1 chat, verbatim: "we need to add a random (sometimes on launch in
a reasonable amount) star rating review on launch. must be uninvasive. (or could be after they use a certain feature?)
needs thought for 0.10 maybe? or 0.11?"

Notes from the 0.9.1 chat (not the owner's; to be checked against the platforms' current docs before building):
the platform dialogs are the Play In-App Review API and iOS SKStoreReviewController; both throttle themselves and
give no callback on whether the dialog showed; Apple caps it at 3 a year; Play forbids asking a question first to
filter happy users; usual practice is to ask after a success moment, never mid-task; Play In-App Review works only
for installs from Play, mainly on the production track, which ties it to production access (T1046).

## Work
1. Design first, no build: when to ask. Options for the owner with a recommendation: (a) at launch, at random, with
   a cool-down; (b) after a success moment (the Nth water opened, a spot saved, a day checked); (c) both, with one
   shared cool-down. Never offline-critical moments, never mid-task, never the first sessions.
2. Rules: our own cool-down and a lifetime cap on top of the platforms'; never again after the reader rated or
   dismissed N times; nothing shown on a sideloaded or staging build; a way to rate from More → About at any time.
3. Read the platforms' current rules (Play In-App Review, StoreKit review request) and cite them in the ticket.
4. Then build with the platform dialogs through a well-established package (owner rule: a better established
   package beats a minimal one); tests on the trigger and cool-down logic; a staging switch to force it for a look.

## Acceptance
A one-page design the owner agreed to; after the build: tests for the trigger, cool-down and caps that fail when
loosened; a device look on each platform.

Depends: T1046 (Play production access) for the Android half to be useful. Severity: feature. Release: 0.10 or
0.11 (owner: "needs thought"); the design can be done earlier. Model: Sonnet for the design page, Opus for the
build. Size: S for the design, M to build. Owner: yes (picks when it asks).
