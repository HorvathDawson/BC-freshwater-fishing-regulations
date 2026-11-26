# T740 — The map kept under a water's page costs memory (iPhone measured, Android not paired)

Status: OPEN (owner 2026-11-17 ~15:11: "After 0.8.0 (Recommended)"; 2026-11-18 put on the 0.8.1 list ("All these fixes can go in 0.8.1"), not built in 0.8.1 to 0.8.3; first step: one like-for-like measure; fix may be small)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T740 | The map kept under a water's page costs memory: +49 to +66 MB with a page up on iPhone, 389 MB peak with the rule book over it (Android not paired) | OPEN (owner: after 0.8.0, then the 0.8.1 list; not built yet; one like-for-like measure first) | — | iOS chat's memory scenario 2026-11-14 (T725) after T717; option: drop the hidden map's tiles while keeping camera and colouring |

Source: iOS chat's memory scenario, 2026-11-14 (written in T725, fd045cd3c), after T717 (6c4547b13: the browse map is
kept mounted, hidden, under a water's page so Back shows it at once).

## Measured (iPhone 17 simulator, build 081d93f90 against baseline 8ba815a23)
- A water's page up (Cultus): 279 MB against 230 / 213 MB: +49 to +66 MB.
- Peak (the rule book opened a second time over a page): 389 MB against 274 MB.
- With a page up, closing the rule book frees about 10 MB (348 → 338; 389 → 343); the baseline freed about 50 MB.
  Leaving the page drops to 158 MB. So about 180 MB is held while a page is up after the book was opened.
- Not like for like: the new build carries every change since the baseline, not T717 alone; two baseline runs differed
  by up to 40 MB on one step; the simulator's memory-warning trim frees nothing in either build.

## Android (build 32 verify lane, emulator)
Measured only after T717, no before/after pair: 354-411 MB with a page up over 15 Conditions repeats, 332-404 MB over
15 Map repeats, level, no climb. The T717 lane's own estimate of the kept map was 10-25 MB.

## Work
1. One like-for-like pair on each platform: the same build with the kept map switched off by a test-bed flag (not a
   shipped setting) against it on: page up, rule book open, rule book closed, page left. That splits T717 from the rest.
2. If the kept map is the cost: while it is hidden, drop what is cheap to get back (tile cache, off-screen sources;
   MapLibre has a low-memory call on both platforms) and keep the camera and the colouring, so Back is still immediate.
   Re-run T717's `back-flash` scenario: 0 flash frames must hold.
3. The rule book over a mounted map (the 389 MB peak, and the 10 MB freed on close) may be a second thing: say what holds
   it (the PDF view kept alive, or the map under it) and whether closing the book should release it.
4. A memory line in the test bed that fails when a page up costs more than an agreed figure over the map alone.

## Acceptance
- The pair's numbers in this ticket for both platforms.
- A page up costs no more than about 25 MB over the map alone on either platform, or the reason it cannot is written
  here with the owner's call.
- `back-flash` still 0 flash frames.

## Owner

- [x] 2026-11-17 — Kept-map memory: when? → "After 0.8.0 (Recommended)" (2026-11-17).
