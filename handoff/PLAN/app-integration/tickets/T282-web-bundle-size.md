# T282 — Web app bundle: 4 MB delays the app taking over (43 s on Fast 3G)

Status: OPEN (found by T269 on 2026-10-29; not blocking — the prerendered pane is readable the whole time).

## Work
Measure the web export's JS by package (source-map-explorer or equivalent). Split what a first screen doesn't need
(conditions charts, runs, menu pages, walkthroughs, review/debug code, large word tables) into lazy chunks; drop dead weight
(dev-only code, duplicate libs); keep the map + regs pane path first. No look change.

## Acceptance
Before/after bundle sizes and Fast 3G time to "app took over" on /water/<slug> and on /; all checks green.

Depends: T269. Size: M. Owner: no.
