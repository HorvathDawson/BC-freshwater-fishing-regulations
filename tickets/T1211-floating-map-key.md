# T1211 — A floating "Map key" over the map, and the words "Its own rules / Region rules only"

Status: IN PROGRESS — lane T1208 (worktree wt-T1208); release 0.10.0 re-cut

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1211 | A floating "Map key" over the map, and the words "Its own rules / Region rules only" | IN PROGRESS — 0.10.0 re-cut | — | build in progress (lane T1208) |

Source: T1200 (owner 2026-11-26: "Floating \"Map key\" over the map, near the bottom (Recommended)"; the key's words:
"Its own rules / Region rules only").

## Work

1. The Map tab's key (Trout and Salmon) is a chip floating over the map near the bottom, the map behind it; a tap opens
   the whole key in a card above it, a second tap closes it. The bar under the map goes.
2. It covers neither the scale bar, the attribution nor the rail; a 44 x 44 target; named for screen readers; 320-412
   wide; text at 1.3 and 2.0 by `type.ts` (`LABEL_PROPS`, `labelMinWidth`).
3. The words "Its own rules / Region rules only" wherever the two statuses are named (map key, status labels, Help).
4. The walkthrough and Help that point at the old key follow it. The Conditions tab's key is out of scope (its own
   component).

## Acceptance

A render test of the key (closed, open, its name, its target); the words pinned; pictures at 412 and 360 at text 1.3.

Depends: none. Size: M. Owner: no.
