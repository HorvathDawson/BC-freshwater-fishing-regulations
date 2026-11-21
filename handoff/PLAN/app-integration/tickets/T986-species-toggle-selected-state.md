# T986 — Trout / Salmon switch: which one is on is hard to tell

Status: OPEN — joins the 0.8.5 batch (owner 2026-11-21: "Another feedback to add to this build").

Source: a tester, via the owner's messages 2026-11-21 12:43 (shot: `T986-shots/species-toggle-tester.png`, the app part
only). Verbatim: "Its hard to tell which one is toggled for the trout/salmon … Because when the trout is toggled it could
be both … You could like move the bar so it is on the left border instead?"

## Problem
The selected-mode bar is drawn under the TROUT button, i.e. on the line between TROUT and SALMON, so it reads as
belonging to either.

## Work
Design proposal with pictures (batched owner question): e.g. the bar on the outer (left) edge of the selected button,
or a filled selected button. Keep `aria-selected` / role=tab (AGENTS 34). Same on Android, iPhone, web.

## Acceptance
The owner picks a look; the selected mode is unambiguous in light and dark.
