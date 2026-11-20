# T773 — A tidal boundary cut written from a mark always reads "tidal boundary"; the owner's name for it is lost (Kitsault)

Status: OPEN (pipeline + review app; small).

Source: owner note on mark 2682 (Kitsault River); second-look fix lane B, 2026-11-15 night ("Stopped or held").

## What is wrong
A `mouth` row written by a mark gets the fixed label "tidal boundary" in code. The owner's note asked for a name for
the Kitsault one; it could not be changed without a code change or a new cut, so nothing was changed.

## What to build
- A mouth row may carry a label from the mark; "tidal boundary" stays the default when none is given.
- The review app's tidal form has the field; the card and the cut list show it.
- Then the Kitsault mark is re-saved with its name (unsure, for the owner's re-tick).
