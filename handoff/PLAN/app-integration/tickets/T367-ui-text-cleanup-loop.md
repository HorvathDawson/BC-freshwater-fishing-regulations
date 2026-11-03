# T367 — UI text clean-up, owner back-and-forth (regs, conditions map, More)

Status: OPEN (owner 2026-11-03; in 0.5.0, batch 6b of RELEASE-PLAN-0.5.0.md, after the small app fixes, before the
release check).

Why: the owner wants a clean-up pass mostly in the regs pane, the conditions map pane and More: badly formatted text,
and much less top-level text (debloat what shows before you tap in).

## How it runs (owner: "a back and forth item")
A loop, one round per owner note:
1. The owner says what bothers them (screen + what it should be).
2. Claude writes a short plan for that note (what changes, which file, any data-side cause).
3. Claude fixes it on the dev web app; the owner checks it there.
4. Repeat until the owner calls it done.

Only then does it go into the 0.5.0 APK (the one APK for the batch). No APK per round.

## Notes
- Text that comes from the data (rule wording, place names) is shown as sourced. A fix to its formatting is a display
  change; rewording or dropping content needs the owner's yes (ask-before-filtering rule).
- Keep a running list of the rounds here (note → fix → owner verdict).

## Rounds
(none yet)
