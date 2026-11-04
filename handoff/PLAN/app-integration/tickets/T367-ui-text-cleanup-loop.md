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
- 0 (2026-11-03): regs loading spinner sat in the pane's top corner → centred, large size (0db30c2). Owner: OK.

Start a UI chat from `handoff/PLAN/NEXT-SESSION-UI.md`.

## From T343 (2026-11-03, data chat → UI chat)
Two salmon-card items are yours (presentational files): (a) the COLLAPSED salmon section shows one image, the
ocean-phase chinook (`app/apps/mobile/assets/fish/chinook-salmon-ocean-phase.webp`); OPENED, each species row uses its
own `<species>-salmon-spawning-phase.webp`. (b) The DFO source info (page link, date modified, notice numbers) is one
collapsible line, collapsed by default; opened it shows the full source details. The view model is unchanged (core
`salmonCard`: `source`, `asOf`, `credit`, `dfoWords`).
(c) T343: draw the salmon card's new `hours` line (core `salmonCard(...).hours`, e.g. "Salmon fishing in daylight only:
one hour before sunrise to one hour after sunset"; null when all closed) near the gear line. Then tell the data chat (or
change core `salmon.ts` `asWords`: drop `n.fold === "hours" ||`) so the daylight note leaves the region block. Before 0.5.0.
(d) Forecast day chips: the every-day feed (+2, +4, +6–9) is the DATA chat's, T375. Build the chips on the days the feed
carries (by date); the rest fill in when T375 lands.
