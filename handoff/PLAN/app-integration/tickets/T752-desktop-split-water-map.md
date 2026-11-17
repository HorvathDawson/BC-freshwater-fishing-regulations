# T752 — Desktop: the water's Map tab as a split (map on one side, the part's text on the other)

Status: DONE d3c7376a0 (owner 2026-11-17 15:04 after the desktop shots: "Keep it (Recommended)"; was: IN PROGRESS (proposal: pictures to the owner))

Source: owner 2026-11-15, staging website, desktop, a water's page, Map tab (Fraser River, part 4 of 19), verbatim:
"on desktop the section navigator should maybe be map on one side and text on other (split column?) thoughts?"
His screenshot at about 1512x760: the map is a strip about 1512x270, wider than the content column, and the controls
(the parts toggle, the part's name, Downstream / Upstream, "Show regulations for this part") sit in the 1280 column under
it with a lot of empty width.

This is a LOOK PROPOSAL. Nothing is committed in the app; the patch and the pictures are in
`~/bcfish-session-backup-2026-11-12/desktop-split/` (`split.patch`, the sheets).

## The two looks built
- A: map LEFT (about 60% of the column, the full height under the tabs, at least 560 px), the text column RIGHT (the
  part's label, name, Downstream / Upstream, "Show regulations for this part", "Show all N parts" as a list that opens in
  place and scrolls inside the column, the report link at the bottom).
- B: the same mirrored: text LEFT (fixed 420 px), map RIGHT filling the rest.
Both: "Whole river" / "This part" and the zoom buttons stay on the map; the open list marks the current part and a click
steps to it; below the breakpoint (the app's `tablet`, 900 px, `useLayoutClass`) nothing changes; keyboard order is the tabs,
then the text controls, then the map's.

## What shipping it would need (not done)
- Tests: the layout at the breakpoint (899 phone unchanged, 900 and up split; the same test class as
  `tools/water-frame-layout.test.ts`), the framing at the new map size (a part frames well in a taller, narrower
  canvas; T747's framing tests assume the strip, re-check their size assumptions), the part list scrolling inside the
  column and a click stepping, keyboard order, and the phone golden unchanged.
- Inventories: `tools/back-layers.test.ts` (the list is the same back layer, no new one expected), `tools/failures.test.ts`
  if any new read; `app/MAP.md` line for the Map view; the water-frame header comment ("the Map may go full-bleed") updated:
  the split sits inside the column, not full-bleed.
- Native tablets: the layout class is the same on a tablet (900 and up), so an Android tablet or iPad in landscape would get
  the split; decide whether it should (default: yes, it is the same room).
- Decision for the owner: A or B (or keep today's), and whether the split starts at 900 (tablet) or 1280 (desktop).

## What the build showed (2026-11-15)
- The split sits inside the frame's 1280 column (not full-bleed), so the map edge lines up with the header's gutter; at
  1920 the layout is the same 1280 block centred. The text column is first in the tree (keyboard: tabs, text, map).
- Framing: the fit uses the map's real size; Fraser part 4, Skeena part 1 and Vedder frame well at 1280x720, 1512x760 and
  1920x1080. A part that is wide (Skeena part 1) sits small in the taller pane; nothing is cut off. T747's tests assume the
  strip's size and need a second size case.
- The open part list in the text column: capped at 380 px and scrolls inside; the column scrolls as a whole under it, so the
  steps can sit below the fold at 720 px with the list open (a shipped version may want the steps above the list).
- Phone (390 wide): identical with the variant switched on.
- The proposal switch was a `globalThis.__bcSplit` flag; a shipped version picks one look and drops the flag.

## Owner answer 2026-11-15
"for the layout choose which ever you think is best practice". Main chat's choice: variant B (text column on the left,
map on the right). Reason: it is the common pattern for a detail panel beside a map (panel left, map right, as the big
map and listing sites do), and reading starts at the left with the part's name and steps. Defaults also chosen by the
main chat, his to change: starts at the existing 900 px breakpoint on every platform, so app tablets get it too; phone
unchanged. Build lane started in a worktree; lands after Play build 33 is uploaded (app/ pushes are held until then).

2026-11-17 ~15:04, owner: see OWNER-TODO 15:04 (answer recorded verbatim there).
