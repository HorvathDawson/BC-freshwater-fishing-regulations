# T344 — Species identification: swipe between the fish a species line covers

Status: DONE in code (device check in the batch 5 emulator sitting) (09d2d845e) 2026-11-04 (was: OPEN (owner 2026-10-31: "for the species identification, if you clicked one that has multiple we should be)
able to swipe between them, like whitefish (2 types)").

When a rule line names a group that covers several fish, tapping it should open the species ID view as a SWIPEABLE set
(one page per fish, dots or "1 of 2"), not a single image:
- "Whitefish" → lake whitefish + mountain whitefish (`lake-whitefish.webp`, `mountain-whitefish.webp`);
- any other grouped code the regs use (e.g. trout/char groups, "salmon" → each species; a species with ocean +
  spawning phase art → both phases as pages).
Build the group → members map from the regs species codes (one place, shared), not per screen. Single-fish lines stay
as they are. Swipe must not fight the sheet's own drag or the edge-back gesture; works with TalkBack/VoiceOver (next/
previous actions). Test on the emulator and the iOS simulator.
Size: S–M.

## Done (2026-11-04)
A group the species book names (WHITEFISH: LW, MW; BASS; CHAR; TROUT_CHAR) opens one page per member with a sheet,
swiped between (`SwipePages`: snapping pager, "1 of 2", dots, adjustable for TalkBack/VoiceOver); a group with no picture
of its own shows its first member's. A species' own several looks (ocean + spawning) stay side by side as T46 laid them
out (owner-approved), not pages. Left: the swipe vs sheet drag / edge-back check on the emulator and the simulator.
