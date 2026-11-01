# T344 — Species identification: swipe between the fish a species line covers

Status: OPEN (owner 2026-10-31: "for the species identification, if you clicked one that has multiple we should be
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
