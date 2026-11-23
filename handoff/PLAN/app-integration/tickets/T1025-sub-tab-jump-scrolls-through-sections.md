# T1025 — Switching regs sub-tabs ("What you can keep" ↔ "Licence") scrolls through every section between

Status: DONE (code 641725521) — a regs sub-tab press now JUMPS (`animated: false`), so no pass through Gear/Methods and the scroll-spy highlight cannot flicker. RegsScreen.test asserts the jump. NOT recorded on an emulator.

Source: a closed tester, relayed by the owner 2026-11-22 (screen recording, Android dark, Alouette River "From 216 Street
bridge up to the Alouette boundary signs"). Verbatim: "The only weirdness I've seen is toggling between What you can keep
and Licence … It goes through all sections, but that's not unreasonable". Same message: "Really nice! Very information
dense but still readable and everything makes sense."

## Work
1. Reproduce: tapping a sub-tab far from the current one animates the scroll through Gear/Methods/… on the way.
2. A tap on a sub-tab lands on its section without the long animated pass (jump, or a short fade/scroll capped in time);
   the tab strip highlight must not flicker through the sections passed. Web and native (rule 34a if it differs).
3. A test on the scroll call; before/after recording on the emulator.

Depends: none. Size: S. Owner: no.
