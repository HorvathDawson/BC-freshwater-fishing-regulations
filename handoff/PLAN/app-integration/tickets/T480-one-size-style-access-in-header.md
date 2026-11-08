# T480 — One size style on the regs pane (owner 2026-11-07); access chip in the header tried and reverted

Status: DONE — owner phone check 2026-11-08 passed (0.6.9). Was: IN REVIEW (proposal) — 3a1026e20. Owner review of the Vedder River, What you can keep, phone width:

1. "Why is size written now in 2 different styles? We need to commit to a single style."
2. "Maybe the access may be restricted should live in header (without making it taller, just somewhere where there is
   whitespace already)?"

## What changed

1. **One size style** (core `tree.ts` `sizeVM`, `KindVM.size: SizeVM`; ui-native `GroupCard` `SizeChip`). Every kind's
   size is ONE chip (the existing tinted `regsChip`) holding the size words, with what happens outside it as plain words
   beside it. Words are the store's, split at their own seams (put back together they are the store's words exactly,
   pinned in `t480.test.ts` for every `tree.py size_rule` form):
   - `size_rule` "must be 60 cm (24 in) or longer; release any shorter" → chip "60 cm (24 in) or longer" + "release any shorter"
     (also "or less; release any longer", ranges / gaps "; release any other size").
   - a slot "release any between 60 cm (24 in) and 90 cm (35 in)" → "release any" + chip "between …".
   - a `detail` size ("50 cm (20 in) or less") → the chip alone, as before.
   - **the steelhead definition** (a ST kind whose detail is "rainbow trout over 50 cm (20 in)") → "Counts as a
     steelhead:" (`WORDS.COUNTS_AS_STEELHEAD`, new) + chip "rainbow trout over 50 cm (20 in)" + its ⓘ. Its presence line
     ("A rainbow trout over 50 cm (20 in) here counts as a steelhead.") said only that, so it is not drawn twice (64 of
     the 69 such kinds in the scratch release; other presence lines stay). The loose grey "rainbow trout" is gone (it is
     inside the chip). A steelhead's real size limit ("30 cm (12 in) or longer") stays a plain size chip.
   - Qualifiers ("hatchery") now lead the chip instead of trailing it.
   Not changed (not a kind's size): the size box's quota sentence ("Up to 1 over 50 cm (20 in) combined — 2 if …"), the
   DFO salmon lines (DFO's words), rule sentences in caveats/banners (`describe`), Other fish's "· size limit" hint.
   The split is layout over the store's words, no regulation decided in TS; an unknown `size_rule` form falls back to
   the whole words in the chip. Pipeline unchanged; fixtures re-cut (harness).
2. **Access chip in the header — REVERTED (owner 2026-11-07: "The access may be restricted I think for now should go
   back to where it was. This is too much in header until we figure out a better solution.")** It was built in 3a1026e20
   (a two-line chip right of the place / sun lines, a tap opening the strip under the header) and taken back out: the
   note is the full-width strip under the header again, exactly as before (T367). A better place for it is still OPEN.

Tests: `core/regs/t480.test.ts`, `ui-native/regs/T480SizeStyle.test.tsx`, `tree.test.ts` / `model.test.ts` updated; VM fixtures re-cut. Shots (390 / 360, scratch release t479, before/after: Vedder, Lac Le Jeune, Roche Lake, Cowichan; access strip
open: Vedder, Cowichan) in the session scratchpad `sizeaccess/`.
Size: S-M.
