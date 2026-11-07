# T480 — One size style on the regs pane; "Access may be restricted" in the water header (owner 2026-11-07)

Status: IN REVIEW (proposal) — 3a1026e20. Owner review of the Vedder River, What you can keep, phone width:

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
2. **Access chip in the header** (ui-native `AccessNoticeBlock` `AccessChip`, `RegsHeader` `aside` slot, `RegsScreen`).
   The gentle note ("Access may be restricted") is a compact two-line chip (accent rail + tint, max 108 px) at the right
   of the place and sun lines, bottom-aligned. Header height measured unchanged (125 px at 390 and 360) for Vedder, Roche
   Lake, Lac Le Jeune. LIMIT: a long extent that fitted one line now wraps beside the chip — Cowichan ("From the mouth up
   to the CNR Mile 66 Trestle") 125 → 142 px at 390 and 360; no overlap, and still 19 px less than before (the 36 px
   strip is gone). Owner call: accept, or the chip only beside the sun line (one line needs ~160 px, the sun line leaves
   ~124 at 360, so the words would have to shorten). A tap
   opens the same strip as before under the header (title · what the land is, then the sentence); a tap on the chip or
   on the strip closes it. The no-access BANNER stays a strip (it is the warning). The Conditions view's strips are
   unchanged.
   Open for the owner: should the no-access banner become a header chip too?

Tests: `core/regs/t480.test.ts`, `ui-native/regs/T480SizeStyle.test.tsx`, `AccessNotice.test.tsx` (chip + strip),
`tree.test.ts` / `model.test.ts` updated; VM fixtures re-cut. Shots (390 / 360, scratch release t479, before/after: Vedder, Lac Le Jeune, Roche Lake, Cowichan; access strip
open: Vedder, Cowichan) in the session scratchpad `sizeaccess/`.
Size: S-M.
