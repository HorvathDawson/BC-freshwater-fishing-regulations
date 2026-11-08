# T475 — Quota block: one clear headline, sub-limits as parts of it (owner 2026-11-07, four asks)

Status: DONE — owner phone check 2026-11-08 passed (0.6.9). Was: IN REVIEW — 225ad72a5 14a759829. Code + tests in; scratch rebuild (answers/store/release r-4e3c9abcbada, not promoted) passes the card gate (0 problems, 17,173 groups); its diff vs the live answers moves only card wording (0 non-wording changes). Proof pending: the owner's look at the Fraser R2 shots, then a release.

Asks (verbatim in the coordinator's message): (1) trout/char stream share as one big line "2 hatchery trout/char a day from streams"
with an ⓘ explaining the regional total shared with lakes and 2–3 examples from the numbers; How to tell and the book link stay
reachable; (2) steelhead line concise ("Steelhead: closed, no catch and release"), accidental-hook meaning in the ⓘ; (3) every
sub-limit one pattern, "up to N", no "only"; (4) sub-limits read as parts of the headline total ("Up to 2 brook, brown or cutthroat
combined"), not "Of your 2 trout and char a day in streams, …" on each line.

Where the words are made (found):
- headline "2 a day per angler from streams" and "of the 4 … across Region 2, lakes included": pipeline `answers/card.py` (`head`,
  `unit`) and `tree.py` (share/total tier `says`); the app only splits them (`core/regs/grouphead.ts groupHeadOf`, form a).
- "From streams, keep hatchery fish only — wild trout and char go back.": pipeline `card.py` ~l.705-716 (`origin`, `share_origin`);
  it reaches the app as a STRING on the share tier (`TreeTier.origin`). Folding "hatchery" into the headline from data fields needs a
  structured field (e.g. `origin_kind: "hatchery"|"wild"` and the group noun) exported on the share tier / group; the app must not
  parse the sentence.
- kind lines "Of your 2 trout and char a day in streams, up to 2 may be hatchery …" / "only 1 may be": `tree.py` `kind_names`, the
  kind `says` and `total_named`/`total_words` (T128), and the "only" form; steelhead line from the steelhead presence/`size_note` words.
So the change is in the PIPELINE export (+ regenerate `answers.generated.ts`, the golden/frozen fixtures, and a bundle rebuild, which
the owner said not to run tonight), then the app draws the new fields (ⓘ via the existing ⓘ/term pattern).
Test required when built: over a hatchery-only stream, a wild-allowed stream, a lake, a water with no regional total, plus
sub-limits (combined group, single species, size condition) and the steelhead line: every rule still present (the card gate's
`check_tree` proves each number and kind once).
Size: M-L.
