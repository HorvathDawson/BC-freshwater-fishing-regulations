# T763 — Kootenay Lake's rainbow card says "20 a year · Record each on your licence right away" without "over 50 cm"

Status: DONE (7958b3c60, e3d6d5f26; on staging in r-46e4355a7277, 2026-11-16).

Source: trout display review 2026-11-15, data note 1 (`handoff/PLAN/design/trout-display-review-2026-11-15.md`); traced
read-only by the app chat on release r-f749475d0962, 2026-11-15 22:45.

## What happens
- Printed (Region 4, p.37, `r4:kootenay_lake_main_body_for_location_see_map_on_page_34@4-19`): "Bull trout daily quota
  = 1; rainbow trout daily quota = 10 (any size)" and "Conservation Surcharge Stamp required to keep rainbow trout over
  50 cm, annual quota = 20".
- Corpus: `kootenay_lake_main_body.r4` take 10, any size; `kootenay_lake_main_body.r6` take 20, annual, lengths
  min 50 cm; `zp:kootenay_rainbow_stamp.r1` record on the licence, lengths min 50 cm.
- Shipped answer for Kootenay Lake, Main Body (`wbk:-20`, part 0), rainbow row: the kind is "Up to 10 rainbow trout",
  detail "any size", duties ["20 a year", "record each on your licence right away"]. No size in the duties. (The stamp
  line keeps its condition: "Only if you keep rainbow trout over 50 cm".)
- The card therefore reads "20 a year · Record each on your licence right away" under "any size": the reader is told
  every rainbow counts toward 20 a year and must be recorded. The book says only those over 50 cm.
- Cause: `pipeline/deliver/answers/card.py:347-350` (`_duties`): the annual line is `"<take> a year"` and the record
  line a fixed sentence; both ignore the rule's `lengths`. The app only joins the stored duties (`core/src/regs/tree.ts:185`).
- How wide: today exactly one answer key shows wrong text (Kootenay Main Body). The other yearly limits with a size
  band (Shuswap Lake r8 rainbow over 50 cm, r10 lake trout / Dolly Varden over 60 cm, `z3:shuswap_annual`) sit on kinds
  whose own detail already says "50 cm (20 in) or longer" / "60 cm (24 in) or longer" (Shuswap, Little Shuswap, Little
  River, Mara Lake), so "5 a year" reads right there. Steelhead "10 a year" (about 360 kinds) has no band in its rule.
  The code fault is general: any annual or record rule whose band is narrower than its kind's.
- Not checked: staging was not screenshotted again (the review did; its view matches); which owner ruling chose the
  short duty text beyond the code comment "UR1 G; ROUND20g 2".

## Work
`_duties` words the band of an annual or record rule whose `lengths` narrow the kind ("20 over 50 cm (20 in) a year";
"record each one over 50 cm (20 in) on your licence right away") and keeps the short form when the kind's band is the
same. Rebuild the answers. Whole-sentence tests.

## Acceptance
- The Kootenay Main Body rainbow duty line names over 50 cm (20 in) in both parts.
- The Shuswap-family and steelhead duty lines are unchanged (diff of every duty line across the release: one key moves).
- A test pins the Kootenay sentence, and a case where the band equals the kind's keeps the short form.

Size: S. Area: pipeline answers (the main chat). The app needs no change.
