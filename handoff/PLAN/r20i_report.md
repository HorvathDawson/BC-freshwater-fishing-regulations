# Round 20i — the UR4 user panel's page-side fixes (2026-10-19)

Source: the UR4 panel (45/46 right; ~/.claude/projects/…/handoff/PLAN/USER-ROUNDS.md "Round UR4"; screens app/design/regs-v11/screens/ur4/).
Data: 9fc6d7b1 (rows 26, gear 12, glossary 5; handoff/PLAN/r20i_data.md), re-snapped. Page: regs v33; details UI-NOTES v33.
Scratchpad: /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20i/ (frames-after = the UR4 frame set re-shot on the result; after = every tab, light + dark; gate-after = the tall set).

## What changed on the page, issue → fix → where to look
| UR4 issue (persona) | fix | before → after |
|---|---|---|
| "Arctic grayling 4 a day" hid a 37–43 cm release slot (Gord G5) | the row says "4 a day · size limit ›" when its kinds carry a size rule | screens/ur4/frames/bennett-f03.png → r20i/frames-after/bennett-f03.png |
| size chips read as facts, not rules (Maya M3, Claire C5) | rows 26 `size_rule` on the chip: "keep 30–50 cm (12–20 in); release the rest" | ur4/frames/chilliwack-p2-oct10-f02 → r20i/frames-after/chilliwack-p2-oct10-f02; bella-coola-may1-f02 |
| a line half under the pinned bar; a heading stranded at the frame's edge; the Duncan line split from its picture (Claire, Dale, Brent, Rob) | `--pin-h` + scroll-padding / scroll-margin under the stack; frames.mjs steps back by the stack and never ends a frame on a heading | ur4/frames/bella-coola-may1-f02, chilliwack-p2-oct10-f01/f02, glenn-jun15-f01/f02 → r20i/frames-after/ (same names) |
| "stop fishing here" in small grey text; the stamp only on Licence (Kev K5, K4) | the duty line is a full-weight ink line, "this river" → the water's name, "Steelhead stamp ›" on the steelhead row | ur4/frames/vedder-oct10-f02 → r20i/frames-after/vedder-oct10-f02 |
| "Closed from Jun 1" (Arjun A6) | "Closed Jun 1–30" from the strip's runs | ur4/frames/vedder-may15-f01 → r20i/frames-after/vedder-may15-f01 |
| Glenn's year-round bait ban with no span (Rob R4) | "NOW All year: No natural bait of any kind while the ban runs." | ur4/tall/t-glenn-jun15-gear → r20i/after/glenn-jun15-gear-light.png |
| four licence items read as four purchases (Arjun A5) | the "Only if…" stamps under "Also, only if…" after what everyone needs | ur4/tall/t-vedder-may15-licence → r20i/after/vedder-oct10-licence-light.png |
| grey text and the amber "release" wash out in sun (Dale, Gord) | --sub 5.83 → 6.79 : 1 (light), 7.66 → 8.87 : 1 (dark); "release" bold | r20h/after/*-keep-light → r20i/after/*-keep-light |
| steelhead stamp listed where steelhead are closed (owner O3) | dropped from that water's list when the card's not_kept says steelhead closed | r20i/after/bella-coola-may1-licence-light.png |
| kokanee and the salmon stamp (Brent B7) | the short line keeps the record's "(not kokanee)" | r20i/after/kootenay-lwa-oct12-licence-light.png |

## Readers
Two fresh opus readers, PNGs only (prompts /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20i/prompt-gate39.md and prompt-after.md; answers saved beside them as reader-gate39.md / reader-after.md).
- The 39-question comprehension set (r20h's set on the r20i gate-after + rows-after + NEW shots): **39 / 39**, no new wrong answer
  (r20h AFTER 39/39). Lowest: Q17 Glenn "other trout" conf 3 (was 4): "If you keep 2 rainbow trout: none from other streams" still
  reads as if the rainbows use up the stream 2 (data words; logged for UR5/owner). F2 "of the 2" conf 4.
- The UR4 fix questions (14, on the re-shot UR4 frame set): **14 / 14**; the UR4 failures all read right: Glenn R3 "yes" (conf 3,
  was ✗2), Bennett grayling "4 a day · size limit" → tap before keeping (conf 4; UR4 Gord nearly kept it), size chip "release smaller
  ones" (4), no heading under the bar on Chilliwack f01/f02 (4; UR4 Dale conf 2), "Stop fishing the Vedder River after 2 a day" (4),
  Vedder May 15 "NEXT Jul 1–Apr 30" (5), "(not kokanee)" (5), no steelhead stamp on the Bella Coola (5), Glenn "All year: bait ban" (5).
- Still noted by readers (owner / next round): the Bella Coola licence's carry box still says "Keep an adult chinook or hatchery
  steelhead?" on a water closed to steelhead; "Closed Jun 1–30" leaves "reopens Jul 1" to inference (both readers got it); a
  scroll-step frame can open with a lone "Bull trout ›" link under the bar (frame stepping, not a page fault).
- After the readers: the Other-fish "· size limit" hint now shares the count's baseline (it floated high as its own flex item);
  Bennett frames re-shot (/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20i/look-hint/bennett-f03.png).

## Checks
round20i: 8 checks (r20i-size-rule-chip, -other-size-hint, -duty-line, -licence-stamps, -closed-span, -all-year-ban, -glare,
-scroll-margins), all fail on regs v32 (/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20i/check-20i-before.txt), pass after. Full check.mjs **111 / 111** (/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/266b6a4b-cf8d-417b-b326-25e034ead4e0/scratchpad/r20i/check-full-4.txt).

## Commits
regs v33: see git log ("Regs v33 (round 20i …)").
