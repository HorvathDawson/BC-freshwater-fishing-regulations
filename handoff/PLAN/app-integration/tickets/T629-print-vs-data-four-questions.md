# T629 — Owner: four places where our text differs from the printed book (no value changes)

Status: DONE — 4734dbdca (items 3, 4); items 1, 2 kept as they are on the owner's answer

Source: T553 report 2026-11-11. None of these changes a rule value. Each needs the owner's reading.

1. Region 7B possession exception: our wording against the printed wording, p.64.
2. Provincial "no fishing" buffers: we number two points "3." and "4."; the book prints bullets, p.9.
3. licensing.md has a WILD/FWID paragraph ("2026-27 licence year") that is not in the repo's PDF. Keep, source it, or drop?
4. provincial-regulations.md has our own commentary ("26 rules", "25,772 sections") inside the transcription half. Move it out?

## Acceptance
- The owner answers each; the text is changed to match and the T553 pins updated.

Depends: T553. Size: S. Owner: four answers.

## Owner answers 2026-11-11
3. Keep the WILD/FWID paragraph; find and name its source, and mark it as not from the book.
4. Move our own notes out of the transcription half.
Items 1 and 2 wait: the owner needs the book's words beside ours (with the T623 readings page).

## Owner answers 2026-11-11 (items 1 and 2)
1. Region 7B possession wording: ours is fine. Keep; the print check keeps its allowance.
2. p.9 bullets: leave as is. We keep only the 23 m and 100 m bullets because the other three are on the map and shown.
   Owner: "unless you can find a way to get all the fishways and government fish facility locations: I couldn't find them
   but that would be sick". See T635.

## Done 2026-11-12
- Item 3: the WILD/FWID paragraph moved out of the transcription, after "What this settles", under "Not from the book".
  Source: the exact words were not found online; the facts are in the Province's news release 2026WLRS0004-000093
  (February 2026, https://news.gov.bc.ca/releases/2026WLRS0004-000093), which the note names. Note for the owner: the
  "Proof of Identity" paragraph also says "state your FWID"; the print check does not flag it (no figure), so it was left.
- Item 4: the Haida Gwaii commentary and the Region 3 "kept here as a test case" line moved to a "What this settles —
  our notes" section at the end of `provincial-regulations.md`.
- The four `DISAGREE` lines they caused (licensing 2026/27, provincial 26/772) are removed; the print and quote tests
  pass (`test_base_rules_in_print.py`, `test_catalogue_entries.py`). The bundle reads neither file; nothing ships.
