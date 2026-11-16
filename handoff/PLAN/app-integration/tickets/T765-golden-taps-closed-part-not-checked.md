# T765 — Golden taps: a closed part is not checked, so a moved index lands on the wrong part and nothing objects

Status: OPEN (filed 2026-11-16 by the app chat; found by the main chat on release r-46e4355a7277).

Source: the main chat, 2026-11-16 03:08. Area: the app chat (`app/tools/regs-recut/`).

## What happened
- `app/tools/regs-recut/taps.py` `PART_MOVED` declares, per golden state, which store part the golden part is now
  (`"x15p9_1010": 8`). The tool checks a declared part by its label. A part that is closed all year has no label, so
  for a closed golden state nothing is compared.
- Release r-46e4355a7277 added two part ends on the Fraser below Hell's Gate (the Hwy 1 bridge at Hope and the
  Alexandra Bridge). Every index above them moved up by two. The declared index 8 for x15p9 ("From the signs 2800 m
  downstream of Hell's Gate up to Hell's Gate", closed all year) then pointed at the NEXT closed part (Hell's Gate to
  the signs 6,500 m below Boston Bar). The golden page was compared with the wrong stretch (fraser_river r3 / r12 in
  place of r2 / r11) and no check said so. The main chat fixed the declaration by hand (53d8e74c5: part 7).

## Fix
- A declared golden part is checked by what it IS, whether open or closed: its two end tokens (`part.lo_s` /
  `part.hi_s`) or, where it has none, its km mark and section count. A declaration whose part does not match is an
  error that names the state, the declared index and what the store holds there.
- Better: declare the part by its ends in the first place (as the app tests do since T667, `sectionNames.part`), so
  one more cut below renumbers nothing by hand.

## Done when
- Moving x15p9's declared index by one fails the tool, naming the state (a mutation test).
- Every closed golden state in taps.py is checked by ends or km mark.
