# T765 — Golden taps: a closed part is not checked, so a moved index lands on the wrong part and nothing objects

Status: IN PROGRESS (built on branch app-fix/t765-t768; not merged) (filed 2026-11-16 by the app chat; found by the main chat on release r-46e4355a7277).

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

## Added 2026-11-16 04:44 (the general audit, `handoff/PLAN/audit-2026-11-16/guards.md`)
- Four closed golden states are held only to "the part at that index is closed": x15p9, x16p7, chw0, x17p5.
- `taps.py` stops at the FIRST stale declaration; on r-46e4355a7277 that cost three release attempts. It must list
  every stale declaration in one run.
- Five comments still cite the removed `exclusions.ts`: `app/tools/regs-recut/taps.py` (about lines 41 and 47),
  `app/packages/core/src/regs/testing/golden.ts` (30 and 37), `app/packages/core/src/regs/tree.ts` (54).

## Built (branch app-fix/t765-t768)
- `taps.py` `CLOSED_ENDS`: each closed golden state (x15p9, x16p7, chw0, x17p5) is declared by its two end tokens
  (`part.lo_s` / `part.hi_s`) and held to them wherever it is tapped, whichever path resolved it (PART_MOVED, the T517
  group, remap). A closed state with no declaration is an error. `resolve_all` collects every stale declaration and the
  run lists them together (one `SystemExit` with all), instead of stopping at the first.
- Proof on r-46e4355a7277: taps.json output identical to before; moving x15p9 to part 8 fails naming the state, the
  declared ends, what part 8 holds, and "those ends are store part [7]".
- The five `exclusions.ts` comments (taps.py x2, core `testing/golden.ts` x2, core `regs/tree.ts`) now point at taps.json
  `gone` / `tidal` and the golden parity.
- Test: `pipeline/tests/test_taps_closed_ends.py` (5 tests, no data). Mutation: with the ends check removed the
  "lists every stale declaration" test fails.
