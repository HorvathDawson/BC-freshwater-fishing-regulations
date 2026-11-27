# T1203 — Larger system text: the screens the T1201 walk could not reach

Status: IN PROGRESS — 0.10.0 re-cut (owner 2026-11-26: every change from this batch)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1203 | Larger system text: the screens the T1201 walk could not reach | IN PROGRESS — 0.10.0 re-cut | — | walk the Map tab's legend, day bar and switch in place, Offline maps, Spots and a spot page, fish and source sheets, the walkthrough at 1.15–2.0; fix what breaks; one check on a real phone |

Source: T1201's sweep (2026-11-26, `app/tools/system-text-sweep.test.ts`): ten screens at 360 and 412 wide and text
scales 1 to 2.0 came out clean after its fixes, but these were not reached by the harness (the map is blanked there,
one page rendered blank, the rest were not walked):

1. The Map tab in place: the legend strip, the day bar, the search bar, and the Trout / Salmon switch on the rail (its
   label is pinned only by `system-text-labels.test.ts`, outside the shell).
2. More → Offline maps (rendered blank in the walk: find out why first).
3. Spots with saved spots, and a spot page.
4. A fish's sheet and a Sources sheet.
5. The walkthrough.
6. Runs and Conditions with real data (walked only in their empty or fixture state).
7. At 360 wide the five tabs are no longer equal in width when a label needs more room (T1201's fix, owner saw the
   picture 2026-11-26 and said it "looks good"): nothing to do unless a sixth tab or a longer word arrives.
8. A water page's top tabs (Regulations / Conditions / Map) at a larger text size (owner, 2026-11-26: "another thing i notice
   here is the regulations, conditions, map top tabs should be dynamic if they get too big with text increase then
   regulations could become regs or rules.. also then the data selector could go up into tab bar maybe? i guess the back
   button is up there tho which will take up extra space so scratch that...."). Built: the tabs measure their row and the
   full words (`Faces.tsx` `ViewTabs`) and shorten Regulations to "Regs" (`WATER_VIEW_SHORT`, core `waterViews.ts`) only when
   the full words do not fit; the full word stays the accessible name; the labels are one line, capped at 1.3. The date
   selector stays where it is (withdrawn). The sub-tab row (What you can keep / Gear / Methods / Licence) already wraps to a
   second row (T40), which loses nothing; proposal: keep that.

## Work

Extend the walk in `system-text-sweep.test.ts` to these screens (same widths and scales), fix what clips, overlaps,
breaks mid-word or loses a control, by T1201's rule in `type.ts` (reading text follows the system size; a label in a
fixed-size control is capped at what its box holds; a control grows before its label shrinks). Then one check on a
real Android phone with the system font set large and the display size set large: T1201 was laid out in headless
Chrome with the text size emulated, and `labelMinWidth` (0.72 em a character) is an estimate checked with one face.

## Acceptance

Every screen above is in the walk and clean at 1.3; usable at 2.0. Pictures of anything changed go to the owner before
merge. The phone check is recorded here (who, what build, what was seen).

Depends: T1201 merged. Size: M. Model: Sonnet. Owner: the phone check, or a look at its pictures.

## Owner

- [x] 2026-11-26 — the switch is wider than the rail buttons at 1.3x → "we just want to make sure bigger text plays nicely in all parts of the app" (2026-11-26): not a defect; check larger text on EVERY screen and fix what breaks.
- [ ] 2026-11-26 — the phone check: system font large + display size large on a real Android phone (what to look at is in the report).
- [x] 2026-11-26 — the water page's top tabs should shorten when text is large (Regulations to Regs or Rules); the date selector up in the tab bar withdrawn → "regulations could become regs or rules", "scratch that" (2026-11-26): built as Regs, measured; sub-tab row left to wrap.
