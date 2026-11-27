# T1203 — Larger system text: the screens the T1201 walk could not reach

Status: IN PROGRESS — 0.10.0 re-cut (owner 2026-11-26: every change from this batch)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1203 | Larger system text: the screens the T1201 walk could not reach | IN PROGRESS — 0.10.0 re-cut | see commits | the phone check (system font large + display size large); owner's pick Regs / Rules; the redesign-sized items under Built |

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

## Built (2026-11-26)

`app/tools/system-text-sweep.test.ts` now walks 43 screens, sheets and states at 320, 360 and 412 wide: the Map tab in place (search, day bar, rail, the Trout / Salmon switch, legend), the day picker, Layers, search open and with results, Conditions, Runs and its sheet, Spots (empty, with a spot), a spot page, adding a spot, More and every page under it (Offline maps was blank only because the harness gave the Shell no release loader), What's new, Report, a water's Regulations with Gear, Methods and Licence, a fish sheet, a sources sheet, both day pickers, the water's Conditions and Map views, the bell's inbox (rows and a notice), the join card, the developer card, the app-update popup, the sync-error, not-fishing, obstacle and spot-drop sheets, the error and empty states, the first-run notice and the walkthrough. Gate: a fault counts only when a larger size caused it (it is absent at 1.0), at 1.3x; a near-empty render fails. Passes (1.3x clean at all three widths).
Fixed: weekday heads, the day picker's buttons, sheet titles (cap 1.3) and Done, the spot sheet's Continue, the rating stars, the obstacle sheet's label column, long ids (tile version, release) wrap instead of running off, the water page's top tabs (Regulations to Regs, measured, `water-view-tabs.test.ts`; picture beside "Rules" for his pick).
Left at 2.0x because they wrap or cut cleanly and lose nothing: the fish key's rows ("Kokanee" on two lines), the Layers "Regulations" option, the Conditions legend title and "Change" chip, the "NO PHOTO" thumbnail, the water map's "Flows into" line, the inbox rows' two-line clamp (designed). New-ticket candidates (a redesign, not a fix): a species-key row that stacks its name over its rule at large text; a Layers option grid that reflows to one column; a legend that stacks its title and count.
Not walkable here: More → Notifications (phone only) and the walkthrough card's placement (placed from a measured target) are owed to the phone check. The sub-tab row (What you can keep / Gear / Methods / Licence) wraps to a second row as T40 built it; kept.

## Owner

- [x] 2026-11-26 — the switch is wider than the rail buttons at 1.3x → "we just want to make sure bigger text plays nicely in all parts of the app" (2026-11-26): not a defect; check larger text on EVERY screen and fix what breaks.
- [ ] 2026-11-26 — the phone check: system font large + display size large on a real Android phone (what to look at is in the report).
- [x] 2026-11-26 — the water page's top tabs should shorten when text is large (Regulations to Regs or Rules); the date selector up in the tab bar withdrawn → "regulations could become regs or rules", "scratch that" (2026-11-26): built as Regs, measured; sub-tab row left to wrap.
