# T1203 — Larger system text: the screens the T1201 walk could not reach

Status: IN REVIEW — owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1203 | Larger system text: the screens the T1201 walk could not reach | IN REVIEW — owner: see ## Owner | ca93dc14b, 524aaf3f9, a793ef25e, f4c02f0d5, f5b0a8529, 8b44cab79, 10a587159, 97a50ffd6, a9ed2c1e9, 8a46930af, 19eb56632, f0a3a72c4, 5421dd342, d71bf0aa3 | the owner's phone check (one open Owner line) |

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
   full words (`Faces.tsx` `ViewTabs`) and shorten Regulations to "Rules" (the owner's pick, 2026-11-26) (`WATER_VIEW_SHORT`, core `waterViews.ts`) only when
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

## Built (2026-11-26, after the independent review)

The walk is `app/tools/system-text-walk.ts` plus six shard files `app/tools/system-text-sweep.<group>.test.ts` (shell-map, spots-first-run, more, water-regs, water-views, sheets-states), 43 screens, sheets and states at 320, 360 and 412 wide and text 1, 1.3 and 2.0; each shard runs 35 to 165 s under load. They are ordinary `tools/` tests, so CI runs them in the app suite's four path-hash shards (`app-ci.yml`, `vitest`), next to `regs-text-scale.test.ts` and the other Chrome layout tests (the runner has Chrome). The gate: 1x and 1.3x fail on a cut word, text over text, text past an edge or above the window, sideways overflow, a one-word label broken in two, a designed clamp past two lines, a near-empty render; 2.0x fails on LOSS only (those but cut and broken words); the faults a screen may carry are the `ALLOWED` list in the walk (one entry today: the chart's one-line zoom hint). The test header lists every exclusion: More -> Notifications (phone-only), the walkthrough card's place (a measured target), the glossary's virtual-list cell heights, sideways-scrolling rows, covered page words under a sheet, ids that cannot break and hyphenated words, and Yoga's own measuring (the phone check).
A water's Conditions is walked with real gauge rows (a three-gauge panel, a reading, a year of series, the feed's standing): the real estimate card, chart header, legend ramp, Change chip and key.
Fixed: the top tabs (Regulations to Rules when the measured words do not fit; the twin is measured in a box wider than any row and the first paint is decided from an estimate, `water-view-tabs.test.ts`), weekday heads and day numbers, the day picker's buttons, the day sheet's header (its body scrolls at large text), sheet Done and titles (full size; a word wider than the share breaks), the spot sheet's Continue, the rating stars, the obstacle sheet (label column, scrolls), the licence choice (stacks past 1.3), the Layers cards, the horizon chips and kickers, the water map's exit line, the key under the route map, long ids.
THE DEFAULT SIZE IS 0.9.0's: every change to a control grows only when the text size is above 1 (`useCappedScale() > 1`) or is a cap that cannot act at 1; pinned by `SpotDropSheet.scale.test.tsx`, `Sheet.scale.test.tsx`, `DaySheet.scale.test.tsx`, and by a pixel compare of the walk at 1x on 129 pages (3 widths x 43 screens) against the T1201 tip: identical except the search field's rotating placeholder.
Left at 2.0x because they wrap or cut cleanly and lose nothing: the fish key's rows ("Kokanee" on two lines), the Conditions legend title, the "NO PHOTO" thumbnail, a sheet title word broken at 320 ("Notificatio / ns"), the inbox rows' two-line clamp (designed). New-ticket candidates (a redesign): a species-key row that stacks its name over its rule; a legend that stacks its title and count; a title that shrinks to fit instead of breaking.
Owed to the phone check: More -> Notifications; the walkthrough card's place; Yoga's measuring of the view tabs on a phone (the web path is proven by layout in Chrome, the phone path is by construction only); the glossary's rows at large text. The sub-tab row (What you can keep / Gear / Methods / Licence) wraps to a second row as T40 built it; kept.

## Owner

- [x] 2026-11-26 — the switch is wider than the rail buttons at 1.3x → "we just want to make sure bigger text plays nicely in all parts of the app" (2026-11-26): not a defect; check larger text on EVERY screen and fix what breaks.
- [x] 2026-11-26 — the water page's top tabs should shorten when text is large (Regulations to Regs or Rules); the date selector up in the tab bar withdrawn → "regulations could become regs or rules", "scratch that" (2026-11-26): built as Rules (his pick over Regs), measured; sub-tab row left to wrap.
- [x] 2026-11-26 — merge → "Merge as shown (Recommended)" (2026-11-26).
- [x] 2026-11-26 — the short word for the water page's first tab, Regs or Rules → "Rules" (2026-11-26): built as Rules; the full word stays "Regulations" and its accessible name.
- [x] 2026-11-26 — long sheet titles at 2x (a long word breaks; the follow-up to put Done on its own line) → not picked (2026-11-26): the title stays as it is.
- [ ] 2026-11-26 — four checks on your own phone, system font large + display size large: More → Notifications; the walkthrough's cards; the water page's tabs shortening to Rules on a real phone; the glossary's rows.
