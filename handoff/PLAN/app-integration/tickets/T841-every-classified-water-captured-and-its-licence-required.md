# T841 — Every classified water the book marks (by its symbol, or as "tributaries of …") is captured, bound and requires the Classified Waters Licence; a badge in the card header

Status: OPEN (0.8.0; data audit first, then the app's badge).

Source: owner 2026-11-17 ~20:27, verbatim: "Also we need to make sure the classified water symbols are maintained properly and modify the license requirement. Also if a water is classified it should have a badge in the card I header I think. Make sure we capture all classified waters whether labeled by symbol or by tributary of something"

## Data (main chat)
1. List every classified water in the 2025-27 synopsis: each water printed with the classified symbol (Class I / Class
   II), each "tributaries of X" classification, each period (e.g. Skeena sections, Elk + tributaries, Dean). Cite printed
   pages.
2. Compare with what ships: each one bound to the right water and stretch (tributaries included where the book says
   so), with its class and period, and the Licence tab's Classified Waters Licence requirement on it, only there.
3. Every gap and every extra listed; fix the pipeline so the symbol and the tributary rule are both read; a check that
   fails the build if a book-classified water ships without its requirement.

## App (app chat, after T802's look)
A Class I / Class II badge in the card header of a classified water (owner's direction; T802 was "looks first"). One
picture to the owner before it ships.

## App half — built 2026-11-17 (app chat, branch app/t841-class-badge), owner picks 21:38
`core/src/regs/classBadge.ts` `classBadgeOf` → `HeaderVM.classed`; drawn by `ClassBadge` in `ui-native/src/regs/RegsHeader.tsx`
(name row, before the date pill; outlined chip, tokens only; press opens `gl:classified_waters`). The period comes from the
record's stored `period.dates` (existing `rangeTxt`); every day's frame lists the part's designations in
`holds.considered`, so no union over days. On r-46e4355a7277: Skeena S4 "Class II · Jul 1–Dec 31", Skeena River 2 Jul 1–Sep 30,
Dean Class I Jun 1–Sep 30 (canyon parts) and Class II Jun 16–Oct 31 (Iltasyuko–Anahim), the Elk and its tributaries "Class
II" (no dates: whenever open), lower Coal Creek none. Two classes on one part: the highest shows (none in the release).
Owner 2026-11-17 21:38 (app chat dialog, pictures of A and B sent): "B: always, with dates (Recommended)" and "In the name
row (Recommended)". Possible data gap sent to the main chat: upper Coal Creek (gnis:17550 part 1, above the Old MF&M
Railway bridge) carries no designation.
