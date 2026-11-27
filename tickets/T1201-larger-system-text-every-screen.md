# T1201 — Larger system text: every screen holds up ("SALMON" wrapped in the mode switch, "CONDITIONS" was cut in the tab bar)

Status: OPEN — 0.10.0 re-cut (owner 2026-11-26)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1201 | Larger system text: every screen holds up ("SALMON" wrapped in the mode switch, "CONDITIONS" was cut in the tab bar) | OPEN — 0.10.0 re-cut | — | 1: fix the two labels (render tests at scale 1.0, 1.15, 1.3). 2: sweep every screen at larger text on web, fix what breaks; pictures to the owner before merge |

Source: seen by the tester feedback chat, 2026-11-26, in a closed tester's pictures of the main map (app 0.9.0, a large
Android phone; the tester did not mention either). Feedback log D21.

1. The Trout / Salmon switch reads "SALMO" / "N" on two lines. `RegsLayerToggle`
   (`app/packages/ui-native/src/shell/RegsLayerToggle.tsx`): each cell is 44 wide, the label is 9 px, weight 800 when
   on, letter spacing 0.4, uppercase, with no `numberOfLines`, no `adjustsFontSizeToFit` and no
   `maxFontSizeMultiplier`. "SALMON" is about 40 px in a 42 px box at scale 1.0, so any system font scale above 1.0
   wraps it.
2. The tab bar reads "CONDITIO…". `app/packages/ui-native/src/TabBar.tsx`: `TYPE.tab` is 9.5 px bold with letter
   spacing 1.1, one line, and may scale to `TAB_LABEL_SCALE = 1.3`; at 1.3 the word is wider than its cell on a phone
   whose display size is set large.

Owner, 2026-11-26, on seeing the two: "wwe need to make the bigger text play niocer too. that is what caused the salmon
wrap over issue". So the ticket is the whole app at a larger system text size, the two labels first.

## Work

3. **The sweep.** On the web test bed at phone width (360 and 412), with the text scale forced to 1.15, 1.3, 1.5 and 2.0:
   every screen and sheet (map with its rail, legend, search and day bar; a water's Regulations, Conditions and Map
   tabs; fish and source sheets; Runs; Spots and a spot page; More and its pages; the walkthrough). One table of what
   wraps badly, clips, overlaps or pushes a control off screen, worst first, each with a picture.
4. **One rule, written down** (in the type file's header): body and reading text follows the system size in full;
   labels inside fixed-size controls (rail cells, tab bar, chips, badges, chart ticks) are capped at what their box
   holds and never wrap or cut; a control grows before its label shrinks. Today the caps are ad hoc
   (`TAB_LABEL_SCALE = 1.3`, `CAPPED_SCALE = 2` in `type.ts`, none on the rail). Apply the rule through the shared
   `Text` / type tokens, not label by label.
5. Fix what the sweep found, worst first; anything large becomes its own ticket (this chat's range).

Lines 1 and 2 (the two labels):

Both labels fit on one line, whole, at system font scales 1.0, 1.15 and 1.3 and at 360 and 412 wide. Smallest correct
change first: one line and a capped scale on the switch label (or a wider cell); on the tab label a lower cap, tighter
letter spacing, or shrink-to-fit with a floor. Keep the labels readable for a user who set a large font: do not cap
below what the cell can hold. Check the other fixed-width labels of the map rail and the tab bar for the same fault and
fix them here.

## Acceptance

- A render test per label at 360 wide and font scale 1.3 that fails without the fix (one line, no ellipsis).
- Web pictures of the map rail and the tab bar at scale 1.0 and 1.3, before and after, to the owner before merge.
- The sweep table exists with its pictures; no screen in it clips, overlaps or loses a control at scale 1.3, and at
  2.0 every screen is still usable (wrapping is fine, loss is not). A test walks the screens at 1.3 and fails on a
  cut or two-line fixed label.

Depends: none. Size: S (the two labels) + M (the sweep). Model: Sonnet. Owner: a look at the pictures before merge.
