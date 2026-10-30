# T309 — Region salmon rules readable (not walls of text); text in the app can be copied

Status: DONE (cbbd809d4, 69e5ab5 selectable text; phone long-press copy to be checked on the next APK) (owner 2026-10-31: "improve the way the region salmon regs show up right now they are walls of text. Also we
want to make it so we can copy text on the app").

## Work
1. Region-wide salmon (DFO) rules in the regs pane / salmon card: structure them like the game-fish rows — species chips,
   one row per rule (what, when, how many), the DFO wording under a "Full wording" fold, area/subarea names as small tags,
   the "checked <date>" line once. No information dropped; words in core tables.
2. Selectable/copyable text: rule text, glossary definitions, water names, gauge numbers, coordinates, spot notes — long-press
   copy on Android (selectable Text), normal selection on web; a "Copy" action on rule rows. Don't make buttons/links
   selectable.

## Acceptance
Before/after phone shots of a busy salmon region (e.g. Fraser / Region 2) and a copy demo.

Depends: none. Size: M. Owner: no.

## Done (cbbd809d4)
- Region rules: one row per DFO rule (kind chip, species tags, one line, "Full wording" fold, Copy), `salmonCard().regionRules[].rules`; words in SALMON_WORDS; nothing dropped. `RegionRules.tsx`, `copyText.ts` (web clipboard, else the share sheet, as the Updates log).
- Selectable (`selectable`) on the salmon card's rows, notes, sizes, partial line. NOT yet: glossary, water names, gauge numbers, coordinates, spot notes (spots lane is separate) - leftover.
- Shots: ../T309-shots/before-/after-salmon-region-{phone,desktop}.png (+ wording-open). Long-press copy needs a phone check (no APK).
- Owner bug, legend vs the attribution: the Runs map's key sat at the same corner as the attribution (bottom + 10); it now starts above the attribution and scale (CORNER_CLEAR). Trout, Salmon, Conditions, satellite were already clear (insetBottom). Shots before-/after-{runs,...}-{phone,narrow,desktop}.png (satellite shots show the Layers sheet; Esri badge clear in T275-shots).

- Selectable text finished: rule/line text (LineText, so every rule line and its glossary terms), glossary definitions, water names (WaterFrame), the spot Record (gauge numbers, readings), parking coordinates, visit detail, spot notes. Buttons and links are not selectable.
