# T698 — Remove the Parts sheet (start-link only since T150)

Status: DONE — caa3a0133 (RULINGS P56; ships with the next app build)

Owner 2026-11-12, dialog option verbatim: "Remove the screen". The sheet had no button since T150 (2026-10-23); it opened
only with a start link `&sheet=parts`, and its second line said "Closed all year · N stretches" on 45 waters (T697).

## Done
- core: no `"parts"` sheet key (`sheetKey.ts`; a golden `parts` key maps to nothing, as `licsrc:` does), no `partsSheet`
  (`sheets/parts.ts` deleted), no `choice` block, no `PART_OPEN/CLOSED/UNKNOWN` words, no `SheetCtx.names`, no
  `RegsInputs.choicesOnDay` / `ChoiceDay` (the data layer no longer reads one display frame per picker choice per page).
  `PARTS_TITLE` stays: the water map's list reads it.
- ui-native (native and web, one component tree): `PartsChoice.tsx` deleted; `onPart` gone from `SheetBody`, `SheetHost`,
  `RegsScreen`, `WaterScreen` and `WaterModeHost`'s WaterScreen (the map's own `onPart` stays).
- A start link `sheet=parts` is refused and said: "start link: sheet=parts is not a sheet key" (`startLink.test.ts`).
- Frozen VM fixtures `bc1_501`, `edge_x15p2_1010` lose their `parts` sheet; tools `regs-web/states.mjs` mirrors core.
- Guards: `tools/one-parts-list.test.ts` (no key, builder, row component or `choice` block anywhere) and the start-link
  test both fail with the old key. knip clean.
