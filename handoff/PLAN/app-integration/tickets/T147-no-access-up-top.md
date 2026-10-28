# T147 — "No public access" is a bigger, more obvious part of the regulations at the top

Status: DONE — 1e663952 (core words, page path, block, shots), d5d032d8 (square corners), e5903a77 (review: the page path pinned); colour from df1c8bf1 + 41ace93b (T146; light block is now spruce). Built with T54 Q1 default (permit-only = gentle note).

Source: owner 2026-10-23 (app walk): "make the no public access a bigger part of the regulations at the top and more
obvious". Related: T54 (no-access streams; permit-only areas get the gentle note, Q1–Q3), T146 (its colour).

## Work
On the regulations page of a water (or part) with no public access, the fact leads the page: a clear banner/block at the
top, above the rules, in the T146 colour, saying what it means in plain words (and the permit-only variant per T54's
Q1 default). Never hidden in a caveat line. Web + native, light + dark; reader-gate check of the wording. Size: S.

## As built
- core `access.ts`: `AccessNotice.title` ("No public access", the legend's words; null for the gentle note). New
  wording, one function (`noAccessNotice`): "This section is inside Coquitlam Watershed. The public may not go there, so
  you can't fish here. The regulations below are the Province's; they don't give you access." / partly: "Some of this
  section is inside … so you can't fish that stretch. …". NEW `waterAccessNotice(sections, "part" | "water")`: a page
  opened with NO tapped section (search, a start link — T54's "Left 3") now leads with the banner too: "This water is
  inside …" when every section of the page's part is mostly inside, else "Some of this part of the water is inside …".
  Banner only at that level: no gentle note (a long river crosses private land somewhere; a note on every such page
  says nothing).
- `@app/ui` `pageAccessNotice` + `useAccessNotice(loader, section, store, page)`: tapped section first (T54), else the
  page's part's sids (`navRows` → `part_section`) against paint's no-access rows. `WaterScreen` passes the page.
- `AccessNoticeBlock` (ui-native, web + native): the block LEADS the scroll (now above the lapsed line too): a 6 px bar
  in `waterStatus.no_access`, its 14 % tint as the ground (`palette.noAccessBand`, `NO_ACCESS_BAND`), the legend's line
  swatch beside a large title (TYPE.tier, header role), the body (regsBody, alert role), the link "Show area on map" in
  ink bold underlined (the dark accent is under 4.5:1 on the tint). The gentle note (private land, reserves, permit-only
  land) is unchanged: one small grey line.
- Tests (whole strings): core `access.test.ts` (banner, partly, two areas, permit gentle note with no title, the water /
  part notice, MUTATION PIN: gentler land never puts a banner on a water's page; a mutation of `every` → `some` fails
  "one section outside"), `ui/regs.test.ts` (page path reads only the page's part, names "water" vs "part"),
  `AccessNotice.test.tsx` (block colours, title role, order before the lapsed line, permit note), `theme.test.ts` (the
  band is a visible tint, ink 7:1 on it, every theme).
- Regs conformance / goldens: none changed — no golden or conformance fixture carries an access notice (the T54 render
  test and the ui test strings were updated, as the intended wording change).
- Shots: `T147-shots/` — Coquitlam Lake (one part: "This water is inside") and Seymour River part 0 ("Some of this part
  …"), phone 390 and desktop 1280, light and dark, from the live release r-012fb43aa175 (`T146-shots/regs-shots.mjs`).
  The shots show 6 px rounded corners; d5d032d8 then squared them (RADIUS.box, the regs tokens gate) — otherwise
  identical.

## Checks (both tickets, 2026-10-23)
`REGS_STORE_REQUIRED=1 pnpm -C app check` via heavy.sh: every gate before `test` green; vitest 7301 passed, 7 failed —
none from this change: 5 regs conformance tests refuse the main checkout's `data/generated/bundle/regs.sqlite`
(store f977e8646d7ddcd8; the oracles / taps.json were cut from db98ffb0563359a2 — "re-run
app/design/regs-v11/port/taps.py"), and 2 source-scan tests (`back-layers`, `one-speaker`) hit a transient
`_plat.native.ts` another test writes and removes; both pass alone, and passed in the first full run.

## Reader check of the wording
Not a full reader gate (tests.md §4, 39 questions): a self-check only. The words are short sentences, no jargon ("the
public may not go there", "you can't fish here", "they don't give you access"); the fact comes first as a title. A real
reader round is still owed (see below).

## Ships with
App build only. No pipeline / bundle / release change (paint/3 already carries the no-access rows; `navRows` reads the
regs store's water file).

## Owner questions (default built)
- Q-T147a: the block also leads the CONDITIONS view of a tapped no-access section (T54 Q2 default) with the same look.
  Keep, or a smaller line there? Default: keep.
- Q-T147b: a page opened with no tapped section shows the banner for its part (not the gentle note). Also show the
  gentle note at water level? Default: no.

## Adversarial review (2026-10-23)
Reproduced on this worktree's app (own port 39541, live release): Coquitlam Lake (`?regs=wbk:329291808`) and Seymour River
part 0 (`?regs=gnis:6105&part=0`), phone, light and dark — the block leads, title, body and link as pinned
(`T147-shots/review--*.png`, in T146's review colours: a spruce tint in light, ice cyan in dark).
- **Test gap (fixed, e5903a77): the page path was unpinned where it is wired.** Mutating `WaterScreen` back to T54's
  call (`useAccessNotice(loader, section)` — no store, no page) left the whole ui-native suite green (1125 passed): only
  `@app/ui` `pageAccessNotice` and core's words were pinned, never that the screen hands them the page. NEW in
  `WaterScreen.regs-props.test.tsx`: the fixture release (Chilliwack, part 0, no tapped section) with a paint whose
  no-access rows cover every section → the block's whole string ("This part of the water is inside Test Watershed. …")
  first in the scroll. The mutation fails it.
- Mutations confirmed caught: `waterAccessNotice` `every` → `some` (core + ui tests), `pageAccessNotice` reading every
  part's sections (ui test).
- Colour: the block's bar, swatch and tint read `waterStatus.no_access` / `noAccessBand`, so T146's review change (light
  spruce) reaches it with no code change; `theme.test.ts` still holds ink 7:1 on the tint in every theme.
- Seen, not changed (owner question Q-T147c): a page opened by TAPPING a section that is not itself in no-access land
  shows no block, even when the page's part runs through it — tapping the Coquitlam River at Port Coquitlam opens part 3
  ("Lougheed Highway Bridge → a lake outlet", through the Coquitlam Watershed) with "✓ Open" first and no mention of
  access (`T146-shots/review--defect--regs-coquitlam-river-tapped-light.png`). T54's rule is "the tapped section first".

## Owner questions (review)
- **Q-T147c**: a tapped section outside no-access land, on a part that runs through it: show the part's banner too
  ("Some of this part of the water is inside …")? Default built: no (T54: the tapped section speaks).

## Left / follow-ups
- A real reader-gate round on the new wording (tests.md §4).
- Native device shot (the component is shared; not run on a device here).
- "Show area on map" still opens the water's Map view without framing the area polygon (T54 Left 2).

## Owner answer (2026-10-28, Q140 = Q-T147c)
"yes add a banner but just keep formatting consistent. unless a section is full out of the no access area it should have a
banner". Read: every section of a part that runs through no-access land leads with a banner, in the one existing block.
A tapped section with any no-access land keeps its own banner ("This section is inside …" / "Some of this section is
inside …"); a tapped section wholly outside, on such a part, leads with the PART's banner ("Some of this part of the water
is inside …"). One block per page; the part's banner outranks the section's gentle note. Built: core
`leadingAccessNotice` (the one choice), `@app/ui` `useAccessNotice` now asks the page's part beside a tapped section.
Tests: core `access.test.ts` (Q140 block), `WaterScreen.regs-props.test.tsx` "(Q140) …" (mutation: asking the part only
without a tapped section fails it). The Conditions view (Q139) is unchanged: it still speaks for the tapped section.
