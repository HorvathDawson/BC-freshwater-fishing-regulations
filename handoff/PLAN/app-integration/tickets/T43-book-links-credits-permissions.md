# T43 — Rule book links on native, fish credits, the image-permissions ship gate
Phase 4 · Depends on T20 (fish manifest → `fish.generated.ts` + image map), T25/T34 (`RuleBookLink`), T36/T37 (sheet
`book`/`pages` blocks, `sp:` image credits), T38 (adipose drawings in the manifest) · Blocks release · Decisions **D6**
(a: open the PDF URL plainly on native), **D15** (a: stand-ins ship captioned, behind the permission gate) · Size ½ day

## Goal
1. **One** book-link behaviour everywhere ("Rule book p.N ↗" on cards, tiers, gear lines, sources, sheets; bare
   "p.N ↗" after a "Rule book" label in glossary sheets): on web it opens the synopsis PDF at `#page=<pdf>`; on native it
   opens the PDF URL with `Linking.openURL` and **no fragment** (D6 a) — the label already names the printed page, so the
   reader is told where to look. A page the map lacks (pdf null) is plain text "p.N", not a link.
2. **Credits**: every fish picture the app can show is credited — on its `sp:` sheet (T36 credit line, already) and in one
   "Fish pictures" list on the app's credits surface, using the existing `Credits` component.
3. **Ship gate**: no image ships unless `app/apps/mobile/assets/fish/fish_credits.tsv` has a row for its file with a
   cleared permission status. `manifest.json`'s `_about` says permission "is being obtained (user 2026-10-10); test use" —
   the gate is how "test use" cannot reach a store build.

## Files
- create `app/packages/ui-native/src/regs/openBook.ts` — the one implementation (replaces T25's `.native/.web` pair idea and
  T34's inline `Platform.OS` — whichever landed, refactor both callers to this); `Platform.OS` branch, no file pair needed
- modify `app/packages/ui-native/src/regs/RuleBookLink.tsx` (T34): props gain `bare?: boolean`, `pdf: number | null`
- create `app/packages/ui-native/src/regs/RuleBookLink.test.tsx`
- create `app/apps/mobile/assets/fish/fish_credits.tsv` — one row per manifest image (39 + 2 adipose drawings = 41)
- create `app/tools/check-fish-credits.mjs` + `app/tools/check-fish-credits.test.ts`; add `"fish-credits": "node tools/check-fish-credits.mjs"`
  to `app/package.json` scripts and to `check` (after `deps`) in **report mode**; `--ship` mode fails on any non-cleared row
- modify `app/tools/emit-fish.mjs` (T20): `FISH_CREDITS: readonly string[]` emitted into `fish.generated.ts` — one line per
  image `"<caption>[ · <phase>] — <credit> (<source>)"`, sorted by caption, de-duplicated
- modify the credits surface: `app/packages/ui-native/src/regs/RegsScreen.tsx` foot (after Licence) renders
  `<Credits palette lines={FISH_CREDITS} label="Fish pictures" />` — only the lines for pictures this page shows
  (`creditsFor(page)`), so the foot never lists 41 lines under a water with two fish
- create `app/packages/core/src/regs/credits.ts` — `creditsFor(page: RegsPage): readonly string[]` (+ test)

## Signatures
```ts
// openBook.ts
export const SYNOPSIS_PDF = "https://www2.gov.bc.ca/assets/gov/sports-recreation-arts-and-culture/outdoor-recreation/fishing-and-hunting/freshwater-fishing/fishing_synopsis.pdf";
//   (regs-v11.html @04765ab5 L928; also core words.ts SYNOPSIS_PDF if T00 put it there — import, do not duplicate)
export function bookUrl(pdf: number, os: typeof Platform.OS = Platform.OS): string;   // web: `${SYNOPSIS_PDF}#page=${pdf}`; ios/android: SYNOPSIS_PDF
export function openBook(pdf: number): Promise<void>;                                  // Linking.openURL(bookUrl(pdf)); swallow + log a rejected open (no Alert — banned in shared code)

// RuleBookLink.tsx
export function RuleBookLink(p: { printed: number; pdf: number | null; bare?: boolean; palette: Palette }): JSX.Element;
//   label: bare ? `p.${printed} ↗` : `Rule book p.${printed} ↗`   (words.ts BOOK / BOOK_BARE)
//   pdf === null → <Text> `p.${printed}` (no arrow, no role) — prototype pageLink 935's <span>
//   accessibilityRole="link", accessibilityLabel `Rule book page ${printed}, opens the PDF`, hitSlop per T40

// credits.ts
export function creditsFor(page: RegsPage, fish: typeof FISH, lines: readonly string[]): readonly string[];   // codes reachable from page.keep (pictures, otherFish thumbs) ∪ sheet sp: keys
```
`fish_credits.tsv` (tab-separated, header row, UTF-8, sorted by file):
```
file	code	caption	credit	source	licence	status	evidence	checked
brook-trout.webp	EB	Brook trout	Freshwater Fisheries Society of BC	GoFishBC	permission	pending	email 2026-10-10 (user)	2026-10-20
adipose-wild.webp		Wild · adipose fin there	drawn for this app (FISHART, page v36)	this app	own	cleared	—	2026-10-20
```
`status` ∈ `pending | cleared | own | public-domain | refused`. Cleared for ship = `cleared | own | public-domain`
(Commons rows are `public-domain` only after the file page's licence is recorded in `evidence`; a CC-BY file is
`cleared` with the attribution in `credit`).

## Ports
- regs-v11.html @04765ab5: `bookLinks` 930 (`Rule book p.N ↗`, `href=SYNOPSIS_PDF#page=pdf`, `target=_blank`), `pageLink`
  935 (bare `p.N ↗`; no pdf → `<span>p.N</span>`), `booksOnce` (one link per distinct printed page), the `sp:` sheet credit
  line `"<caption>[ · <phase>] · <credit> (<source>)"` (sheetHtml sp branch).
- `check.mjs` `rule-book-label` (L1972), `sheet-book-words` (`#page=` = `SNAP.pdfPages[p]`).
- manifest `app/apps/mobile/assets/fish/manifest.json` `{file, code, caption, phase, stand_in_for, credit, type, source, width, height}`
  — 39 images; sources: eRegulations WA 13, Commons 11, GoFishBC 8, fws.gov 4, whattofish.ca 2, DFO sturgeon ID guide 1.

## Reads
`Run {t:"book", printed, pdf, bare}` (T36 contract change), `SheetBlock image.credit`, `FISH` / `FISH_CREDITS` (T20),
the manifest, `fish_credits.tsv`.

## Tests
**C — `RuleBookLink.test.tsx`**:
1. `label` — `printed 21, pdf 23` → link text `"Rule book p.21 ↗"`; `bare` → `"p.21 ↗"`; `pdf null` → text `"p.21"` with no `link` role.
2. `web opens the page` — `Platform.OS = "web"`: press → `Linking.openURL` called with `SYNOPSIS_PDF + "#page=23"` (golden
   `sheets["gl:bait_ban"].pages` = `[{text:"p.4 ↗", pdf:6}, {text:"p.8 ↗", pdf:10}]` → `#page=6`, `#page=10`).
3. `native opens the file` — `Platform.OS = "ios"` and `"android"`: `openURL(SYNOPSIS_PDF)` exactly, no `#`.
4. `every book run in every golden sheet` (store present): for vedder_1010's 63 sheets, each rendered link text matches
   `/^(Rule book )?p\.\d+ ↗$/` and its pdf equals the golden `pages[].pdf` in order (`rule-book-label`, `sheet-book-words`).
**A — `check-fish-credits.test.ts`**: on a temp manifest + tsv: (a) a manifest file with no tsv row → error `"no credits row: x.webp"`;
(b) `--ship` with a `pending` row → exit 1 listing the file; report mode → exit 0 with a `"⚠ 1 pending"` line; (c) the
real files: every manifest image has exactly one row, every row's file exists, `code`/`caption`/`credit`/`source` equal
the manifest's (the tsv never contradicts the manifest).
**A — `credits.test.ts`**: vedder_1010 page → `creditsFor` lists the Rainbow trout line
`"Rainbow trout — Freshwater Fisheries Society of BC (GoFishBC)"` and the two Steelhead lines (ocean · GoFishBC, spawning ·
WDFW), and nothing for a fish the page does not show; sorted, unique.
**C — `Credits.test.tsx`** (existing) unchanged — `Credits` renders every line verbatim; the regs foot test asserts the
"FISH PICTURES" label and the vedder lines.

## Acceptance
```sh
pnpm -C app vitest run packages/ui-native/src/regs/RuleBookLink tools/check-fish-credits packages/core/src/regs/credits
node app/tools/check-fish-credits.mjs           # report: N cleared, M pending (expected today: pending = every third-party row)
node app/tools/check-fish-credits.mjs --ship    # MUST pass before a store build — run by the release checklist, not by pnpm check
pnpm -C app check
```

## Notes
- D6 (a) loses the page jump on native; the visible "p.21" is the instruction. If D6 → (b), only `openBook` changes (an
  in-app viewer honouring `pdf`) + a `deps.md` line; every caller already passes `pdf`.
- **The synopsis PDF is not byte-identical** to the repo copy (memory: cite PRINTED pages; PDF−2 to p.40, PDF−6 after) —
  never compute `pdf` from `printed`; the data carries both (`RuleFacts.book` pairs; glossary via `Statics.pdfPages`, T36).
- D15 (a): stand-ins (`stand_in_for` non-null) keep their caption as the manifest says (e.g. a coastal cutthroat standing
  in) — the credit line names the pictured fish, not the stood-for one.
- `check-fish-credits` lives in `pnpm check` in report mode so a new manifest image without a tsv row fails CI at once
  (missing row = error in both modes); only `pending` is a ship-time failure.
- Owner question (no D id): who records permissions (the evidence column) — the user said on 2026-10-10 that permission is
  being obtained; the tsv starts with every third-party row `pending`.
