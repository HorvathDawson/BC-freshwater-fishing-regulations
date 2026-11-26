# contracts.md: the Regulations page's view model and where its fields come from

What the pipeline ships is `pipeline/docs/06-ui-data-contract.md` and `pipeline/deliver/answers/ANSWERS-SPEC.md`; this
page is what the app makes of it. The types are the source files below; this page holds the rules they keep, not a
copy of the types.

| layer | file | who builds it |
|---|---|---|
| store wire (decoded blobs) | `app/packages/core/src/answers.generated.ts` (generated) | Python, `pipeline.deliver.answers.model` |
| inputs (one tap, hydrated) | `app/packages/core/src/regs/inputs.ts` | `@app/data` `loadRegsInputs()` (`packages/data/src/regs/inputs.ts`) |
| view model (what components draw) | `app/packages/core/src/regs/model.ts` | core `buildRegsPage()` (`regs/page.ts`) |

`regs/versions.ts` holds the formats and section versions the app reads (`REGS_FORMATS`, `KNOWN_VERSIONS`,
`SECTION_NAMES`). `@app/core/regs/testing` is a test-only subpath export, never re-exported from the core index.
Components never compute: every finished sentence is in `RegsPage` and pinned by a core test (AGENTS 23-25, 33).

## 1. Rich text: `Line`, and the sheet keys

Every visible sentence is a `Line` (`regs/line.ts`): runs of `text`, `num` (`big` | `big2` | `inline`), `term` (the ⓘ),
`book` (a Rule book page link), `link` (opens a sheet, a day or a tab) and `web` (an outside page a closure cites).

- A presenter writes a sentence's own spaces into its text runs (`"Up to "`, num `"2"`, `" may be …"`); "50 cm" carries
  a no-break space (`nbsp()`).
- **`plain()`, the comparison form:** within one Line the runs concatenate as they are (`num` -> its digits, `term` ->
  "ⓘ" for one id or "What these words mean" for two or more, `book` -> "Rule book p.<printed> ↗" (`bare`: "p.<printed>
  ↗"; no pdf page: unlinked), `link` -> its words); **between two Lines or fields the page draws as separate elements,
  one space** (`plainAll`, `line.ts`); then whitespace collapses and trims (`squash`). Pinned by `line.test.ts`.
- A one-word ⓘ says "What does <title> mean?" to a screen reader, the title from the glossary (`RegsPage.terms`).
- **Sheet keys** (`regs/sheetKey.ts` `SheetKey`) carry NO state id: a `RegsPage` is one state. `parseSheetKey` refuses
  an unknown shape; `appKeyOf` / `goldenKeyOf` are the only bridge to the oracle's keys, which embed the state id.

## 2. `RegsPage`

`regs/model.ts` holds `RegsPage` and everything below it; `buildRegsPage(inputs, {today, who})` builds it; `sheet(key)`
builds a sheet lazily from the same inputs.

- `status` (open | closed | tidal | outside | unknown) comes from the stored display frame, never from the status index.
- The tabs are Keep, Gear, Methods, Licence; `licence` is null outside B.C. (the tab is hidden).

### 2.1 Header

The water's name (`item.name`, or the place's name for an unnamed section), the day word and date, the status dot, the
place label in two candidates (`full`, `short`; the component shows `full` when it fits two lines), and the action (Map
for a named part, Parts when the item has more parts and no map, none for an unnamed section).

### 2.2 Keep tab

`KeepVM.order` is the draw order, from `keep.ts` `keepOrder`: strip, inherited, banner, caveats, the rows (groups and
plain rows), "uncertain", other fish, possession, where-not. **Outside B.C.** the order is `["outside"]` and `outside` is
`words.OUTSIDE_BC` ("This part of the water is outside B.C.: no B.C. rules apply."), drawn from the order like every
block. On a closed day no row is drawn as a group. The year strip's runs are the stored `display_frame.strip` per day of
the viewed year, each day at its own weekday (a common year drops day 60). The trout and char table is its own contract:
`handoff/PLAN/design/trout-table-contract.md`.

### 2.3 to 2.6 Gear, Methods, Licence, Sheets

`GearVM`, `MethodsVM`, `LicenceVM` and `SheetVM` (`model.ts`) are built from the stored `gear` and `licence` frames and
the statics. A closed day's Gear says only when it opens. Every source line opens a `srcs:` sheet whose rows are the
cited rules. A sheet's fold is always closed by default. `sheetLines()` is a sheet's comparison form.

### 2.7 Component-side contracts (ui-native; not in `RegsPage`)

**testID prefixes**, the palette census's selectors (RN has no classes; `ui-native/src/regs/testing/census.ts`): an
element's role is its own or its NEAREST ANCESTOR's prefixed testID, and one without a prefix is role `text`.

- `st:open|closed`: the strip's status word and its pill, the Gear/Methods ✓ / ✕ marks, the closed banner's big lines,
  the not-kept ✕; `st:closed|release`: a not-kept row's words, a plain row's pill, an Other-fish key word;
  `st:keep|release|closed|nolimit`: an Other-fish row's value (its `tone`); `st:zero`: a kind row whose take is 0;
  `st:contested`: the licence's "?" flag.
- `n:big|big2|inline`: every number run of a `Line`; `n:pin`: the pin bar's bold number.
- `chip:size|also|change|source|level`: the size chip, the also-counts chip, the changes chip, the closed chain's
  Source button and level label (the only text a `tint` ground may sit behind).
- `btn:<value>`: each segmented `Choice` option.
- Marks the gate reads: `tier:size` / `pin:size` (the size section's line, numbers in `rule`), `pic:<code>` (a fish
  picture, no ground of its own; `pic:none` the silhouette), `note:paper` (the licence paper note: a keyed `tint`
  exception, `census.ts` ALLOW), group containers `keep-group` / `keep-plain`, the pin bar `pin-bar`.

### 2.8 Hooks

`@app/ui` `regs.ts`: the store, the target and the inputs load as hooks (keyed on the calendar date); `RegsPage` is a
`useMemo` over `buildRegsPage`, so a change of angler rebuilds the page from the same inputs without a reload. Data
loading stays apart from presenting.

## 3. Inputs: `RegsInputs`

`regs/inputs.ts` holds the shape; `@app/data` `loadRegsInputs(store, target, day, weekday, calendarYear)` fills it, and
`targetFor` maps an item and section to the lowest part sharing the section's key.

- The segment is `segmentIndex(starts, dayOfYear(day), moments, weekday, inside=false)`, the port of
  `encode.segment_index` (ANSWERS-SPEC §2). Frames are looked up by `cell(akey, s)`; `displayByDay` holds all 366 days.
- `rules` is a `RuleIndex` over every store rule, viewed through THIS part's `via` / rank; `get()` never misses (it
  returns the `noRule` stand-in), so a view is never shared across parts.
- **Unnamed sections:** `section_akey` block `h >> 12`, a u16le at `2 * (h & 4095)`; `section_place` byte `h & 4095`
  minus 1; the name is `place.names[akey.kind]` (`reads.test.ts`).
- **Blobs:** first byte `0x02` raw inflate with the table's zdict, `0x01` raw inflate, else UTF-8 JSON (`blob.ts`).
- **`Statics.pdfPages`** (printed -> PDF page) is the release's static `export.pdf_pages`
  (`pipeline.common.synopsis_pages`); the app never computes an offset.
- **`FISH_TIPS`** ("What to look for", not in the store) is generated into `regs/fish.generated.ts` by
  `app/tools/emit-fish.mjs` from page v36's tips (`emit-fish.test.ts`).

## 4. Field by field

Each view-model field reads one stored answer: the header from the item and part rows, the strip and banner from the
`display` frame, groups and other fish from the `rows` frame, Gear and Methods from the `gear` frame, the Licence tab
from the `licence` frame and its statics, sheets from `display_rule`, the export words (`rule.j`) and the glossary. The
presenters (`regs/*.ts`) are the map; a field with no stored answer is a pipeline change, never a TypeScript rule.

## 5. Oracle comparison rules

The parity suites (`app/conformance/src/regs/`, `tests.md` §1.2) compare `plain()` of a VM field with the oracle's
string after whitespace collapse and trim. The oracle's text fields and sheet lines carry one space where two elements
abut, never inside a text run; a VM field made of several Lines compares as `lines.map(plain).join(" ")`.

## 6. Resolved conflicts (as built)

1. **Sheet keys carry no state id**; `opts.pid` is a test-harness page id only and never enters a key (§1).
2. Timed caveats are `CaveatVM{kind: "timed"}`; there is no `TimedVM`.
3. The possession sheet key is `pos:pos`.
4. A kind's size chip is `KindVM.size {text, rule}`.
5. A ladder step's line is a `Line`.
6. `head.leadN` is a number; parity compares its string form.
7. The water's subtitle is composed from the item's kind, sections and part label; there is no `item.sub`.
8. Hooks load inputs (`useRegulations`) apart from building the page (`useRegsPage`).
9. Rules are read through `RuleIndex`, never a bare map.
10. A display frame per day is `DisplayFrame | null`; a tidal frame or null gives no strip.
