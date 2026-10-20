# T19 — presenter: not kept, How it adds up, quiet line, other-fish list

Phase 1 · Depends on T00, T06, T08 (`CardModel.rows`, `line`), T12, T17 (`rowTitle`), T18 (`pictures`), T20 · Blocks T21, T26, T27 · Decisions — · Size ¾ day

Target **regs v33 @ 04765ab5**: prototype buildKeep `nk` 1191, `els`/`worked` 1199–1201, `quiet`/`more` 1203, Other fish
1305–1323 (incl. the v33 size hint and `owk`), `thumb` 959–962; page v36 `rowsHtml` 692–700 (`TROUTISH`
691), `noRulesLine`/`STEEL_LINE` 984–990, `cardBlock` 946–974 (where `.gquiet` and `.linkrow` come from), `summaryHtml` 1113–1144.

## Goal
The group's tail and the Other fish list: each not-kept line ("Rainbow trout and cutthroat trout: release every one" · "This
river’s rule · Rule book p.22 ↗", ✕ for a closure, pictures), "How it adds up" (the data's elsewhere sentence, then each worked
line — a keep-here question with its colon, a kept-elsewhere question with its answer), the steelhead quiet line, "See every
regulation line ›", and the compact Other fish rows ("Whitefish · 15 a day ›", "Kokanee" with "Weekends: keep 5 · Weekdays: release" under it).

## Files
- create `app/conformance/src/regs/tail.parity.test.ts`, `app/conformance/src/regs/others.parity.test.ts` — the golden parity (conformance may import `@app/data/regs/node` and read golden.json; core may not import `@app/data` or `node:fs`). The core `*.test.ts` files named below hold **unit tests over small hand-built inputs only**.
- create `app/packages/core/src/regs/tail.ts`, `tail.test.ts`, `app/packages/core/src/regs/others.ts`, `others.test.ts`
- modify `words.ts`, `model.ts`

## Signatures
```ts
// tail.ts — prototype nk, worked, quiet, more; page v36 noRulesLine/STEEL_LINE
export function notKeptOf(g: GroupTotal, env: TreeEnv): readonly NotKeptVM[];
export function workedOf(row: Row, g: GroupTotal): WorkedVM | null;
export function quietOf(row: CardRow, card: CardModel): Line | null;
export const MORE: string;                                      // "See every regulation line ›" (words.ts)
// others.ts — page v36 rowsHtml split, prototype Other fish rewrite + thumb
export const TROUTISH: readonly string[];                       // ["RB","CT","EB","GB","LT","DV","ST","BT","CT_ws","CT_c"]
export function splitRows(card: CardModel): { main: readonly number[]; rest: readonly number[] | null };   // rest null: no Other fish section
export function otherFishOf(card: CardModel, inp: RegsInputs, fish: FishManifest): readonly OtherFishRowVM[] | null;
```
Model (supersedes contracts.md §2.2):
```ts
export interface NotKeptVM { status: "closed" | "release"; says: string; whose: Line; mark: "✕" | null; presence: Line | null; pictures: readonly FishPicVM[] }
export interface WorkedVM { label: string; says: string | null; lines: readonly { at: "here" | "elsewhere"; q: string; a: string }[] }  // q carries the ":" for at=here
export interface OtherFishRowVM { thumb: string | null; name: string; week: string | null; value: string; sizeHint: string | null; open: SheetKey }
// GroupVM.quiet: Line | null ; GroupVM.more: { text: string; open: SheetKey }   ("src:r<i>")
```
Words: `HOW_IT_ADDS_UP = "How it adds up"`, `MORE = "See every regulation line ›"`, `OTHER_FISH = "Other fish"`, `A_DAY = (n) => n + " a day"`,
`NO_LIMIT = "no limit"`, `CLOSED_LC = "closed"`, `RELEASE_LC = "release"`, `SIZE_HINT = "· size limit"`, `COLON = ":"`, `DOT_SEP = " · "`,
`STEEL_LINE = { possible_with_rules: "Steelhead rules apply here; steelhead may not be present in this water.", known_with_rules:
"Steelhead are known to be in this water.", known_no_rules: "Steelhead have been recorded here, but no steelhead rule applies: treat any rainbow, however big, as a rainbow trout." }`.

## Ports
- **not kept** (1191): per `g.not_kept[x]`: says = x.says; whose = `[text cap1(x.whose)]` + (rule of `x.by` → `[" · ", ...bookRuns]`);
  mark "✕" iff closed; presence = `x.presence` → `[says, " ", term steelhead]` (NOT presOf); pictures(x.species) (T18).
- **worked** (1199–1201): drawn when `row.elsewhere?.says || g.worked.length`: label HOW_IT_ADDS_UP, says = elsewhere.says || null,
  lines = `g.worked.map(w => ({at: w.at === "here" ? "here" : "elsewhere", q: w.q + (w.at === "here" ? ":" : ""), a: w.a}))`
  (data `at` stream|lake|elsewhere all read as "elsewhere").
- **quiet** (page v36 `noRulesLine` 990): `card.line === "known_no_rules" && row.allMembers.includes("RB")` → `[STEEL_LINE.known_no_rules, " ", term steelhead]`.
- **more**: page v36 `summaryHtml` 1123 always ends a carded row with "See every regulation line ›" (`data-src` → sheet `src:r<i>`).
- **split** (page v36 `rowsHtml` 692; rule proved by `app/design/regs-v11/port/verify_map.py` on 134/134): on a closed day
  (`display.status === "closed"`) NO rows are drawn → groups `[]`, otherFish null. Else `main` = rows with any member (all_members
  when present) in TROUTISH; `rest` = the others; if `!main.length || rest.length < 2` every row is drawn in order and there is no
  Other fish list. Groups (T21) = drawn main rows with `group.headline`; the others are plain rows.
- **other fish** (prototype 1305–1323): per rest row i: `name = rowTitle(row)`; `value` = page v36 `short`: keep → `A_DAY(daily)`,
  nolimit → "no limit", closed → "closed", else "release"; `week = row.week?.status ?? null`; `thumb` (prototype `thumb` 959): the
  first member with a manifest image, else null (silhouette); `sizeHint` (v33, inside 1305–1323): when the row's `group` JSON matches
  `/"size_rule":"[^"]|"detail":"[^"]*\d+ cm/` over `group.tree || group.kinds` AND the value is a keep count → SIZE_HINT; `open = "other:o" + i`.

## Reads
`GroupTotal.{not_kept[], worked[]}`, `Row.{elsewhere, week.status, kind, daily, members, all_members, group}`, `RowsValue.steelhead_line`,
`inp.frames.display.status`, `inp.rules.get(by).facts.book`, fish manifest.

## Tests
Golden parity lives in `app/conformance/src/regs/tail.parity.test.ts`, `app/conformance/src/regs/others.parity.test.ts`: `for await (const { id, g, page } of cases("keep"))` with `requestOf` and `eqPlain` from
`app/conformance/src/regs/{golden,eq}.ts` (tests.md §1.2); the presenter under test is called on the case's inputs (until T21 lands the
harness hands the hydrated `RegsInputs` — T12 exposes `inputs()` on each case — after T21 the same assertions read `page().keep`).
Every numbered case that names a golden state id is a parity case there; cases marked **Unit** (and property tests over hand-built
`GroupTotal`/`StatusFrame` fixtures) stay in the core `*.test.ts`, which never loads golden.json.
`tail.parity.test.ts` (conformance; parity over the 97 golden groups): `notKept[]` (`status, says, plain(whose) = whose, mark, plain(presence), pics, pages`),
`worked` (`says`, `lines[]{at,q,a}`), `quiet` = plain, `more`.
1. vedder_515 notKept → `[{status:"release", says:"Rainbow trout and cutthroat trout: release every one", whose:"This river’s rule · Rule book p.22 ↗", mark:null, pics:[RB "Rainbow trout ›", CT "Coastal cutthroat trout ›"]}]`.
2. ape_501 notKept → closed "Steelhead: no fishing for them, not even catch and release — release any you hook by accident", whose
   "The Bella Coola River’s rule · Rule book p.43 ↗", mark "✕", presence "A rainbow trout over 50 cm (20 in) here counts as a steelhead. ⓘ";
   release "Char (Dolly Varden/bull trout, lake trout and brook trout): release every one", whose "The Bella Coola River’s rule for its tributaries · Rule book p.43 ↗".
3. vedder_1010 worked → says "Your own limit, counted across every Region 2 stream you fish today."; lines
   `{here, "If you keep 4 hatchery rainbow trout:", "that’s your trout and char for the day, everywhere."}`,
   `{here, "If you keep 2 hatchery rainbow trout:", "none from other streams today — up to 2 more of any kind, from here or from lakes."}`,
   `{elsewhere, "Kept 1 at a lake?", "Keep 3 more here. Fish from lakes count toward the 4 a day, not toward the 2 from streams."}`, + the over-50 cm line.
4. klwa_1010 worked → says "This lake’s rule: 2 a day here. Trout and char other than steelhead you kept anywhere else today count toward those 2.", lines [].
5. x15p0_1010 quiet → "Steelhead have been recorded here, but no steelhead rule applies: treat any rainbow, however big, as a rainbow trout. ⓘ".
`others.parity.test.ts` (conformance; parity over 134 states: golden `otherFish` null ↔ null; per row `thumb`, golden `name` = `name + (week ?? "")`
(textContent of `.on`, which holds the `owk`), `week`, `open` via T12 `sheetKeyOf`, golden `lines[0]` = `eqPlain` of `[name + (week ?? ""), value, sizeHint]` — `name`+`week` stay fused because the week is inside the `.on` element, the value and hint are separate elements):
6. vedder_1010 → `[KO Kokanee release, LW Whitefish "15 a day", LMB Bass "20 a day", BCB Black crappie "20 a day", WSG White sturgeon release, null Crayfish "25 a day", null Protected species closed]`.
7. klwa_1010 → first row `{thumb:"KO", name:"Kokanee", week:"Weekends: keep 5 · Weekdays: release", value:"5 a day"}`; Bass "no limit"; Northern pike and walleye "closed".
8. x12p_1010 → Arctic grayling `{value:"4 a day", sizeHint:"· size limit"}` (golden "Arctic grayling 4 a day · size limit"); 4 states carry a hint.
9. vedder_615 (closed) → otherFish null, groups [] ; x15p8_1010 (open, no g28 group) → otherFish per split.

## Acceptance
`pnpm -C app check`; `vitest run conformance/src/regs/tail.parity packages/core/src/regs/tail conformance/src/regs/others.parity packages/core/src/regs/others` → 97/97 groups, 134/134 otherFish.

## Notes
- Contract: `GroupVM.quiet: Line | null`, `GroupVM.more: {text, open}`, `GroupVM.worked` (`label`, q with its colon), `OtherFishRowVM.sizeHint` — applied to contracts.md (`// (T19)`).
  `week` stays separate from `name` in the VM; the golden fuses them ("KokaneeWeekends: keep 5 · Weekdays: release").
- Plain rows (25 states, page v36 rows without a card) are **T19b** (`tickets/T19b-plain-rows.md`).
