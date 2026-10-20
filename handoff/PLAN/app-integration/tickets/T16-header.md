# T16 — presenter: header (name, date words, dot, part label, action, tabs)

Phase 1 · Depends on T00, T09 (`RegsInputs.item/partIx/place`), T12, T13 (strip status) · Blocks T21, T23 · Decisions D1 (map action), D7 (date sheet) · Size ½ day

Target **regs v33 @ 04765ab5**: prototype `hdr` 1012–1021, `statusOf` 979–982, `fullPart` 983, `hereLabel` 984–991,
`shortLabel` 992–1000, `placeRow` 1001–1002, `placeGo` 1003–1007, `fitLabel` 1008–1011 (inventory.json names).

## Goal
`headerOf(inp, opts)` returns `HeaderVM`: the water's name, "Today"/weekday + "Oct 10", the date pill's aria and status dot,
the part's two label candidates (the component picks the full one when it fits in two lines — `fitLabel` is layout, T23),
the Map/Parts action, and the four tab labels.

## Files
- create `app/conformance/src/regs/header.parity.test.ts` — the golden parity (conformance may import `@app/data/regs/node` and read golden.json; core may not import `@app/data` or `node:fs`). The core `*.test.ts` files named below hold **unit tests over small hand-built inputs only**.
- create `app/packages/core/src/regs/header.ts`, `app/packages/core/src/regs/header.test.ts`
- modify `words.ts`

## Signatures
```ts
// header.ts — ports prototype hdr, statusOf, fullPart, hereLabel, shortLabel, placeRow, placeGo
import type { RegsInputs } from "./inputs";
import type { HeaderVM, Day, Weekday } from "./model";

export function partLabels(inp: RegsInputs): { full: string; short: string } | null;   // hereLabel + shortLabel, the "same as name" rule
export function headerOf(inp: RegsInputs, o: { today: Day; status: "open" | "closed" | "tidal" | "outside" | "unknown"; canMap: boolean }): HeaderVM;
```
`HeaderVM` as contracts.md §2.1 (unchanged), with `dot` from `status`: closed → "closed", open → "open", anything else → "faint".

Words: `TODAY = "Today"`, `WD3 = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]` keyed by `Weekday`, `MON3`, `DATE_ARIA = (d) => "Change date, now " + d`,
`MAP = "Map"`, `PARTS = "Parts"`, `MAP_ARIA = (full) => "View on map: where these rules apply" + (full ? ", " + full : "")`,
`PARTS_ARIA = (full) => "Other parts" + (full ? " — now " + full : "")`,
`TABS = [["keep","What you can keep"],["gear","Gear"],["methods","Methods"],["licence","Licence"]]`.

## Ports
- **dayWord / date**: `day == today ? "Today" : WD3[weekday]`; date `MON3[m-1] + " " + d` ("Oct 10", "May 1"); `dateAria = DATE_ARIA(date)`.
- **dot** (`statusOf`): the prototype reads the strip's `wsstat` class from v36's card; the app passes T13's `StripVM.status`
  (`null` strip → tidal/outside → "faint"). Golden: x31p_1010 (tidal) `dot: "faint"`, vedder_615 "closed".
- **labels** (`hereLabel`/`shortLabel`/`placeRow`): with a part picker, `o = inp.item.parts[partIx]`:
  `long = o.label` (page v36's `t` minus `/ · \d+ sections?$/` — the store's `label_s` is already without the count; assert it);
  `full = sub names long ? long : (o.short uncut ? o.short : long)` where "uncut" = no "…";
  `short = o.short uncut ? o.short : long`. In v33 the "sub names it" test is `inp.item.sub.includes(long)` (page v36 subtitle).
  A water with no picker: the prototype falls back to the water map's part name; in the app that is `PartFacts.short`/`label` of
  part 0 — **when `short === item.name` the label slot is empty (`place: null`)** (golden klwa_1010, wilkes_1010, glenn_501).
- **action** (`placeGo`): prototype = Map when its water map export has the water, else Parts when the item has > 1 part, else
  none. App (D1): `canMap` (true for every named part the map can frame) → `{kind:"map", label:"Map", aria: MAP_ARIA(full || name-when-no-label)}`;
  else > 1 part → `{kind:"parts", label:"Parts", aria: PARTS_ARIA(full)}`; else null; unnamed section → null.
- **tabs**: always four (the Licence tab is hidden by T21 when `licence === null`).

## Reads
`inp.item.{name, sub, parts[partIx].{label, short}, picker}`, `inp.partIx`, `inp.day`, `inp.weekday`, `inp.place` (unnamed section).

## Tests
Golden parity lives in `app/conformance/src/regs/header.parity.test.ts`: `for await (const { id, g, page } of cases("header"))` with `requestOf` and `eqPlain` from
`app/conformance/src/regs/{golden,eq}.ts` (tests.md §1.2); the presenter under test is called on the case's inputs (until T21 lands the
harness hands the hydrated `RegsInputs` — T12 exposes `inputs()` on each case — after T21 the same assertions read `page().header`).
Every numbered case that names a golden state id is a parity case there; cases marked **Unit** (and property tests over hand-built
`GroupTotal`/`StatusFrame` fixtures) stay in the core `*.test.ts`, which never loads golden.json.
1. Parity, 134 states, of `name`, `dayWord`(golden `header.day`), `date`, `dateAria`, `dot`, `place.full`, `place.short`, `tabs[].label` (golden `tabs[].text`).
2. Action parity: **only where the prototype's water had a map** (golden `action.kind === "map"`): label + aria equal; where golden
   is `"parts"` assert the app's `canMap:false` branch reproduces `{label:"Parts", aria}` exactly (bc1_501:
   "Other parts — now 4 side channels, joining 55–59.1 km from the mouth"); golden null with a one-part water → `canMap:false` gives null.
3. vedder_1010 → `{name:"Vedder River", dayWord:"Today", date:"Oct 10", dateAria:"Change date, now Oct 10", dot:"open",
   place:{full:"Mouth → Vedder Crossing Bridge", short:"Mouth → Vedder Crossing Bridge"}, action:{kind:"map", label:"Map",
   aria:"View on map: where these rules apply, Mouth → Vedder Crossing Bridge"}}`.
4. vedder_615 → dayWord "Mon", date "Jun 15", dot "closed".
5. bc1_501 → dayWord "Fri", place `{full:"4 side channels, joining 55–59.1 km from the mouth", short:"4 side channels"}`.
6. cow1_1010 → place `{full:"From the CNR Mile 66 Trestle up to the CNR Bridge (mile 70.2)", short:"CNR Mile 66 Trestle → CNR Bridge (mile 70.2)"}`.
7. x5p0_1010 → place full = short = "Josephine Falls → Source" (the long label is not in the subtitle, the short is uncut).
8. klwa_1010 → place null; action aria "View on map: where these rules apply, Kootenay Lake — Lower West Arm".
9. x31p_1010 → dot "faint", place null.

## Acceptance
`pnpm -C app check`; `vitest run conformance/src/regs/header.parity packages/core/src/regs/header` green (100 % of the parity fields above).

## Notes
- The golden `place.shown` is the result of `fitLabel` at 390 px with fallback fonts — not a core field; T23 picks at layout.
- The prototype's weekday is the 2026 weekday of the snapshot day; the app's is `inp.weekday` (T05 `weekdayOf`). Same in 2026.
- No contract change.
