# T31 — presenter: bait list

Status: DONE — efa0a841.

Phase 3 · Depends on T00, T06, T09, T12, T13 (book days), T30 (`sourceLine`, `changeVM`, `bookBits`, `GearCtx`) · Blocks T34,
T35 · Decisions — · Size ½ day

## Goal
`core/src/regs/bait.ts` builds the Gear tab's **Bait** item: the ban note with its ⓘ ("No natural bait of any kind while the
ban runs."), the dated bait-ban changes from the strip ("Next May 1–31: bait ban", "Now Jun 15–Oct 31: bait ban · in force
today", v33's "Now All year: bait ban · in force today"), ONE ✓ Allowed / ✕ Not allowed list (each item its name, its note
small under it — "Roe (fish eggs) · the fish or fish parts ban excepts it"), and the item's source line.

## Files
- create `app/packages/core/src/regs/bait.ts`, `app/packages/core/src/regs/bait.test.ts` (unit)
- create `app/conformance/src/regs/bait.parity.test.ts` (golden, block `gear` → `.bait`)
- create `app/packages/core/src/regs/yesno.ts` (shared with T32) + `yesno.test.ts`
- modify `app/packages/core/src/regs/words.ts` — add `BAIT`, `YESNO`, `ELEM`

## Signatures
```ts
// yesno.ts — prototype yesNo (1452–1456): ✓ items then ✕ items; an empty column is omitted; note "" → null
export function yesNo(ok: readonly (readonly [string, string])[], no: readonly (readonly [string, string])[]): YesNoVM;

// bait.ts
export interface BaitItem { readonly ok: boolean; readonly name: string; s: string; readonly rix: number | null }  // s mutable: the UR2-7 move edits it
export function baitItems(g: GearAnswer): BaitItem[];            // page v36 gearParts bait: ELEM name, [sub, carry].join(" · ")
export function note2(s: string): string;                         // prototype buildGear.note2: strip /^(allowed|banned|bait ban)(\s*·\s*)?/
export function moveAllowances(items: BaitItem[]): void;          // UR2 7 / 20h (see Ports)
export function datedBans(bits365: readonly (number | null)[], today365: number, labels: readonly StripState[]): readonly ChangeVM[];  // prototype dated(st, 16)
export function allYearBan(bits365: readonly (number | null)[], items: readonly BaitItem[]): ChangeVM | null;  // v33 UR4 fix 6
export function buildBait(inp: RegsInputs, ctx: GearCtx): BaitVM | null;     // null on a closed day / no gear frame
```
`words.ts`:
```ts
export const ELEM = { roe: "Roe (fish eggs)", invertebrate: "Water insects and crayfish", worms: "Worms", fin_fish: "Fish or fish parts",
  dead_fin_fish: "dead fish", live_fin_fish: "live fish", artificial_fly: "flies", artificial_lure: "lures",
  angling: "Angling (rod and line)", fly_fishing: "Fly fishing", ice_fishing: "Ice fishing", set_lining: "Set lines",
  spear_fishing: "Spear or bow", crayfish_trapping: "Crayfish traps", netting: "Nets", snagging: "Snagging",
  chumming: "Chumming", downrigger: "Downrigger", light: "Light to attract fish" } as const;          // page v36 ELEM L1166
export const BAIT = {
  LABEL: "Bait",
  BAN_NOTE: "No natural bait of any kind while the ban runs.",                  // + term "bait_ban" (page v36 L1226)
  WHY: { bait_ban: "bait ban", on_streams: "on streams", on_lakes: "on lakes", not_on_streams: "not on streams",
         not_on_lakes: "not on lakes", banned_roe_ok: "banned · roe is still OK", allowed: "allowed" },   // page v36 WHY L1228
  ALLOWED: "allowed",
  EXCEPTED: (elem: string) => `allowed · the ${elem} ban excepts it`,            // elem = ELEM[...].toLowerCase()
  CARRY: (kg: number) => `carry up to ${kg} kg`,
  STILL_OK: (w: string) => `${w} is still OK`,
} as const;
export const YESNO = { OK: "Allowed", NO: "Not allowed", OK_MARK: "✓", NO_MARK: "✕" } as const;
```

## Ports (@04765ab5)
- page v36 `gearParts` bait block L1227–1233 → `baitItems()`: per `GearAnswer.bait[]` in order:
  `sub = b.excepted_from ? BAIT.EXCEPTED(ELEM[b.excepted_from.element].toLowerCase()) : b.by ? (b.why ? (WHY[b.why] ?? b.why) : "") : BAIT.ALLOWED`;
  `s = [sub, b.carry_kg != null ? CARRY(b.carry_kg) : ""].filter(Boolean).join(" · ")`; `rix = b.by?.[0] ?? null`.
  `also_allowed[]` ("✓ dead fish …" `<small>`) is NOT shown — the prototype reads only the `<span>` (parity; see Notes).
- `buildGear.note2` 1532 → `note2()`.
- buildGear bait 1533–1550 "UR2 7 / Round 20h" → `moveAllowances()`: for each ✕ item whose `note2(s)` matches
  `/^(.*?)(?:\s*·\s*)?\b(\w+) is still OK$/i`: find the first ✓ item whose name starts with m[2] (case-insensitive); if found,
  `x.s = m[1]` and, unless the ✓ item's `s` already matches `/ban excepts it/i`, `to.s = [to.s, STILL_OK(m[2])].filter(Boolean).join(" · ")`.
  (Gear 11+ data gives the ✓ roe item `excepted_from`, so in v33 only the ✕ side changes; the fallback is kept and unit-tested.)
- `dated` 1366–1372 with bits = 16 → `datedBans()`: walk `runs` of the 365 book-day bits; a run with `f != null && !(f & 1) &&
  (f & 16) && n < 365` → `changeVM(spanWords(i, i+n-1), labels(16), today365 ∈ run)` where the words are
  `[4, 8, 16].filter(b => b & 16 & f).map(b => static display.strip[bit=b].label)` joined " · " (= "bait ban").
- v33 1541–1545 (UR4 fix 6) → `allYearBan()`: when `datedBans` is empty AND the year has runs AND every day's bits are
  non-null with bit 16 AND some item is ✕ AND the first ✕ item's `s` matches `/ban/i` →
  `changeVM(GEAR.ALL_YEAR, firstNo.s.replace(/^(allowed|banned)\s*·\s*/, "").trim(), true)` → plain "All year: bait ban · in force today".
- layout 1547–1550 → `BaitVM`: `label` "Bait"; `notes` = `g.bait_ban ? [[text BAN_NOTE, term bait_ban (if in glossary)]] : []`;
  `dated` = all-year ?? dated runs; `list` = `yesNo(ok.map(x => [x.name, note2(x.s)]), no.map(x => [x.name, note2(x.s)]))`;
  `src` = `sourceLine("srcs:bait", items.filter(x => x.rix != null).map(x => x.rix), ctx)`.

## Reads
`inp.frames.gear.bait[]{element, ok, by, why, carry_kg, excepted_from{element, by}}`, `.bait_ban`; `inp.frames.display.status`;
`bookBits(inp)` (T30) ← `inp.year.displayByDay[].strip`; `statics.strip[]{bit, label}`; `statics.glossary`.

## Tests (golden v33)

**Where the tests live** (tests.md §1.2): core may not import `@app/data` or `node:fs`, so every golden-driven case below
is in `app/conformance/src/regs/bait.parity.test.ts` over T12's harness — `for await (const {id, g, page} of cases("gear"))`,
`requestOf(id)` (today Oct 10, keys compared via `goldenKeyOf(key, id)`), `eqPlain(plainLines(vm), goldenLines(g))`, one `it(id)` per state, skipped
when `provinceStore()` is null. ``bait.test.ts`` keeps only unit cases over small hand-built `GearAnswer`/`LicenceValue` literals
(cases marked *unit* below).
Cases 1–6 are parity; 7–8 are *unit*, plus `datedBans`/`allYearBan` over hand 365 arrays (a Feb run reads "Jan 1–Feb 28").

1. **Parity, all 134 states** vs `golden.states[id].gear.bait`: `label`, `notes` (plain, ⓘ included: `"No natural bait of any kind while the ban runs. ⓘ"`),
   `dated` (`chip`, `when + ":"`, `plain` = `text`, `inForce` = `now`), `list` (`col`, `head`, items `mark`/`name`/`note`), `src`.
   Tallies: 107 with bait (27 closed → null), 67 with the ban note, 68 with a dated line (67 "Now"), 20 "All year".
2. `vedder_1010`: `dated` `[{chip:"Next", plain:"May 1–31: bait ban"}]`; list ✓ `Worms` (note null), ✓ `Roe (fish eggs)` note
   `"the fish or fish parts ban excepts it"`, ✓ `Water insects and crayfish` note `"on streams"`; ✕ `Fish or fish parts` (null);
   src `{who:"Province ›", open:"srcs:bait" (golden "srcs:bait|vedder_1010"), pages:[8→10]}`; notes `[]`.
3. `vedder_515`: notes `["No natural bait of any kind while the ban runs. ⓘ"]`, dated `{chip:"Now", plain:"May 1–31: bait ban · in force today"}`,
   only a ✕ column (Worms, Roe (fish eggs), Water insects and crayfish, Fish or fish parts), src `"This water’s ›"` p.22.
4. `glenn_501`: dated `[{chip:"Now", when:"All year", plain:"All year: bait ban · in force today"}]`, src `"Downstream ›"` p.36→38.
5. `x15p0_1010`: one ✓ column, `Roe (fish eggs)` note `"carry up to 1 kg"`, `Fish or fish parts` ✓; dated `[]`; src `"This water’s ›"` p.59.
6. a lake with ✕ `Water insects and crayfish` note `"not on lakes"` (12 states; pick the first from golden).
7. `moveAllowances` unit (hand items): ✕ `{name:"Fish or fish parts", s:"banned · roe is still OK"}` + ✓ `{name:"Roe (fish eggs)", s:""}`
   → ✕ note null, ✓ note `"roe is still OK"`; with ✓ `s:"allowed · the fish or fish parts ban excepts it"` → ✓ unchanged.
8. `yesNo([], [["Nets",""]])` → one column `{col:"no", head:"Not allowed", items:[{mark:"✕", name:"Nets", note:null}]}`.

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project logic packages/core/src/regs/bait packages/core/src/regs/yesno conformance/src/regs/bait.parity
```
134/134 `gear.bait` equal.

## Notes
- CONTRACT CHANGE: applied to contracts.md (§2.3 `BaitVM.notes: Line[]`).
- The dated words come from `static display.strip` labels (bit 16 → "bait ban"), not words.ts.
- `also_allowed` (dead fish as bait while …) is dropped by the prototype's DOM read — a data fact the page never shows.
  Kept for parity; raise with the owner (decisions.md) before showing it.
- A ban run that is also closed (`f & 1`) is skipped — the closure says it.
