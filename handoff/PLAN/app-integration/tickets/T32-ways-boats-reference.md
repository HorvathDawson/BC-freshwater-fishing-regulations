# T32 — presenter: ways to fish, boats, reference fold

Phase 3 · Depends on T00, T06 (`RuleView`, species words), T09, T12, T30 (`sourceLine`, `closedUntil`, `GearCtx`), T31
(`yesNo`, `ELEM`) · Blocks T34, T35 · Decisions — · Size ≤ 1 day

## Goal
`core/src/regs/methods.ts` builds the Methods tab: **Ways to fish** as one ✓/✕ list with each allowed way's conditions small
under it ("Angling (rod and line) · with a downrigger: attached to your line by a quick-release · with a light: …",
"Spear or bow — not for trout or other game fish" over "not for game fish, salmon or protected species · burbot allowed"),
**Boats** (only when the water has boat rules), and the closed **Reference** fold "Handling fish · 21 rules that apply
everywhere" (the province's conduct rules grouped by moment). Each item ends in its own source line. A closed day: the same
closed sentence as Gear.

## Files
- create `app/packages/core/src/regs/methods.ts`, `app/packages/core/src/regs/methods.test.ts` (unit)
- create `app/conformance/src/regs/methods.parity.test.ts` (golden, block `methods`)
- modify `app/packages/core/src/regs/words.ts` — add `METHODS`

## Signatures
```ts
export interface WayChip { readonly method: string; readonly allow: boolean; readonly rix: number | null; readonly info: string }
export function countTxt(slot: string, c: GearClause | null): string;          // page v36 countTxt L1175–1186
export function tgtTxt(codes: readonly string[], species: SpeciesWords): string; // page v36 tgtTxt L1172 (GROUPWORD, else lcNames(expand([c])))
export function wayChips(g: GearAnswer, ctx: GearCtx & { species: SpeciesWords; exportConduct: Record<string, string> }): readonly WayChip[];
export function waysVM(chips: readonly WayChip[], pid: string, ctx: GearCtx): MethodsVM["ways"];
export function boatsVM(g: GearAnswer, pid: string, ctx: GearCtx): MethodsVM["boats"];     // null when vessel.active+timed is empty
export function referenceVM(g: GearAnswer, pid: string, ctx: GearCtx & { statics: Statics }): MethodsVM["reference"];  // null when 0 rules
export function buildMethods(inp: RegsInputs, ctx: GearCtx): MethodsVM;
export type GearClause = { slot?: string; max?: number; min?: number; unlimited?: boolean; must_be?: readonly string[]; members?: readonly string[]; unless?: readonly unknown[] };
// clauseOf([rix, i]) = ctx.rules.get(rix)?.export.fields.gear?.[i] ?? null   (page v36 clauseOf L1173)
```
`words.ts` `METHODS` (verbatim page v36 L1164–1171, L1235, L1249; prototype buildGear 1555–1570):
```ts
export const METHODS = {
  WAYS: "Ways to fish", BOATS: "Boats", REFERENCE: "Reference", HANDLING: "Handling fish",
  RULES_EVERYWHERE: (n: number) => `${n} rules that apply everywhere`,
  NOT_GAME: " — not for trout or other game fish", NOT_GAME_TEST: /not for game fish/,
  NOT_FOR: (t: string) => `not for ${t}`, FOR: (t: string) => `${t} allowed`,
  MUST_BE: (xs: string) => `must be ${xs}`, WITH_DEVICE: (d: string, xs: string) => `with a ${d}: ${xs}`,
  CRAYFISH_ONLY: "release anything in the trap that isn’t a crayfish", CRAYFISH_TEST: /other than crayfish/i,
  CAUGHT_HEAD: "When you catch a fish",
  WAYWHY: { fly_fishing_only: "fly fishing only: nothing but the fly on the line, no float, sinker or attractor",
            not_allowed_here: "not allowed here", not_lawful: "not a lawful way to sport fish", fly_fishing_only_here: "fly fishing only here" },
  SLOT_LABEL: { lines_per_angler: "Lines", terminal_attachments_per_line: "Hook, lure or fly per line", hooks_per_line: "Hooks per line",
    flies_per_line: "Flies per line", points_per_hook: "Hook points", hook_gap_mm: "Hook gap", weight_per_line_kg: "Weight on the line",
    bait_possession_kg: "Bait you can carry", light_to_hook_mm: "Light to hook" },
  MUST: { quick_release_to_line: "attached to your line by a quick-release", submerged: "submerged", attached_to_line: "attached to the line" },
  GROUPWORD: { ALL_GAME_FISH: "game fish", SALMON: "salmon", PROTECTED_SPECIES: "protected species", ALL_FIN_FISH: "any fish" },
  MOMENT_HEAD: { release: "When you release a fish", keep: "When you keep a fish", carry: "Carrying fish", never: "Never", also: "Also" },
  COUNT: { NO_LIMIT: "no limit", AT_LEAST: "at least ", UP_TO: "up to ", SINGLE_POINT: "Single point only (no trebles)" },
} as const;
```

## Ports (@04765ab5; page v36 @ same commit)
- page v36 `gearParts` ways L1234–1245 → `wayChips()`, per `GearAnswer.ways[]` in order:
  - `!allowed` → `{allow:false, info: WAYWHY[why] ?? ""}` (info is NOT drawn for ✕ — `WAYS` passes "").
  - `why === "fly_fishing_only"` → `{allow:true, info: WAYWHY.fly_fishing_only}`.
  - else bits, in this order: `not_for` → `NOT_FOR(tgtTxt(not_for))`; `for` → `FOR(tgtTxt(for))`; each `while[]` clause
    `c = clauseOf(e.clause)` (skip null): `c.must_be ? MUST_BE(c.must_be.map(m => MUST[m] ?? m.replace(/_/g," ")).join(", "))
    : \`${SLOT_LABEL[c.slot].toLowerCase()}: ${countTxt(c.slot, c)}\``; each `conduct[]` → `exportConduct[a] ?? a`;
    each `while_rules[]` rix → label matches `CRAYFISH_TEST` ? `CRAYFISH_ONLY` : `clean(export.label)`; each
    `devices[d]` → `WITH_DEVICE(d, must_be words ", ") + (within_mm != null ? ", " + countTxt("light_to_hook_mm", {max: within_mm}) : "")`.
    `info = [...new Set(bits)].join(" · ")`.
  - `tgtTxt`: each code → `GROUPWORD[c] ?? lcNames(expand([c]))` (T06 species words); 2+ → "a, b or c".
  - `countTxt`: verbatim switch (points_per_hook, hook_gap_mm `/10 cm`, weight/bait `kg`, light `/1000 m of the hook`, default `${pre}${n}`).
- prototype buildGear ways 1555–1560 + `WAYS` 1573 → `waysVM()`: ✓ name = `ELEM[m] + (NOT_GAME_TEST.test(info) ? NOT_GAME : "")`,
  note = info; ✕ name = `cap(lc(ELEM[m]))` (= ELEM[m] for every ELEM value), note "". `yesNo(ok, no)`.
  Source keys = the ✓ ways' `by[0]` in order, THEN the ✕ ways' (DOM order of `[data-rule]`), `open` `srcs:ways|${pid}`.
  The `set_line` ⓘ on an allowed "Set lines" is outside `<b>` — not in the item (parity).
- boats: page v36 L1255–1257 + prototype 1561–1565 → `boatsVM()`: rules `[...vessel.active, ...vessel.timed]`; item =
  `clean(export.label)`; source keys = same rules, `open` `srcs:boats|${pid}`; label "Boats".
- reference: page v36 L1246–1253 (`MOMENT_HEAD`, `PHRASE`, caught card, `alwaysN`) + `renderKit` always tile L1404
  (`{v:"Handling fish", s:\`${alwaysN} rules that apply everywhere\`}`) + prototype 1566–1570 → `referenceVM()`:
  - `n = Σ conduct[m].length + caught.length`; 0 → null.
  - groups: when `caught.length`: `{head: CAUGHT_HEAD, items: caught.map(rix => facts.plain ?? "")}` first; then for each
    `[m, acts]` of `Object.entries(g.conduct)` (decoded JSON key order — keep it): `{head: MOMENT_HEAD[m] ?? m, items:
    acts.map(([a]) => PHRASE[a] ?? statics.conductMeans[a] ?? statics.exportConduct[a] ?? a)}` where `PHRASE` =
    `static gear.moments` entries `[m, [[act, short, long]…]]` → act → `short` (the 2nd element).
  - `{label:"Reference", title:"Handling fish", sub: RULES_EVERYWHERE(n), groups, src}`; src keys = caught rixs then each
    act's `rs[0]`, `open` `srcs:always|${pid}`. Folded by default (`open:false` is the RN component's initial state).
- closed day: `{closed: closedUntil(...), ways:null, boats:null, reference:null}` (prototype `!st.kitTiles` branch).

## Reads
`inp.frames.gear.ways[]{method, allowed, by, why, not_for, for, while[{clause}], conduct, while_rules, devices{downrigger|light{must_be, within_mm}}}`,
`.vessel{active, timed}`, `.conduct{release|keep|carry|never|also: [act, rix[]][]}`, `.caught[]`; `inp.rules.get(rix).export.{label, fields.gear}`,
`.facts.plain`; statics `gearMoments`, `conductMeans`, `exportConduct`, `exportSpecies` (species groups for `expand`/`lcNames`).

## Tests (golden v33)

**Where the tests live** (tests.md §1.2): core may not import `@app/data` or `node:fs`, so every golden-driven case below
is in `app/conformance/src/regs/methods.parity.test.ts` over T12's harness — `for await (const {id, g, page} of cases("methods"))`,
`requestOf(id)` (today Oct 10, `pid` = the golden id), `eqPlain(plainLines(vm), goldenLines(g))`, one `it(id)` per state, skipped
when `provinceStore()` is null. ``methods.test.ts`` keeps only unit cases over small hand-built `GearAnswer`/`LicenceValue` literals
(cases marked *unit* below).
Cases 1–6 are parity; 7 is *unit*, plus `wayChips` over a hand `Way` with `not_for:["ALL_GAME_FISH","SALMON","PROTECTED_SPECIES"]` + `for:["BB"]` → name suffix and note.

1. **Parity, all 134 states** vs `golden.states[id].methods` (`closed`, `ways{label, list, src}`, `boats{label, items, src}`,
   `reference{label, title, sub, groups, src}`; `open` must be `false`). Tallies: 27 closed; 107 ways; 14 boats (bc1_501,
   bc1_1010, x14p0/1/2, x15p2, x16p1, x18p0, x21p0/1/2, x27p0/1/2); 107 references, every one
   `"Reference" · "Handling fish" · "21 rules that apply everywhere"` with heads
   `["When you catch a fish","When you release a fish","When you keep a fish","Carrying fish","Never"]`.
2. `bc1_501` ways ✓ `["Angling (rod and line)", "Spear or bow — not for trout or other game fish", "Crayfish traps"]` with notes
   `"with a downrigger: attached to your line by a quick-release · with a light: submerged, attached to the line, within 1 m of the hook"`,
   `"not for game fish, salmon or protected species · burbot allowed"`, `"release anything in the trap that isn’t a crayfish"`;
   ✕ `["Set lines","Nets","Snagging","Chumming"]`; src `"Province ›"` `srcs:ways|bc1_501` p.8.
   Boats `["No powered boats — between the Goat Creek confluence and the Talchako River confluence"]`, src `"This water’s ›"` p.43→49.
3. `klmb_1010` ✓ `Ice fishing` note `"lines: 1 · hook, lure or fly per line: 1 · Remove your ice hut before breakup · Warn others of your ice hole"`;
   ✕ includes `"Spear or bow"`.
4. a fly-only state (`vedder_515`): ✓ `Angling (rod and line)` note `"fly fishing only: nothing but the fly on the line, no float, sinker or attractor"`.
5. Reference items: `"When you catch a fish"` → `["Any fish snagged — even by accident — must be released."]`;
   `"When you release a fish"` → `["Gently, back in the water","Without harming it"]`; `"Never"` → 6 items starting
   `"Sell, buy or trade your catch"`; src `srcs:always|<id>` pages `[8, 10]`.
6. `vedder_615`: `{closed:"No fishing here until Jul 1.", ways:null, boats:null, reference:null}`.
7. `countTxt` units: `("points_per_hook",{max:1})` → `"Single point only (no trebles)"`; `("weight_per_line_kg",{max:1})` → `"up to 1 kg"`;
   `("light_to_hook_mm",{max:1000})` → `"within 1 m of the hook"`; `("lines_per_angler",{unlimited:true})` → `"no limit"`.

## Acceptance
```sh
pnpm -C app check
pnpm -C app exec vitest run --project logic packages/core/src/regs/methods conformance/src/regs/methods.parity
```
134/134 `methods` equal.

## Notes
- `clean(s)` = strip `*`, collapse whitespace, trim (page v36 L33) — put it in `words.ts` helpers or `line.ts`, not methods.ts.
- The "!" limited-way glyph (`i.lim`) of page v36 is NOT drawn; the prototype marks every ✓ row "✓" and moves the warning
  into the name suffix.
- The reference fold's item set is province-wide and identical across the 107 open states; the test still runs per state so
  a regional conduct rule would show up.
- No contract change: `MethodsVM` matches golden (sources use the T30 `SourceLineVM`).
