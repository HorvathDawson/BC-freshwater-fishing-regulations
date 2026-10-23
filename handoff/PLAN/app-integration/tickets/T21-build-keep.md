# T21 — `buildRegsPage()` Keep slice + Keep golden parity on all states

Status: DONE — 3665f151, a445aa91, b824f4ee.

Phase 1 · Depends on T07, T08, T09, T12, T13–T20 · Blocks T23, T27, T28 · Decisions D1 (map action) · Size 1 day

Target **regs v33 @ 04765ab5**: prototype `buildKeep` 1027–1326 (the composition and draw order), page v36 `renderCard` 702–716
(what the card holds on an open / closed / tidal / outside day), `rowsHtml` 692–700.

## Goal
`buildRegsPage(inp, opts)` exists and fills `header` and `keep` (Gear/Methods/Licence/sheets come in T30–T36; until then they
are `null`-stubs behind the same signature). One parity test walks all 134 golden states and compares every Keep block; a set of
property tests pins the three owner rules the readers fought for (number once, region vs stream words, form a head).

## Files
- create `app/conformance/src/regs/keep.parity.test.ts` — the golden parity (conformance may import `@app/data/regs/node` and read golden.json; core may not import `@app/data` or `node:fs`). The core `*.test.ts` files named below hold **unit tests over small hand-built inputs only**.
- create `app/packages/core/src/regs/page.ts`, `app/packages/core/src/regs/keep.ts`, `keep.test.ts` (unit: composition order over hand-built VMs),
  `app/conformance/src/regs/keep.props.parity.test.ts` (the property tests below, over every golden case and the T10 fixture store)
- modify `app/packages/core/src/regs/index.ts` (export `buildRegsPage`, the VM types), `model.ts`

## Signatures
```ts
// keep.ts — prototype buildKeep composition; page v36 renderCard
export function keepOf(inp: RegsInputs, env: { settled: Settled; card: CardModel; fish: FishManifest; today: Day; tot: "a" | "b" }): KeepVM;
// page.ts
export function buildRegsPage(inp: RegsInputs, opts: { today: Day; who: Who; tot?: "a" | "b"; canMap?: boolean; fish?: FishManifest }): RegsPage;
```
KeepVM (supersedes contracts.md §2.2):
```ts
export type KeepBlock = "strip" | "inherited" | "banner" | { caveat: number } | { group: number } | { plain: number } | "otherFish" | "possession" | "whereNot";
export interface KeepVM {
  readonly order: readonly KeepBlock[];
  readonly strip: StripVM | null; readonly inherited: InheritedVM | null; readonly banner: BannerVM | null;
  readonly caveats: readonly CaveatVM[];          // T15b (empty until it lands)
  readonly groups: readonly GroupVM[]; readonly plainRows: readonly PlainRowVM[];   // plainRows: T19b (row ids only until it lands)
  readonly otherFish: readonly OtherFishRowVM[] | null; readonly possession: PossessionVM | null; readonly whereNot: WhereNotVM | null;
}
export interface GroupVM { row: number; title: string; scope: string | null; head: GroupHeadVM; origin: OriginVM | null;
  tree: readonly TreeNodeVM[] | null; notKept: readonly NotKeptVM[]; worked: WorkedVM | null; quiet: Line | null; more: { text: string; open: SheetKey } }
```

## Ports — the composition (`keepOf`)
1. `inp.key == null` (outside B.C.) → page v36 renderCard: one foot line "This part of the water is outside B.C.: no B.C. rules
   apply." (words `OUTSIDE_BC`) — `order: []`, KeepVM all null; T42 draws it.
2. Tidal (`inp.key.tidal`) → `order: ["banner"]`, banner = T14 tidal form, nothing else (golden x31p_1010 `["div.banner"]`).
3. Otherwise, in this order (golden `keep.order` selectors in brackets):
   `strip` [div.wstrip] (T13) → `inherited` [div.inh] (T15; the prototype inserts it right after the strip) → `banner` [div.banner]
   (T14, closed day only) → each caveat [div.caveats.top | details.inpart…] (T15b) → for each drawn row in page v36 order
   (`splitRows`, T19): `{group: i}` [article.row.ledger.g28] when the row has `group.headline` (T17 head + T18 tree + T19 tail),
   else `{plain: i}` [article.row.ledger] → `otherFish` [section.sec] (T19; only when `rest` exists) → `possession`
   [section.posrow] (T15) → `whereNot` [section.nofish] (T15; the prototype appends it last).
4. Closed day (`display.status === "closed"`): no rows, no other fish, no possession — `strip, banner, whereNot` (26 states,
   golden `["div.wstrip","div.banner","section.nofish"]`). Rule proved by `app/design/regs-v11/port/verify_map.py` (134/134).
5. `GroupVM.scope` = `moveTag ? null : scopeBadge` (T17).
6. `RegsPage.status`: tidal → "tidal"; key null → "outside"; strip.status; no strip → "unknown". `header` = T16 with `canMap` (D1).

## Reads
everything T13–T20 read; `inp.frames.display.status`, `inp.key.tidal`.

## Tests
Golden parity lives in `app/conformance/src/regs/keep.parity.test.ts`: `for await (const { id, g, page } of cases("keep"))` with `requestOf` and `eqPlain` from
`app/conformance/src/regs/{golden,eq}.ts` (tests.md §1.2); the presenter under test is called on the case's inputs (until T21 lands the
harness hands the hydrated `RegsInputs` — T12 exposes `inputs()` on each case — after T21 the same assertions read `page().keep`).
Every numbered case that names a golden state id is a parity case there; cases marked **Unit** (and property tests over hand-built
`GroupTotal`/`StatusFrame` fixtures) stay in the core `*.test.ts`, which never loads golden.json.
`keep.parity.test.ts` (conformance) — **golden parity, 134 states** (`cases("keep")`; a missing/stale store is a skip with a reason):
- `order`: map each KeepBlock → selector (`strip`→`div.wstrip`, `inherited`→`div.inh`, `banner`→`div.banner`, group→`article.row.ledger.g28`,
  plain→`article.row.ledger`, `otherFish`→`section.sec`, `possession`→`section.posrow`, `whereNot`→`section.nofish`, caveat→ golden's
  `div.caveats.top`/`details.inpart.hot`) = golden `keep.order`.
- strip / inherited / banner / possession / whereNot / otherFish: the T13/T14/T15/T19 field mappings (call their exported comparers).
- groups: `title, scope, head.*, origin, tree, notKept, worked, quiet, more` via T17/T18/T19 comparers; plus golden `group.lines`
  compared with `eqPlain` (contracts.md §1) against every Line in draw order joined by one space (catches a missing block).
- Explicit exclusions (asserted to be exactly these, so a new failure cannot hide):
  - `plainRows` content on 25 states (T19b): cow1_1010, cow2_1010, cow3_1010, klmb_1010, iron_720, iron_1010, x5p1_1010, x11p1_1010,
    x15p8_1010, x15p5_1010, x15p13_1010, x15p4_1010, x15p14_1010, x15p15_1010, x15p6_1010, x16p3_1010, x16p8_1010, x16p2_1010,
    x16p4_1010, x16p6_1010, x16p0_1010, x16p1_1010, x21p0_1010, x32p0_1010, x37p2_1010 (their `order` still must match);
  - caveat block content on 9 states (T15b): x4p_1010, x14p2_1010, x15p2_1010, x17p3_1010, x17p4_1010, x17p7_1010, x19p6_1010, x19p7_1010, x19p4_1010.
- Spot checks with literals: vedder_1010 order `["strip",{group:0},"otherFish","possession","whereNot"]`; vedder_615
  `["strip","banner","whereNot"]`; glenn_501 `["strip","inherited",{group:0},"otherFish","possession","whereNot"]`;
  x25p_1010 two groups (r0 "Trout and char", r2 "Steelhead"); x31p_1010 `["banner"]`.

`keep.props.parity.test.ts` (conformance) — property tests over every golden state's VM (and over every state of the T10 fixture store):
1. **number once (r20b)**: in a group, a folded kind's number appears in its tier's `line` and nowhere in the kind; no kind row
   draws a num equal to its tier's n when it is the tier's only kind; a printed Rule book page appears at most once per root→leaf path.
2. **region vs stream words (r20e/r20f)**: when the root total says " across " (a region's total) and a share tier holds part of it,
   the share's line matches `/^Only \d+ of the \d+ may be /` with both numbers num runs ("Only 2 of the 4 may be from streams, not
   counting hatchery rainbow trout", vedder_1010), the head scope is null and the share's kicker is the moved tag ("All Region 2 streams").
   No group head reads `/streams$/` while its words say " across ".
3. **form a head (r20h)**: `tot:"a"` (default) on every group with a relTier: `head.form === "a"`, the first num run of the head is
   `headline.n` and `plain(lead)` = `headline.says` (vedder_1010 "2 a day per angler from streams"), `ofTotal` true and
   `plain(words)` = the root's says ("4 a day per angler across Region 2, lakes included"). `tot:"b"` flips: first num = root n, `shareOf` set.
   A group without relTier is identical under a and b.
4. Every string in the VM is a data string or a `words.ts` constant (the `no-hand-facts` helper from tests.md, run on the Keep slice).

## Acceptance
`pnpm -C app check`; `vitest run conformance/src/regs/keep packages/core/src/regs/keep` → 134/134 states with only the two listed exclusions;
`.venv/bin/python app/design/regs-v11/port/verify_map.py` passes (the data-side rules this composition relies on).

## Notes
- Contract: `KeepBlock` `{ caveat: number }`, `KeepVM.caveats`, `PossessionVM`, `GroupVM.quiet/more/worked`, `GroupVM.buckets` dropped — applied to
  contracts.md (`// (T21)` and the per-ticket marks). No v33 state draws buckets; `group.tree` is always present at rows 26 — refuse a rows frame
  without `tree` in T08.
- Text comparison is contracts.md §1 (golden b999cfd7): within one Line runs concatenate as is, separate elements are joined by one space, then
  whitespace collapses — "2 a day per angler from streams of the 4 …", "Kokanee release", "… leap Rule book p.9 ↗". `eqPlain` does it.
- The two exclusions shrink as T15b (caveats) and T19b (plain rows) land; T19b keeps klmb_1010's pooled plain row excluded pending its decision.
- Sheet keys in golden carry the state id (`other:vedder_1010|o0`, `pos:vedder_1010|pos`, `src:…|r0`); T12's `sheetKeyOf(id, key)` strips `${id}|`.
- Pin bar parity is T28, not here (25 states throw in the prototype).
