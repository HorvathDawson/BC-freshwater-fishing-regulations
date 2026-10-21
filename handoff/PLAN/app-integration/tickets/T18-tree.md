# T18 — presenter: the quota tree (tiers, size band, kind rows, pictures)

Phase 1 · Depends on T00 (`boldN`, `nbsp`), T06, T08, T12, T17 (`HeadCtx`: hdrRoot, tagShare, tag, rootPages, rootBook), T20 (`FISH` manifest) · Blocks T21, T26, T28 · Decisions — · Size 1 day

Target **regs v33 @ 04765ab5**: prototype buildKeep — `pics` 1124, `presOf`/`presP` 1139–1140, `nbsp` 1222, `boldN` 1223,
`sizeOf` 1224–1227, `stampST`/`dutyWords` 1232–1233, `kt` 1234–1248, `tierWords` 1249–1253, `pagesOf`/`tierH` 1254–1261,
`tnode` 1262–1264, `tops`/`tree` 1292–1294; prototype `fishBtn` 964–968, `FISHPIC`/`fishName` 955–963; page v36 `treeBlock` 936–945.
(inventory.json names: `buildKeep.sizeOf`, `buildKeep.kt`, `buildKeep.tierWords`, `buildKeep.dutyWords`, `buildKeep.presOf`.)

## Goal
`treeOf(ctx, card, inp, fish)` returns the group's quota folders: each tier a line with its number big in its own words
("Only 2 of the 4 may be from streams, not counting hatchery rainbow trout", the 2 big), the size box's two numbers ("Only 1 of the fish in this box may be over 50 cm (20 in) — 2 if one is a hatchery steelhead", both big), its Rule book page once per branch, the moved scope kicker, the tier's origin line; each kind
row = the data's sentence with its number, the size chip, the qualifiers, the pictures FIRST, then also-counts, duties (with
"Steelhead stamp ›"), presence.

## Files
- create `app/conformance/src/regs/tree.parity.test.ts` — the golden parity (conformance may import `@app/data/regs/node` and read golden.json; core may not import `@app/data` or `node:fs`). The core `*.test.ts` files named below hold **unit tests over small hand-built inputs only**.
- create `app/packages/core/src/regs/tree.ts`, `tree.test.ts`
- modify `words.ts`, `model.ts`, `line.ts` (link target, see Notes)

## Signatures
```ts
// tree.ts — ports prototype buildKeep.{nbsp, boldN, sizeOf, dutyWords, kt, tierWords, tierH, tnode, presOf, presP, pics}, fishBtn
import type { RegsInputs, CardModel } from "./inputs";
import type { HeadCtx } from "./grouphead";
import type { TreeNodeVM, TierVM, KindVM, FishPicVM, Line } from "./model";
import type { FishManifest } from "./fish.generated";
import type { TreeTier, TreeKind, Presence } from "../answers.generated";

export function sizeOf(detail: string | null | undefined): { size: string; q: readonly string[] };   // verbatim regex
export function tierWords(t: TreeTier): Line;                                                        // boldN + the size extra's 2nd number ("big2")
export function dutyWords(duty: string, waterName: string): string;                                 // v33: "this river" → "the Vedder River"
export function presOf(k: TreeKind, card: CardModel, drawn: ReadonlySet<number>): Presence | null;   // incl. rows page v36 did not draw
export function pictures(codes: readonly string[], fish: FishManifest, names: (c: string) => string): readonly FishPicVM[];
export function kindRow(k: TreeKind, env: TreeEnv): KindVM;
export function tierNode(t: TreeTier, env: TreeEnv, root: boolean, seen: ReadonlySet<number>): TierVM;
export function treeOf(ctx: HeadCtx, env: TreeEnv): readonly TreeNodeVM[] | null;                   // null when g.tree is empty (legacy buckets)
export interface TreeEnv { readonly inp: RegsInputs; readonly card: CardModel; readonly fish: FishManifest; readonly drawn: ReadonlySet<number>; readonly stampST: boolean; readonly ctx: HeadCtx }
```
Model (supersedes contracts.md §2.2 TreeNodeVM):
```ts
export interface TierVM { t: "tier"; tier: "total"|"own"|"share"|"count"|"size"; tid: string; kicker: string | null; line: Line;
  sub: string | null; origin: OriginVM | null; band: boolean; short: string; children: readonly TreeNodeVM[] }
export interface KindVM { t: "kind"; kind: string; species: readonly string[]; folded: boolean; zero: boolean;
  says: Line | null; size: { text: string; rule: boolean } | null; qualifiers: string | null; pictures: readonly FishPicVM[];
  also: readonly string[]; duty: Line | null; presence: Line | null }
export interface FishPicVM { code: string; caption: string; open: SheetKey; sil: false } | { code: string; sil: true }
```
Words: `HOW_TELL = "How to tell ›"`, `STEELHEAD_STAMP = "Steelhead stamp ›"`, `STAMP_NEEDLE = "Steelhead Conservation Surcharge Stamp"`,
`CAPTION = (c) => c + " ›"`, `THE_NAME = (name) => "the " + name`.

## Ports
- **nbsp / boldN** (1222–1223): `(\d) (cm|in)\b` → U+00A0; the FIRST `\b${n}\b` becomes a num run; `n == null` → no num.
- **tierWords** (1249–1253): `boldN(says, n, "big")`; for `tier === "size" && extra`, after the first " — " the first `\b\d+\b` is a
  `num` run with size `"big2"` (golden `big[].second`).
- **tierNode / tierH** (1254–1264): `key = rules.get(t.by)`; `pages = printed pages of key.facts.book`; `fresh = pages has one not
  in seen`; line = tierWords(t) + (`root && qtip` — never in v33) + (`fresh` → `[" ", ...bookRuns]`). kicker = `ctx.tag` when
  `t === ctx.tagShare`. sub = `tier === "own" && why_here ? cap1(why_here)` (nbsp). origin = `t.origin` → `{says:[text t.origin],
  howtell:true, book: !g.origin ? ctx.rootBook : []}` (prototype draws `obtn` = How to tell ›). Children get `seen ∪ pages`.
  Top level: `tops = hdrRoot ? hdrRoot.children : g.tree`; `root = !hdrRoot`; `seen = ctx.rootPages`. `band = tier === "size"`.
  `short` = T28 `shortOf(t)` (stub "" until T28).
- **sizeOf** (1224–1227) verbatim: split detail on ", "; `/^(.*?)((?:or )?(?:over |under |up to )?\d[\d–]* cm\b.*)$/` → qualifier
  prefix + size; `/^any (size|length)$/` → size; else qualifier; prefer non-"any" sizes, joined ", ".
- **kindRow / kt** (1234–1248), v33: `size = k.size_rule || sizeOf(k.detail).size` (`rule: !!k.size_rule`), shown when `size && !zero`;
  `zero = k.n === 0` (no num run, no chip); `folded` → `says: null`; else `says = boldN(k.says, zero ? null : k.n)` + for
  `species.includes("ST") && /\bhatchery steelhead only\b/.test(k.says)` → `[" ", link HOW_TELL → "hatch"]`. qualifiers = q joined ", ".
  pictures = `pictures(k.species)` (FIRST after the chip, 20f). also = `also_counts.map(a => cap1(a.says))` (nbsp). duty (v33) =
  `duties.map(x => dutyWords(cap1(x)))` joined " · " + (`stampST && species.includes("ST")` → `[" ", link STEELHEAD_STAMP → {tab:"licence"}]`);
  `dutyWords` replaces the first `/\bthis (river|stream|lake|creek)\b/i` with `"the " + inp.item.name`. presence = `presOf(k)` →
  `[text p.says, " ", term ["steelhead"]]`. (`size_note` → `.ksn` is never present in v33 — golden miss list; port it as a
  `also`-style line anyway.)
- **presOf** (1139): `k.presence` else the presence of a row NOT drawn by page v36 (`drawn` = rows with an article: page v36
  `rowHtml` returns '' for a `parentRow` row — T08 exposes `parentRow`) whose members equal `k.species` as a set.
- **pictures / fishBtn** (964): per code, the FIRST manifest image with that code → `{code, caption: CAPTION(image.caption),
  open: "sp:"+code}`; no image → `{code, sil:true}` (neutral silhouette, not a button; golden drops it).
- **stampST** (1232): `/Steelhead Conservation Surcharge Stamp/` found anywhere in the day's licence answer (`inp.frames.licence`
  holds/profiles/documents and the export licence names it cites).

## Reads
`GroupTotal.tree`, `TreeTier.{id,tier,n,says,by,extra,why_here,origin,children}`, `TreeKind.{species,name,n,says,detail,size_rule,
also_counts,duties,presence,folded,size_note}`, `card.rows[].{presence, members, parent}`, `inp.rules.get(by).facts.book`,
`inp.item.name`, `inp.frames.licence`, fish manifest.

## Tests
Golden parity lives in `app/conformance/src/regs/tree.parity.test.ts`: `for await (const { id, g, page } of cases("keep"))` with `requestOf` and `eqPlain` from
`app/conformance/src/regs/{golden,eq}.ts` (tests.md §1.2); the presenter under test is called on the case's inputs (until T21 lands the
harness hands the hydrated `RegsInputs` — T12 exposes `inputs()` on each case — after T21 the same assertions read `page().keep`).
Every numbered case that names a golden state id is a parity case there; cases marked **Unit** (and property tests over hand-built
`GroupTotal`/`StatusFrame` fixtures) stay in the core `*.test.ts`, which never loads golden.json.
Mapping: tier → `tier, tid, kicker, line = plain(line)` (textContent), `big = num runs [{n, second: size==="big2"}]`, `pages`,
`sub`, `origin.{says, howtell, pages}`; kind → `kind, sp, folded, zero, says = plain(says)`, `n` = first num run or null,
`howtell`, `size = size?.text`, `q`, `pics[].{code, caption}` (golden caption is nbsp-normalised: "Brook trout ›"), `also`, `duty = plain(duty)`, `presence = plain(presence)`, `terms`.
1. vedder_1010 share tier t3: kicker "All Region 2 streams", line "Only 2 of the 4 may be from streams, not counting hatchery rainbow trout",
   big [2], pages [], origin `{says:"From streams, keep hatchery fish only — wild trout and char go back.", howtell:true, pages:["Rule book p.21 ↗"#23]}`.
2. vedder_1010 size tier t4: line "Only 1 of the fish in this box may be over 50 cm (20 in) — 2 if one is a hatchery steelhead",
   big `[{n:"1",second:false},{n:"2",second:true}]`.
3. vedder_1010 kinds under t4: "Up to 2 may be hatchery brook, brown or cutthroat trout" n 2, size "keep 30 cm (12 in) or longer; release the rest"
   (v33 size_rule), pics EB "Brook trout ›", GB "Brown trout ›", CT "Coastal cutthroat trout ›"; Steelhead: says
   "Up to 2 may be hatchery steelhead only — wild steelhead go back How to tell ›", size "over 50 cm (20 in)", q "rainbow trout",
   duty "10 a year · Stop fishing the Vedder River after 2 a day · Record each on your licence right away Steelhead stamp ›",
   presence "A rainbow trout over 50 cm (20 in) here counts as a steelhead. ⓘ", terms ["steelhead"].
4. vedder_1010 own tier t2: "This river’s rule: up to 4 hatchery rainbow trout a day Rule book p.22 ↗" (fresh page), child Rainbow trout
   folded (says null), size "50 cm (20 in) or less".
5. klwa_1010 (root = the lake's own tier, so tops are kinds): "Up to 2 may be brook, brown, cutthroat, lake or rainbow trout", also
   ["Only 1 cutthroat or rainbow trout over 50 cm (20 in)"]; "Only 1 of the 2 may be Dolly Varden/bull trout", pics DV "Bull trout ›".
6. glenn_501: own tier "The Duncan River’s rule: up to 5 rainbow trout a day Rule book p.36 ↗", sub "Its row covers its tributaries, so it applies here";
   share tier pages [] (p.34 is the root's — book once per branch).
7. ape_501: Cutthroat trout zero: says "No cutthroat trout may be kept", n null, size null, q "catch and release"; Rainbow size
   "keep 25–50 cm (10–20 in); release smaller ones".
8. bc1_501: count tier t3 "1 cutthroat or rainbow trout in all (part of the 2)" under own tier "This river’s rule: up to 2 a day (part of the 5) Rule book p.43 ↗".
9. x25p_1010 r2 Steelhead kind: presence "Steelhead are known to be in this water. ⓘ", duty "10 a year · Record each on your licence right away Steelhead stamp ›".
10. wilkes_1010: presence "Steelhead rules apply here; steelhead may not be present in this water. ⓘ" (via presOf of an undrawn row if so — assert source).
11. **Property (r20b number-once)**: for every folded kind, its tier's line contains the kind's number and the kind draws no num run;
    for every tier, a Rule book page appears at most once on the path root → tier (count book runs per printed page along each branch ≤ 1).
12. **Property (r20e/r20f)**: every share tier whose parent total says " across " reads "Only N of the M may be …" with N a num run and M ≠ N.

## Acceptance
`pnpm -C app check`; `vitest run conformance/src/regs/tree.parity packages/core/src/regs/tree` → 130/130 tiers, 273/273 kinds. Evidence for the data side:
`.venv/bin/python app/design/regs-v11/port/verify_map.py` (tier text = `TreeTier.says` 130/130; kind sentence starts with `TreeKind.says`, folded none 273/273).

## Notes
- Contract: `Run.link.open` `{ tab: Tab }` ("Steelhead stamp ›" switches to the Licence tab), `KindVM.duty: Line | null`, `KindVM.size: {text, rule} | null`
  (the v33 `data-size-rule` chip is bold), `FishPicVM` `sil` variant — applied to contracts.md (`// (T18)`). Separators per contracts.md §1:
  the kind sentence and its "How to tell ›", the duty and "Steelhead stamp ›", the presence and its ⓘ each get a `" "` text run.
- The krel28 release-only row (`ReleaseRowVM`) is drawn only inside a page-v36 plain row (no `group.headline`) — no v33 g28 tree
  holds one; it belongs to the plain-rows port, T19b. Keep `ReleaseRowVM` in the model.
- Golden `pics` lists only buttons (`.kpics > button`); a silhouette has no caption — compare `pictures.filter(p => !p.sil)`.
- **PIPELINE NEED (review 2026-10-21, owner rule):** "T08 exposes `parentRow`" — it does not; card.ts has no parent field, so
  grouphead.ts infers pool nesting from `xref` facts (`parentRow` → `drawnRows`, used by presOf's undrawn-row presence). The
  store must carry `rows_row.parent: int | null`; until then the port stays, held by the parity (T17 Notes).
  **RESOLVED (batch 9, rows 31):** the store carries `rows_row.parent` (rows.py `parent_of`); grouphead.ts `parentRow` reads it.
- Review 2026-10-21: kind `lines` ARE compared (keepGroups.ts kindProj). Only the prototype's UR7 stampsFor lines ("You need … ›",
  2 kinds in x4p_1010) are dropped from the golden — exclusions.ts STAMP_LINE_IDS, a keyed `lines` entry the summary reports.
  **RESOLVED (batch 9, rows 31):** the store carries `TreeKind.stamps` / `NotKept.stamps` (pipeline `stamps.py`); tree.ts
  `stampLine` words them; the exclusion is gone and the stamp lines are compared.
