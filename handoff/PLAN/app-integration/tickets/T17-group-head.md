# T17 — presenter: group head (title, scope kicker, forms a/b, origin line)

Phase 1 · Depends on T00, T06 (species names, `RuleView`), T08 (`CardModel.rows` = page v36 hydRow rows), T12 · Blocks T18 (tagShare, rootPages, rootBook), T21, T26, T28 (shortOf) · Decisions — (form a is the owner's pick, r20h) · Size 1 day

Target **regs v33 @ 04765ab5**: prototype buildKeep 1027–1326 — moveTag/tagShare 1100–1106, head/origin 1206–1213, rootT/hdrRoot
1268, relTier 1275, bigWords 1277, rootLine 1279–1290, rootBook 1291, tops 1292, tierOrg/orig2 1299–1300, art.innerHTML 1301;
page_v36 `rowTitle` 436–442, `scopeOf`/`scopeBadge` 638–642, `rowHtml` 1146–1155 (the "· streams" rewrite and "Separate from").

## Goal
`groupHeadOf(card, rowIx, inp, opts)` returns everything above a group's tree: the title ("Trout and char"), the head kicker
("This water", "Region 6-wide", "Separate from your 4 trout and char", or null when it moved onto the share), the number line in
form **a** (default: "2 a day per angler from streams" with the 2 big, then "of the 4 a day per angler across Region 2, lakes included")
or form b, or the plain root line ("This lake’s rule: up to 2 a day per angler", the 2 big), and the origin block under it (origin words,
How to tell ›, the root's Rule book page).

## Files
- create `app/conformance/src/regs/grouphead.parity.test.ts` — the golden parity (conformance may import `@app/data/regs/node` and read golden.json; core may not import `@app/data` or `node:fs`). The core `*.test.ts` files named below hold **unit tests over small hand-built inputs only**.
- create `app/packages/core/src/regs/grouphead.ts`, `grouphead.test.ts`
- modify `words.ts`, `model.ts`

## Signatures
```ts
// grouphead.ts — ports page_v36 rowTitle, scopeOf, scopeBadge, rowHtml (scope part); prototype moveTag, relTier, bigWords, rootLine, rootBook, orig, orig2, tierOrg
import type { RegsInputs, CardModel, CardRow } from "./inputs";
import type { GroupHeadVM, OriginVM, Line } from "./model";
import type { TreeTier, GroupTotal } from "../answers.generated";

export function rowTitle(row: CardRow, inp: RegsInputs): string;                  // page_v36 rowTitle
export function scopeBadge(row: CardRow, card: CardModel, inp: RegsInputs): string | null;  // rowHtml's .rtitle .scope words (pre-moveTag)
export interface HeadCtx {                                                         // shared with T18
  readonly g: GroupTotal; readonly hdrRoot: TreeTier | null; readonly relTier: TreeTier | null;
  readonly tag: string | null; readonly moveTag: boolean; readonly tagShare: TreeTier | null;
  readonly rootBook: Line;                         // [] when none
  readonly rootPages: ReadonlySet<number>;         // printed pages the head shows (round 20b)
  readonly tierOrg: boolean; readonly howtell: boolean;
}
export function headCtx(row: CardRow, card: CardModel, inp: RegsInputs): HeadCtx | null;   // null: no group.headline (not a g28 group)
export function groupHeadOf(ctx: HeadCtx, opts: { tot: "a" | "b" }): GroupHeadVM;
export function originOf(ctx: HeadCtx): OriginVM | null;                           // the .ghd.gsub block (null when nothing to draw)
export function shortOf(t: TreeTier): string;                                       // re-exported from T28 when it lands; form b needs it
```
GroupHeadVM / OriginVM (supersede contracts.md §2.2):
```ts
export interface GroupHeadVM {
  readonly form: "a" | "b" | null; readonly rootTier: string | null; readonly nolimit: boolean;
  readonly lead: Line | null;        // form a: [text pre?, " "?, num(gln, "big"), " ", text post]
  readonly words: Line;              // the root's words, its n a num run ("big"; form a renders it small — T26 reads `form`)
  readonly ofTotal: boolean;         // form a: words are preceded by OF_THE
  readonly whyHere: string | null;   // cap1(root.why_here), not in form a
  readonly shareOf: Line | null;     // form b: [text "↳ "], shortOf(relTier) with its first number a num run
  readonly terms: readonly string[]; // the total's ⓘ — always [] in v33 (see Notes)
}
export interface OriginVM { readonly says: Line; readonly tag: string | null; readonly howtell: boolean; readonly book: Line }
```
Words: `OF_THE = "of the"`, `SHARE_ARROW = "↳ "`, `SCOPE_HERE = "This water"`, `SCOPE_WIDE = (s) => s + "-wide"`,
`SCOPE_ALL_STREAMS = (s) => "All " + s + " streams"`, `SEPARATE_FROM = (n, title) => "Separate from your " + n + " " + title`,
`HOW_TELL = "How to tell ›"`, `BC = "B.C."`, `THE_REGION = "the region"`, `THE_AREA = "the area"`, `PROTECTED = "Protected species"`, `TROUT = "Trout"`.

## Ports
1. **rowTitle** (page v36 436–442): pool with one species-group code in `GROUPS` (static `export.species` groups) → that group's
   name ("Trout and char"; "Trout" when `TROUT_CHAR` and `species_except` includes `CHAR`); else `Names(members, " and ")`;
   `prot` → "Protected species"; no members + a winner → cap(species name).
2. **scope badge** (page v36 `scopeOf` 638 + `scopeBadge` 640 + rowHtml 1152), in this order:
   - A0 (rowHtml 1149): a pooled row whose every member's main verdict gives role `lifted` to another pooled row's pool →
     `SEPARATE_FROM(A0.daily, lc(rowTitle(A0)))` ("Separate from your 4 trout and char", x25p_1010 steelhead).
   - else only for `pool && kind === "keep"`: `s = scopeOf(row)` (bc → "B.C."; region/area → entry name ← `inp.statics`/export
     entry names, else `pool.prov.entry_name`, else "the region"/"the area"; water → null). `s && scope.share && narrow` →
     `"${s} · ${narrow.export.fields.water}s"` (e.g. "Region 2 · streams"); `s` → `s + "-wide"`; else "This water".
   - **then rowHtml's rewrite as logic, not a regex**: the share form becomes `SCOPE_ALL_STREAMS(s)` when `nar0(row)`
     (`pool && scope.share && narrowAll(row)` — the narrow rule binds every member), else `SCOPE_WIDE(s)`.
     Golden kicker "All Region 1, excluding Haida Gwaii streams" (cow1_1010) shows `s` is the whole entry name.
3. **moveTag / tagShare** (1104–1106): `rt0 = g.tree[0]`; `moveTag = rt0.tier === "total" && !rt0.whose && / across /.test(rt0.says)
   && tag && !/-wide$/.test(tag)`; `tagShare` = first child tier of rt0 with `tier === "share"`. When moved, `GroupVM.scope = null`
   and T18 draws the tag as that share's kicker.
4. **hdrRoot / relTier** (1268, 1275): `hdrRoot = g.header?.tier === rt0.id ? rt0 : null`; `relTier = hdrRoot.tier === "total"
   && headline.n != null && headline.n !== hdrRoot.n ? first child tier with n === headline.n and tier share|own : null`.
5. **rootLine** (1279–1290):
   - form a (`relTier && tot === "a"`): `lead` = headline.says split at its first `\b${n}\b` → pre (nbsp, may be empty), num, post
     (nbsp); `words` = hdrRoot.says split at `\b${hdrRoot.n}\b` (no match → the whole says as one text run, `nolimit` false);
     `ofTotal = true`; no whyHere.
   - otherwise: `words` as above (no match → `nolimit = true`); `whyHere = cap1(hdrRoot.why_here)`; form b (`relTier && tot === "b"`):
     `shareOf = [SHARE_ARROW, …shortOf(relTier) with its first \b\d+\b as num "big2"]`.
   - no hdrRoot (legacy, no v33 state): prototype buildKeep `hm`/`num` 1116–1119 (headline + header.says split at " · ") — port it, mark untested.
6. **origin block** (1206–1213, 1291, 1299–1300): `obtn` = the How to tell link (page v36 `.gorigin` has it whenever `g.origin`;
   its hatchery_wild ⓘ is removed by prototype 1074); `ocut = g.origin.indexOf(" — ")`; `orig`: none when !g.origin; `g.header && !g.header.tag`
   → says = g.origin whole; `ocut > 0` → tag = `g.header?.tag || origin.slice(0, ocut)`, says = rest; else whole.
   `rootBook` = book runs of `rules.get(hdrRoot.by)`; `tierOrg = !g.origin && any tree tier has an origin` (the prototype tests the
   JSON for `"origin":"`); `orig2 = orig ? orig + " " + rootBook : (rootBook && !tierOrg ? rootBook alone : none)`.
   The gsub block is drawn when `orig2` (or, with no `g.header`, the total line). OriginVM: `{says, tag, howtell: !!g.origin, book: rootBook}`.
7. `rootPages` = printed pages of the root rule (T18 uses it for book-once-per-branch).

## Reads
`card.rows[i].{raw.group, pool, scope, narrow, kind, members, daily, prot, win}`, `GroupTotal.{headline, header, tree, origin, total_line}`,
`TreeTier.{id, tier, n, says, by, whose, why_here, children}`, `inp.rules.get(by).facts.book`, `inp.statics.exportSpecies` (groups), entry names.

## Tests
Golden parity lives in `app/conformance/src/regs/grouphead.parity.test.ts`: `for await (const { id, g, page } of cases("keep"))` with `requestOf` and `eqPlain` from
`app/conformance/src/regs/{golden,eq}.ts` (tests.md §1.2); the presenter under test is called on the case's inputs (until T21 lands the
harness hands the hydrated `RegsInputs` — T12 exposes `inputs()` on each case — after T21 the same assertions read `page().keep`).
Every numbered case that names a golden state id is a parity case there; cases marked **Unit** (and property tests over hand-built
`GroupTotal`/`StatusFrame` fixtures) stay in the core `*.test.ts`, which never loads golden.json.
Mapping: `title`; `scope`; `head.form`; `head.rootTier`; golden `head.lead` = `plain(lead)`; `leadN` = lead's num; `big` = words'
num; golden `ofTotal` = `ofTotal ? "of the " + plain(words) : null`; `whyHere`; golden `head.text` = `[plain(lead), ofTotal ? "of the" : null, plain(words), whyHere].filter(Boolean).join(" ")`
(contracts.md §1: separate elements are joined by one space — "2 a day per angler from streams of the 4 …"); `origin.lines` (`eqPlain`) and `origin.pages`.
1. vedder_1010 (form a): lead "2 a day per angler from streams", leadN "2", big "4", ofTotal "of the 4 a day per angler across
   Region 2, lakes included", scope null (moved; T18 kicker "All Region 2 streams"), origin null (tierOrg).
2. klwa_1010: form null, scope "This water", words "This lake’s rule: up to 2 a day per angler", big "2", origin lines ["Rule book p.37 ↗"] (pdf 39).
3. bc1_501 / bc0_501 (Bella Coola parts): form a, lead "2 a day per angler from streams", ofTotal "of the 5 a day per angler across
   Region 5, lakes included", origin ["Rule book p.42 ↗"].
4. glenn_501: form a, big "5", ofTotal "of the 5 a day per angler across Region 4, lakes included", origin ["Rule book p.34 ↗"].
5. ape_501: form null, scope "This water", text "The Bella Coola River’s rule: up to 1 a day per angler Its row covers its tributaries, so it applies here".
6. x25p_1010 r0: scope "Region 2-wide", text "4 a day per angler across Region 2"; r2: title "Steelhead", scope "Separate from your 4 trout and char",
   text "This lake’s rule: up to 6 a day per angler".
7. cow1_1010: title "Trout", form a, ofTotal "of the 4 a day per angler across Region 1, excluding Haida Gwaii, lakes included",
   origin lines ["Keep hatchery fish only — wild trout go back. How to tell › Rule book p.13 ↗"], howtell true.
8. **Property (r20h form a)**: for every group with `relTier`, `tot:"a"` → `plain(lead)` contains `headline.n` and the first num run
   of the whole head is `headline.n` (the stream's number leads); `tot:"b"` → the first num is `hdrRoot.n` and `shareOf` contains `relTier.n`.
9. **Property (r20e/r20f region-vs-stream)**: whenever `moveTag`, `GroupVM.scope === null` and the share tier's kicker (T18) is the moved tag;
   no head scope ends in " streams" while the head's words say " across ".

## Acceptance
`pnpm -C app check`; `vitest run conformance/src/regs/grouphead.parity packages/core/src/regs/grouphead` → 97/97 groups.

## Notes
- Contract: `GroupHeadVM.terms` (comment) applied to contracts.md (`// (T17)`). The field stays but is always `[]` in v33 — the prototype's `qtip` reads page v36's
  `ul.glifts` last item, which page v36 no longer draws when `group.tree` exists (`cardBlock` → `treeBlock`); golden `head.terms`
  is `[]` on 97/97 and `tier.terms` `[]` on 130/130. Keep the field for a future data change.
- `OriginVM.book` is the root's Rule book page and lives on the origin line (round 20f); when `tierOrg` it moves to the share tier's origin (T18).
- golden `meta.misses` lists `.gtag` and `[data-share-of]`: no v33 state draws an origin tag or form b — form b and the tag branch are
  covered by unit fixtures only (build a `GroupTotal` from vedder_1010's data with `tot:"b"`; expect shareOf "↳ 2 of them from streams"
  once T28's `shortOf` exists).
- **PIPELINE NEED (review 2026-10-21, owner rule):** `narrowAll` / `nar0` decide in TS whether the stored narrowing rule binds
  every fish of a row (rule species sets), and that answer picks the kicker ("All Region 2 streams" vs "Region 2-wide"). The store
  must carry it: `rows_row.scope.narrow_all: bool` (rows.py `scope_of`; `elsewhere_of` already computes
  `all(_covers(nw, S) for S in row["members"])`). `parentRow` (pool nesting from `xref` facts; decides which rows are drawn and
  suppresses "Separate from your N …") likewise needs `rows_row.parent: int | null`. Both stay as faithful page v36 ports, held by
  the parity, until a release carries the fields; then the presenter reads them and the TS derivations are deleted (grouphead.ts).
- Review 2026-10-21: golden `groups[].lines` (the whole group's drawn text in order) is compared by
  `conformance/src/regs/grouplines.parity.test.ts`; only the stamp lines (STAMP_LINE_IDS) and the `.gyear` line (YEAR_WINDOW_IDS,
  T19b) are dropped, each a keyed `lines` entry in exclusions.ts that the summary line reports.
