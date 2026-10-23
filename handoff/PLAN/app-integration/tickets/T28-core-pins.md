# T28 — core: `shortOf`, `pinChain`, `tabInView` + golden pin fixtures

Status: DONE — 14b90015.

Phase 2 · Depends on T00 (core `regs/` skeleton), T12 (golden loader), T17/T18 (the VM fields this fills: `TierVM.short`,
`GroupVM.pinLine`) · Blocks T29 · Decisions D12 (a) · Size ½ day

Target **regs v33 @ 04765ab5**. Prototype refs @04765ab5: `shortOf` **1628–1635**, `pinUpdate` **1637–1655**, `mini`
**1656–1664**, tab press **1904–1905**, `lc1w` 1627. Reference implementation: `app/design/regs-v11/port/pinreplay.mjs`
(committed) — `pinChain` there is what this ticket transcribes; `node app/design/regs-v11/port/pinreplay.mjs` prints
`pin replay: 134 / 134 states reproduce every golden pin transition (25 carry the prototype's pinUpdate error)`.

## Goal
Pure functions that decide the pinned stack under the header ("Trout and char · 2 a day from streams" / "(of 4 across Region
2)" / "↳ of those, 1 over 50 cm (2 if one is a hatchery steelhead)") from measured anchors and a scroll offset, the short line
of every tier, and which header tab is "in view". No React. The RN ticket (T29) only measures and draws.

## Files
- create `app/packages/core/src/regs/pins.ts`
- create `app/packages/core/src/regs/pins.test.ts`
- modify `app/packages/core/src/regs/model.ts`: `TierVM.short: string` (exists in contracts — filled here), add
  `GroupVM.pinLine: PinLine | null`
- modify the T17/T18 presenters (`group.ts`/`tree.ts`, whatever T17/T18 named them) to fill `short = shortOf(rawTier)` and
  `pinLine = groupLine(title, root, headline, form === "a")`
- modify `app/packages/core/src/regs/words.ts`: the connecting words below (constants, not literals in pins.ts)
- modify `app/packages/core/src/index.ts`: export `pinChain, tabInView, anchoredScrollY, boldFirst, PIN_LH, PIN_DROP` and the types

## Signatures
```ts
// pins.ts — ports prototype shortOf / pinUpdate / mini (@04765ab5) via port/pinreplay.mjs
export const PIN_LH = 26;      // pinUpdate LH: one pinned line's allowance
export const PIN_DROP = 110;   // round 20f: a tier's line goes once less than ~a row of its section is left under the bar

/** The raw tier fields shortOf reads (answers TreeTier + the folded kind's says). */
export interface ShortTier {
  readonly tier: "total" | "own" | "share" | "count" | "size" | string;
  readonly n: number | null; readonly says: string | null; readonly whose?: string | null;
  readonly replaces?: boolean | null; readonly over_cm?: number | null;
  readonly children?: readonly ({ node: "kind"; folded?: boolean; says?: string | null } | { node: "tier" })[];
}
export function shortOf(t: ShortTier): string;                         // VERBATIM port of 1628–1635 (+ lc1w 1627)
export interface PinLine { readonly cls: "g" | "g two" | "c"; readonly text: string; readonly sub: string | null }
/** The group line (pinUpdate 1643): form a → share words + "(of …)" sub; else "<name> · <shortOf(root)>". */
export function groupLine(name: string, root: ShortTier, headline: { n: number | null; says: string } | null, formA: boolean): PinLine;

/** = golden pins.anchors[] (golden.mjs, v33). Coordinates are CONTENT offsets at scroll 0 of the scroll view. */
export interface PinBox { readonly top: number; readonly height: number }
export interface PinTierAnchor {
  readonly tid: string; readonly tier: string; readonly n: number | null;
  readonly top: number; readonly height: number;     // the whole section (section.tnode)
  readonly head: PinBox | null;                      // its header (.tier)
  readonly nextHead: number | null;                  // top of the NEXT section's head as pinUpdate finds it (1650), null if none
  readonly short: string;                            // "↳ " + shortOf(tier)
}
export interface PinAnchor {
  readonly row: string;                              // "r0"
  readonly group: boolean; readonly tree: boolean;   // false/false: a ledger article without a group (the prototype throws)
  readonly formA: boolean; readonly headlineN: number | null;
  readonly top: number; readonly height: number;     // the article
  readonly groupLine: PinLine | null;
  readonly pin: PinBox | null;                       // the head block (.ghd.gpin)
  readonly tiers: readonly PinTierAnchor[];          // document order (outer before inner)
  readonly worked: PinBox | null;
}
export interface PinViewport {
  readonly header: number;   // where the bar's top line sits, in the same coordinates as `top`: prototype = header height (119); app = 0
  readonly height: number;   // the scroll area's visible height measured from the same origin (prototype 703 = scroller incl. header; app = ScrollView height)
  readonly LH: number; readonly DROP: number;
}
export interface PinResult { readonly lines: readonly PinLine[]; readonly skip: boolean }
/** pinUpdate as a function. skip = the prototype would throw here (group-less article) and keep its previous lines. */
export function pinChain(anchors: readonly PinAnchor[], scrollY: number, vp: PinViewport): PinResult;
/** What the app shows: the ONE intentional divergence — a group-less article shows NO lines (the prototype keeps stale ones). */
export function appPinLines(r: PinResult): readonly PinLine[];   // r.skip ? [] : r.lines

/** The bold number of a pin line: the first \b\d+\b of `s` (pinUpdate 1653 bolds text's first and sub's first separately). */
export function boldFirst(s: string): { pre: string; n: string; post: string } | null;

/** mini() 1656–1664: the tab whose section has reached the bar. `tops` in content offsets (app: header excluded); a missing
 *  section is null; `methodsHeight` 0 counts as absent (m.offsetHeight). */
export function tabInView(tops: { gear: number | null; methods: number | null; methodsHeight: number; licence: number | null },
                          scrollY: number, viewportH: number, contentH: number): Tab;
/** v33 r20i scroll margins: an anchored scroll lands with the target under the pin stack, not beneath it. */
export function anchoredScrollY(targetTop: number, pinStackHeight: number): number;   // max(0, targetTop - pinStackHeight - 8)
/** tab press (1904): keep → 0; else the section's top + 2 (prototype: offsetTop - header + 2; app coords exclude the header). */
export function tabScrollY(tab: Tab, tops: { gear: number | null; methods: number | null; licence: number | null }): number;
```

## Ports (transcribe exactly; `port/pinreplay.mjs` is the executable spec)
- **`shortOf` 1628–1635**, branch for branch:
  - `total`: `n == null` → `says`; `m = says.match(/ across (.+?)(?:, (?:lakes|streams) included)?$/)` → `` `${n} a day across ${m[1]}` ``;
    else `whose` → `` `${whose}: ${n} a day` ``; else `` `${n} a day` ``.
  - `share`: `w = (says.match(/from (this stream|this lake|streams|lakes)/) || [])[1] || 'streams'`; `/^Only /` →
    `` `${n} of them from ${w}` `` else `` `${n} a day from ${w}` ``.
  - `own`: `k` = first child kind with `folded`; `names = k ? (k.says.match(/ may be (.+?)(?: only —.*)?$/) || [])[1] || '' : ''`;
    `whose = lc1w(t.whose || '')`; `replaces` → `` `plus up to ${n}${names ? ' ' + names : ''} under ${whose}` `` else
    `` `${n} of them${names ? ` (${names})` : ''} under ${whose}` ``.
  - `count`: `` `of those, ${says.replace(/ \(part of the \d+\)$/, '')}` ``.
  - `size`: `cut = says.indexOf(' — ')`, `tail = cut > 0 ? says.slice(cut + 3) : ''` → `` `of those, ${n == null ? '' : n + ' '}over ${over_cm} cm${tail ? ` (${tail})` : ''}` ``.
  - else `says`. The connecting words ("a day across", "of them from", "plus up to", "under", "of those,", "over", "cm") go to
    `words.ts` as template functions (`PIN.total(n, where)` etc.) — the regexes match DATA words and stay in pins.ts.
- **`groupLine`** = 1643: form a and headline → `{cls:"g two", text: `${name} · ${shortOf({tier:"share", n: hl.n, says: hl.says})}`,
  sub: `(of ${shortOf(root).replace(/ a day /, " ")})`}`; else `{cls:"g", text: `${name} · ${shortOf(root)}`, sub: null}`.
- **`pinChain`** = pinreplay.mjs `pinChain` (from pinUpdate 1637–1655), with `top(e) = e.top - scrollY`:
  1. `art` = first anchor with `top(a) < header && top(a) + a.height > header + LH`; none → `{lines: [], skip: false}`.
  2. `!art.group` → `{lines: [], skip: true}` (prototype: `g.headline` on null throws).
  3. `!(art.tree && art.pin && top(art.pin) < header && art.groupLine)` → `{[], false}`.
  4. push `groupLine`; for each tier in order: `nextIn = t.nextHead != null && t.nextHead - scrollY < height - LH` (20h); skip
     `formA && t.tier === "share" && t.n === headlineN` (20h form a: the group line already says the share); push
     `{cls:"c", text: t.short, sub: null}` iff `t.short && t.head && top(t) < header && top(t) + t.height > header + LH + DROP &&
     !nextIn && top(t.head) < header + lines.length * LH` (note `lines.length` grows as lines are pushed).
- **`tabInView`** = `mini` 1656–1664 in app coordinates: `reached(top) = top != null && top - scrollY <= 12`; `atEnd = scrollY +
  viewportH >= contentH - 2 && scrollY > 0`; no gear section → "keep"; licence if `reached(licence) || atEnd` (licence present);
  methods if present, `methodsHeight > 0` and `(reached(methods) || (licence == null && atEnd))`; gear if `reached(gear)`; else keep.

## Reads
Golden `states[id].pins` = `{error?, viewport {width:390, height:703, header:119, LH:26, DROP:110}, anchors: PinAnchor[], samples:
[{y, lines:[{cls, text, sub, big}], height}]}` — samples are recorded only when the bar changes, at 8 px steps from 0 to the end
of the Keep tab.

## Tests (`pins.test.ts`, project `logic`; golden through `testing/golden.ts` — golden.json needs no store, so these never skip)
1. **Golden replay — every state** (the pinreplay loop): for each of the 134 states, step `y = 0, 8, … , lastSample.y + 3200`,
   keep `cur` (`r.skip ? cur : r.lines`), record a transition whenever `cur` changes; the transitions with `y ≤ lastSample.y`
   must equal `samples` (`[cls, text, sub]` per line). **Expected: 134 / 134** (25 states carry `pins.error`; the skip flag
   reproduces the prototype's keep-previous behaviour on them). This is the same count `node app/design/regs-v11/port/pinreplay.mjs`
   prints; if a version bump changes it, `pinUpdate` changed — port the delta.
2. **Big numbers**: for every sample line, `boldFirst(text)?.n ?? boldFirst(sub)?.n ?? null` equals golden `big`
   (e.g. "Trout and char · 2 a day from streams" + "(of 4 across Region 2)" → "2"). Expected: all 316 sample lines (v33).
3. **Divergence pinned**: for each of the 25 states with `pins.error`, the first `y` (8 px steps from 0) where
   `pinChain(...).skip` is true equals `pins.error.y` (cow1/cow2/cow3_1010 → 1336, klmb_1010 → 944, iron_720 → 1440,
   x5p1_1010 → 128, x37p2_1010 → 768, …), and `appPinLines(r)` is `[]` there; no state without `pins.error` skips at any `y ≤` its last sample.
4. **shortOf unit** (literal tiers, the strings golden anchors carry for vedder_1010 / klwa_1010 / glenn_501 / bc1_501):
   - `{tier:"total", n:4, says:"4 a day per angler across Region 2, lakes included"}` → `"4 a day across Region 2"`
   - `{tier:"total", n:2, says:"This lake’s rule: up to 2 a day per angler", whose:"This lake’s rule"}` → `"This lake’s rule: 2 a day"`
   - `{tier:"share", n:2, says:"Only 2 of the 4 may be from streams, not counting hatchery rainbow trout"}` → `"2 of them from streams"`
   - `{tier:"share", n:2, says:"2 a day per angler from streams"}` → `"2 a day from streams"`
   - `{tier:"size", n:1, over_cm:50, says:"Only 1 of the fish in this box may be over 50 cm (20 in) — 2 if one is a hatchery steelhead"}`
     → `"of those, 1 over 50 cm (2 if one is a hatchery steelhead)"`
   - `{tier:"own", n:4, replaces:true, whose:"This river’s rule", children:[{node:"kind", folded:true, says:"Up to 4 may be hatchery rainbow trout"}]}`
     → `"plus up to 4 hatchery rainbow trout under this river’s rule"` (check the folded kind's real `says` in the store; the
     expected output is golden vedder_1010 tier t2 `short` minus "↳ ")
   - `groupLine("Trout and char", total4Region2, {n:2, says:"2 a day per angler from streams"}, true)` →
     `{cls:"g two", text:"Trout and char · 2 a day from streams", sub:"(of 4 across Region 2)"}`;
     with `formA=false` → `{cls:"g", text:"Trout and char · 4 a day across Region 2", sub:null}`.
5. **shortOf parity over the store** (skipIf no province store, T12 harness): for every state and every golden anchor tier,
   `"↳ " + shortOf(rawTier(tid))` = anchor `short`; for every anchor with `groupLine`, `groupLine(title, root, headline, formA)` =
   anchor `groupLine`. Also `buildRegsPage(...).keep.groups[i].tree[*].short` and `.pinLine` agree (the VM carries what the bar draws).
6. **tabInView unit**: tops `{gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700}`, viewport 600, content 3300:
   y 0 → keep; 1488 → gear (1500−1488 = 12); 1487 → keep; 2095 → methods; 2690 → licence; y 2700 (atEnd: 2700+600 ≥ 3298) → licence;
   no licence and atEnd → methods; `methodsHeight: 0` never → methods.
7. **anchoredScrollY / tabScrollY**: `(1000, 65) → 927`; `(30, 65) → 0`; `tabScrollY("keep") → 0`; `tabScrollY("gear", {gear:1500}) → 1502`.

## Acceptance
```sh
node app/design/regs-v11/port/pinreplay.mjs            # the reference: 134 / 134
pnpm -C app exec vitest run packages/core/src/regs/pins # 134/134 replay + units
pnpm -C app check
```

## Notes
- The app's coordinates differ from the golden's only by the header: the golden is measured in the prototype's scroller (header
  inside, sticky), so `viewport.header = 119` and `height = 703`; the app's ScrollView sits under a fixed header, so T29 passes
  `header: 0` and the ScrollView's own height. The functions take the viewport, never a constant.
- `nextHead` is part of the anchor (not derived from the tree) because the prototype's `nextElementSibling` walk depends on DOM
  order (a kind row after a size band means "no next head"). T29 computes it from the VM tree: for tier X, the next sibling node
  in its parent's `children`; if that node is a tier → its head top; if it is a kind/release row → null; if X is the last child
  → the parent tier's next sibling by the same rule (one level only, as `closest('section.tnode').nextElementSibling`); a
  top-level tier's parent is the group → null.
- Do not "improve" the form-a skip to compare strings; it compares `n` exactly as the prototype (`t.n === hl?.n`).
- `shortOf` lives in core because the VM must carry it (contracts `TierVM.short`); the pin bar never builds words.
