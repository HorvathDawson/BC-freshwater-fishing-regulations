# T38 — RN: `AdiposeArt` + the How to tell sheet

Status: DONE — e924a347, 75a8505c (D4: owner 2026-10-22, dark cut-outs with no tile are fine).

Phase 4 · Depends on T36 (`hatch` SheetVM, `adipose` block), T37 (SheetBody), T20 (image map / `apps/mobile` asset
registration) · Blocks T41 · Decisions D4 (no tile behind pictures), D15 (asset permissions — these two drawings are
ours, no third-party credit) · Size ½ day

## Goal
The "Hatchery or wild?" sheet shows the prototype's two rainbow-trout drawings — **Wild · adipose fin there** and
**Hatchery · clipped, a healed scar instead** — with the adipose fin ringed, then the plain line, then the glossary term
"Hatchery or wild (adipose fin)" with its closed "In the book’s words" fold and its pages. Every kind row's
"How to tell ›" and the origin line's How to tell open it (T26 already calls `onOpen("hatch")`).

## Decision: pre-rendered raster assets, not a react-native-svg port
| | react-native-svg port of `FISHART.draw('RB', …)` | **pre-rendered WebP (picked)** |
|---|---|---|
| colours | 12 hex literals (`#4e6b52 #c3cbc0 #e6e4dc #f4f1ea #fff #000 #e0708a #1a1a1a #d9c27a #111 #0e8fb3 #8b5a4a`), 2 gradients, a clipPath → every one would have to become a theme token (`check-colours` bans literals outside `packages/map/style/`), 12 new tokens for one illustration | none in source: the colours live in the image |
| size | 10.3 kB of SVG markup per drawing → 90 `<circle>`, 16 `<path>`, a `<clipPath>` per render; `react-native-svg` clip + gradient on Android is the slowest path | two ~15 kB WebP (800×300, RGBA) |
| fidelity | a hand port of `smooth()`/`rng()`; drift risk on every fishart change | byte-for-byte the prototype's drawing (shot from the same `fishart.js`) |
| precedent | — | the 39 fish pictures already ship as WebP in `apps/mobile/assets/fish/` through the T20 image map |
| deps | none (react-native-svg present) | none |
A raster cannot re-tint for dark mode — accepted: the fish pictures do not either (D4 a: RGBA cut-outs on the card, no
tile). An SVG **asset** file was rejected too: native needs `react-native-svg-transformer` (a new dep + metro config) and an
inline `SvgXml` string in a `.ts` file trips `check-colours`.

## Files
- create `app/tools/emit-adipose.mjs` — evaluates `app/design/regs-v10/fishart.js` with a stub `window`, calls
  `FISHART.draw('RB', {marks:false, adiMark:true})` and `{…, clipped:true}`, asserts each output is identical across two
  calls (the seeded `rng`), writes the two SVGs to a temp dir and rasterises them in headless Chrome (reuse
  `app/design/regs-v11/tools/cdp.mjs`'s `open`) at 800×300, transparent background → `app/apps/mobile/assets/fish/adipose-wild.webp`,
  `adipose-clipped.webp`; prints sha256 of both. Flag `--check`: re-render and compare hashes (not in `pnpm check`: needs Chrome).
- modify `app/apps/mobile/assets/fish/manifest.json`: add two entries `{file:"adipose-wild.webp", code:null, caption:"Wild · adipose fin there", phase:null, stand_in_for:null, credit:"drawn for this app (FISHART, page v36)", type:"Drawing", source:"this app", width:800, height:300}` and the clipped one — `code:null` keeps them out of `FISH` (T20 skips null codes) and in the credits/permissions list (T43)
- modify the T20 image map in `app/apps/mobile/src/fishImages.ts` (or wherever T20 registers `require()`s): export `ADIPOSE = { wild: require("../assets/fish/adipose-wild.webp"), clipped: require("../assets/fish/adipose-clipped.webp") }`
- create `app/packages/ui-native/src/regs/AdiposeArt.tsx`, `app/packages/ui-native/src/regs/HowToTell.test.tsx`
- modify `app/packages/ui-native/src/regs/SheetBody.tsx` (T37): `k:"adipose"` → `<AdiposeArt>`
- create `app/tools/emit-adipose.test.ts` (logic): only the pure part — the fishart evaluation is deterministic and the two
  SVG strings differ exactly by the clipped scar path + the aria-label suffix ", adipose fin clipped"

## Signatures
```ts
// AdiposeArt.tsx
import type { Line } from "@app/core";
export function AdiposeArt(p: {
  figures: readonly { variant: "wild" | "clipped"; caption: Line }[];   // T36: [{wild, "Wild · adipose fin there"}, {clipped, "Hatchery · clipped, a healed scar instead"}]
  images: { wild: ImageSourcePropType; clipped: ImageSourcePropType };
  palette: Palette;
}): JSX.Element;
// each figure: <View accessible accessibilityRole="image" accessibilityLabel={plain(caption)}>
//   <Text style={TYPE.meta …}><Text style={{fontWeight: "700"}}>Wild</Text> · adipose fin there</Text>    (figcaption first, as adiposeHtml)
//   <Image source resizeMode="contain" style={{ width: "100%", aspectRatio: 800 / 300 }} accessibilityIgnoresInvertColors />
// no backgroundColor on the figure or its parents (r20h-picture-surface)
```

## Ports
- page v36 `adiposeHtml` 717 (two `figure.fishart`, figcaption `<b>Wild</b> · adipose fin there` /
  `<b>Hatchery</b> · clipped, a healed scar instead`, then `p.small.muted` "Look on the back, just in front of the tail.").
- prototype `sheetHtml` hatch branch (@04765ab5, `sheetHtml` 1684+): `FISHART.draw('RB', {marks:false, adiMark:true,
  clipped: i++ === 1})` into each figure, then `section.htw[data-term=hatchery_wild]` = `<h3>` term title + `glossBody` 937.
- `app/design/regs-v10/fishart.js` `draw` (36–106): `clipped` adds the scar path (#8b5a4a), `adiMark` the ring (#0e8fb3).

## Reads
`SheetVM` `hatch` (T36): `title "Hatchery or wild?"`, `sub "The adipose fin"`, blocks `adipose`, `para`, `term(hatchery_wild)`.

## Tests
**C — `HowToTell.test.tsx`** (golden `sheets.hatch` @ 04765ab5):
1. `two figures, captions first` — mount `SheetHost` with `open="hatch"` → two `role="image"` elements labelled
   `"Wild · adipose fin there"` and `"Hatchery · clipped, a healed scar instead"`, in that order; each caption Text precedes
   its Image in the tree.
2. `the sheet's lines` — whitespace-normalised text, in order, contains golden lines 0–4:
   `"Wild · adipose fin there"`, `"Hatchery · clipped, a healed scar instead"`, `"Look on the back, just in front of the tail. A small fin there means wild. Only a scar means hatchery. If you can’t tell, treat the fish as wild."`,
   `"Hatchery or wild (adipose fin)"`, `"Hatchery trout and steelhead are marked before release by removing the adipose fin"` (prefix).
3. `term fold closed` — "In the book’s words" `aria-expanded="false"`; the quote `"hatchery trout: in some waters, hatchery trout may be harvested"` (prefix) absent until pressed.
4. `pages` — `"Rule book"` followed by `"p.13 ↗"` and `"p.80 ↗"` links (golden pages `[{p.13 ↗, pdf 15}, {p.80 ↗, pdf 86}]`).
5. `no tile` — neither Image nor its two ancestors carry a `backgroundColor` style.
6. `How to tell opens it` — on the Vedder Keep tab (vm fixture), pressing the ST kind's `"How to tell ›"` calls
   `onOpen("hatch")` (`ur3-howtell-steelhead`, `howtell-has-hatchery`).
**A — `emit-adipose.test.ts`**: `draw()` twice → equal strings; wild vs clipped differ only by one `<path … stroke="#8b5a4a"…>`
and `", adipose fin clipped"` (pins that a fishart change is noticed — rerun the emitter).

## Acceptance
```sh
node app/tools/emit-adipose.mjs            # writes the two WebPs (Chrome), prints sha256
pnpm -C app vitest run packages/ui-native/src/regs/HowToTell tools/emit-adipose
pnpm -C app check                          # colours gate: no literal added (the hexes stay in app/design/, which is not scanned)
# D: node app/tools/compare-regs.mjs --state vedder_1010 --sheet hatch   (side by side, light + dark)
```

## Notes
- `app/design/` is outside `check-colours`' ROOTS (`packages apps tools conformance`), so reading fishart.js from the
  emitter is fine; the emitter itself (`app/tools/emit-adipose.mjs`) must not contain a colour literal — it only calls `draw`.
- The drawings carry their own light ground (body gradient `#f4f1ea`); on the dark card they read as cut-outs like the
  photos (D4). T39's dark shoot includes the hatch sheet; if the owner rejects it, the emitter gains `--dark` and a second
  pair, chosen by `themeName` in `SheetHost` — no core change.
- Credits: the two drawings are listed in Credits (T43) as "drawn for this app" — no permission row needed, but they still
  get a `fish_credits.tsv` row with status `own` so the ship gate sees every shipped image.
