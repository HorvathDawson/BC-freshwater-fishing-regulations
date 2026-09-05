# Colour, and how we know it works

*What changed in the palette, why, and what now holds it in place.*

## The problem with a colour-blind theme

The app has offered a `cvd` setting for a while. Nobody here can tell by looking whether it
works — that is the whole point of the setting, and it is also why it was broken. A palette
aimed at readers you are not is the one kind of design you cannot review by opening it.

So it is measured now. [`tools/cvd.ts`](../tools/cvd.ts) simulates the three dichromacies
using **Viénot–Brettel–Mollon (1999)** — the model behind the standard accessibility tools —
by converting to LMS (cone response), projecting onto the plane the missing cone leaves
behind, and converting back. Separations are measured in **ΔE2000**, not RGB distance,
because two hexes far apart in RGB can be perceptually identical and a check that cannot
tell the difference is theatre. [`tools/cvd.test.ts`](../tools/cvd.test.ts) holds the
palette to it on every run.

## What the first run found

All of it was shipping. None of it was visible to us.

| | was | |
|---|---|---|
| `accent` vs `status.unknown` | **the same hex** (`#5F26E0`) | "You are here" and "no rule found" were one colour, ΔE 0.0 |
| `restricted` | `#D98C00`, 2.73:1 as pill text | a plain WCAG AA failure |
| stocking ramp | three greens | ΔE 2.4 apart under protanopia — a *ramp* nobody could order |
| `highlight` vs mapped water | ΔE 3.5 under protanopia | tapping a river did nothing a protanope could see |

The cause was one line: `CVD = { ...LIGHT, ...outcomes("cvd") }`. It swapped four colours and
inherited everything else, so every ramp added to the palette afterwards silently joined the
colour-blind theme wearing the light theme's hues.

Measuring the colour-blind theme also turned up **WCAG failures in the default themes** —
`faint` at 2.65:1 (light) and 3.86:1 (dark), `closed` at 4.47:1 and 4.20:1, `open` at 3.62:1,
`quiet` at 1.68:1. Those affect every reader. The colour-blind audit was simply the first
thing that ever looked.

## Two findings worth keeping

**Use the published schemes.** Hand-picking hexes and then writing a test for them proves
nothing — the same judgement wrote both. The categorical hues are anchored on **Okabe & Ito's
Color Universal Design** set and the sequential ramp is **cividis**, which exists precisely to
stay monotonic under dichromacy.

**Equal contrast destroys a palette.** Okabe–Ito's safety is carried by *lightness* as much as
hue. Forcing all eight colours to the same contrast ratio — that is, the same lightness —
collapses the set from ΔE 11.1 to **1.1**. So "use the standard palette" cannot mean pasting
its hexes in as coloured text.

That matters here because the status pill draws its outcome **as text** (11.5px bold — not
"large text", so the relaxed 3:1 does not apply). WCAG's 4.5:1 caps every outcome colour's
lightness, and separation then has to come from spreading them *down* through the dark half of
the range. That is why the colour-blind status set reads darker and more muted than the default
one. It is a consequence of the design target's tinted-text chip meeting a real contrast floor,
not a preference.

The relief valve is that **a fill is not text**. The donor identity hues are a pin, a swatch, a
stripe, a dot and a bar — never text — so they only owe WCAG 1.4.11's 3:1, which frees the
lightness range and lets them be vivid.

## The floors, and why they differ

Red/green deficiency (protan, deutan) affects roughly 8% of men; tritan roughly 0.01% of
people. A palette that serves both perfectly does not exist — the two pull in opposite
directions on the hue circle — so red/green carries the higher floor and tritan a real but
lower one. Both are asserted; neither is silently dropped.

## Colour is never the only channel

Nothing above makes colour carry an answer alone, and it must not. Every outcome is drawn with
a **word** (`statusWord`) and, on the map, a **dash** (`outcomeDash`). The thresholds make
colour a good *second* channel. This is also why the "measured here, but no baseline to compare
against" state is a **dashed line** rather than a hue: it is a state, not a position on the
scale, and a sequential ramp must not carry one.

## Centralising

Three colours were living outside the palette and have been brought in:

- the **"you are here" marker** was `#5F26E0` typed into `Map.web.tsx` — the *light* theme's
  accent, worn in every theme. It reads `MapChrome.accent` now.
- **`LayersSheet`** typed in three legend swatches, so the colour-blind legend described a map
  painted differently. They come from the map theme.
- the **satellite swatch** stays hardcoded on purpose, and says so: it previews an imagery
  raster, and aerial photography is dark water and forest whatever theme the app wears.

## Running it

```
pnpm vitest run tools/cvd.test.ts      # the palette
pnpm run style                          # rebuild + validate the map themes
```

`tools/check-style.mjs` independently holds map tokens apart in ΔE — it rejected one of the
candidate colours here for sitting 4.2 from `color.mask`.
