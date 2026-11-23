/**
 * (T940, owner 2026-11-23: "Build as proposed (Recommended)") THE SATELLITE SETS ARE MEASURED, not trusted.
 *
 * Over satellite imagery each theme paints the water statuses in `color.imagery.status.*`. Two gates, on the tokens as built:
 *   VISIBLE   every colour of every theme's set reaches 3:1 (WCAG 1.4.11) against the dark satellite grounds
 *             (`color.ui.imagery.water`, `.forest`: aerial photography is dark water and forest whatever the theme);
 *   SEPARATE  under cvd, every pair a view draws together is at least 20 apart (CIEDE2000) for the worst reader (Viénot and Machado,
 *             as tools/cvd.ts measures; the repo's floor). "Drawn together" is DERIVED from the colour modes (`STYLE_META.colorModes`:
 *             the statuses one water layer paints in one mode, mapped back to their status) plus the one thing the key adds
 *             (Salmon's key lists No access beside its classes), never listed by hand: a new mode that draws two statuses together
 *             puts that pair under the gate. (Today keep and release are Salmon-only, base is Standard-only: they never meet.)
 *   BOUND     the 3:1 gate measures the raw token because that IS what a line paints: `opacity.imagery.edge` is 1 (asserted), so a
 *             stream's edge and a lake's outline are drawn at full strength over the photograph; the fills fade to
 *             `opacity.imagery.fill` by design and never carry a status alone (the outline does).
 * The light and dark set is held by the visible gate and by map-contrast (key swatches); it is not a colour-blind theme.
 */
import { describe, expect, it } from "vitest";
import { resolveTheme } from "../packages/map/src/style";
import { contrast, worstSeparation } from "./cvd";

import { describe, expect, it } from "vitest";
import { resolveTheme, STYLE_META } from "../packages/map/src/style";
import { contrast, worstSeparation } from "./cvd";

const KEYS = ["open", "restricted", "closed", "own", "base", "no_access"] as const;
type Key = (typeof KEYS)[number];
/** a status or salmon colour token -> the status it is (Salmon keep is open, release is restricted, check is own) */
const STATUS_OF: Record<string, Key> = {
  "color.status.open": "open", "color.status.restricted": "restricted", "color.status.closed": "closed", "color.status.own": "own",
  "color.status.base": "base", "color.status.no_access": "no_access",
  "color.salmon.open": "open", "color.salmon.open-area": "open", "color.salmon.release": "restricted", "color.salmon.closed": "closed",
  "color.salmon.check": "own",
};
type Modes = Record<string, Record<string, unknown>>;
const tokensIn = (x: unknown, out: Set<string>): Set<string> => {
  if (Array.isArray(x)) x.forEach((v) => tokensIn(v, out));
  else if (x && typeof x === "object") {
    const t = (x as { token?: unknown }).token;
    if (typeof t === "string") out.add(t);
    Object.values(x).forEach((v) => tokensIn(v, out));
  }
  return out;
};
/** The pairs of statuses some water layer paints in one mode, plus No access beside Salmon's classes (the Salmon key lists it). */
function togetherPairs(modes: Modes): Set<string> {
  const pairs = new Set<string>();
  const add = (ks: Key[]) => { for (const a of ks) for (const b of ks) if (KEYS.indexOf(a) < KEYS.indexOf(b)) pairs.add(`${a}/${b}`); };
  for (const layerModes of Object.values(modes))
    for (const [mode, def] of Object.entries(layerModes)) {
      const ks = [...new Set([...tokensIn(def, new Set())].map((t) => STATUS_OF[t]).filter((k): k is Key => !!k))];
      add(mode === "salmon" ? [...new Set([...ks, "no_access" as Key])] : ks);
    }
  return pairs;
}
const FLOOR = 20;
const colourOf = (t: string, k: string) => String((resolveTheme(t) as Record<string, string | number | number[]>)[`color.imagery.status.${k}`]);
/** The drawn-together pairs of `modes` under the worst-reader floor, in cvd's own set. */
function lowPairs(modes: Modes): string[] {
  const low: string[] = [];
  for (const pair of [...togetherPairs(modes)].sort()) {
    const [a, b] = pair.split("/") as [Key, Key];
    const d = worstSeparation(colourOf("cvd", a), colourOf("cvd", b), ["vienot", "machado"]).dE;
    if (d < FLOOR) low.push(`${pair} ${d.toFixed(1)}`);
  }
  return low;
}

describe("T940 the satellite sets are measured", () => {
  /**
   * RECORDED UNDER 3:1 (the approved light / dark set, owner 2026-11-22): closed #FF3B6D on `color.ui.imagery.forest` (the preview
   * swatch's mid forest green) is 2.26:1; on the water ground it is well over. Listed whole: a new one fails, one lifted to 3:1
   * fails until it is taken off, and none may slip under its recorded ratio. cvd has none (its set was tuned to this gate).
   */
  const UNDER: Record<string, number> = { "light closed forest": 2.25, "dark closed forest": 2.25 };
  it("a line is drawn at full strength over imagery (so the raw token is what the 3:1 gate must measure)", () => {
    for (const th of ["light", "dark", "cvd"]) expect([th, (resolveTheme(th) as Record<string, number>)["opacity.imagery.edge"]]).toEqual([th, 1]);
  });
  for (const th of ["light", "dark", "cvd"] as const)
    it(`every colour reaches 3:1 on the satellite grounds (water, forest), or is a recorded exception that has not slipped — ${th}`, () => {
      const t = resolveTheme(th) as Record<string, string>;
      const grounds: [string, string][] = [["water", t["color.ui.imagery.water"]!], ["forest", t["color.ui.imagery.forest"]!]];
      const under: string[] = [], slipped: string[] = [];
      for (const k of KEYS) for (const [g, hex] of grounds) {
        const r = contrast(colourOf(th, k), hex);
        if (r >= 3) continue;
        const key = `${th} ${k} ${g}`;
        under.push(key);
        if (UNDER[key] !== undefined && r < UNDER[key]!) slipped.push(`${key} ${r.toFixed(2)} < recorded ${UNDER[key]}`);
      }
      expect(slipped).toEqual([]);
      expect(under).toEqual(Object.keys(UNDER).filter((k) => k.startsWith(th + " ")));
    });

  it(`cvd: every pair the modes draw together is at least ${FLOOR} apart for the worst reader`, () => {
    expect(lowPairs(STYLE_META.colorModes as unknown as Modes)).toEqual([]);
  });

  it("the derivation sees today's pairs: Standard's four, Salmon's four with No access; never keep/base or release/base", () => {
    const got = togetherPairs(STYLE_META.colorModes as unknown as Modes);
    for (const p of ["closed/own", "closed/base", "own/base", "closed/no_access", "own/no_access", "base/no_access", "open/restricted", "open/closed", "open/own", "open/no_access"])
      expect(got.has(p), p).toBe(true);
    expect(got.has("open/base")).toBe(false);
    expect(got.has("restricted/base")).toBe(false);
  });

  it("MUTATION: a mode that draws release beside base (0.7 apart under cvd) puts that pair under the gate and fails it", () => {
    const real = STYLE_META.colorModes as unknown as Modes;
    const fake: Modes = { ...real, stream: { ...real.stream, fake: { a: { token: "color.salmon.release" }, b: { token: "color.status.base" } } } };
    expect(lowPairs(fake).some((l) => l.startsWith("restricted/base"))).toBe(true);
  });
});
