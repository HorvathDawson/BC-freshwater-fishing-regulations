/**
 * ONE ICON SET (ticket N1; owner 2026-10-22, decisions.md "icons: Lucide, rounded 1.75"). The app's chrome glyphs — the
 * bottom bar, the map rail, the top bar's search / back / clear, every back and close — are drawn by ONE component
 * (`packages/ui-native/src/icons.tsx` `<Icon>`) from ONE table of Lucide paths. Before N1 they were drawn three ways
 * (two path tables, an inline Svg in RegsHeader, Views for the hamburger) plus typed unicode glyphs, in three sizes.
 *
 *   svg          react-native-svg is imported by icons.tsx and by the DATA DRAWINGS named below, nothing else: a new
 *                bar, rail or header glyph cannot be hand-drawn beside the table
 *   old icons    LayersIcon, MenuIcon, CalendarIcon and RegsHeader's GLYPH paths are gone and stay gone
 *   glyph text   no button draws a typed ‹ ✕ × ← as its glyph (the set has back and close); a lone › outside the
 *                regs page (whose rows keep it as text, the same mark as its inline "Source ›" links) is refused too
 *   drawn marks  no chevron built from a View's two borders and a rotate (the folds' marks are expand / collapse)
 *   the table    every icon is used somewhere; the version is recorded once (`@app/core` ICON_SET) and the licence
 *                file and the credit say the same (each icon's drawing: packages/ui-native/src/icons.test.tsx)
 *
 * The scan is a net, not a proof: how the bar and rail look is in the shots (handoff/PLAN/app-integration/icons-shots/).
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { ICON_CREDIT, ICON_SET } from "@app/core";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["apps/mobile/src", "packages/ui/src", "packages/ui-native/src", "packages/map/src"];
const ICONS_FILE = "packages/ui-native/src/icons.tsx";

/** The data drawings: pictures of data, not chrome. Each may draw its own svg. */
const DRAWINGS: Record<string, string> = {
  "packages/ui-native/src/Hydrograph.tsx": "the flow chart",
  "packages/ui-native/src/DonorMap.tsx": "the route illustration under an estimate (gauges, reaches, the water)",
  "packages/ui-native/src/Chrome.tsx": "the Conditions legend's ramp: the flow standing gradient (data, not a glyph)",
  "packages/ui-native/src/ConditionsPanel.tsx": "a forecast run's days as a small line, its peak marked (T52: data)",
  "packages/ui-native/src/regs/YearStrip.tsx": "the regs year bar (open / closed spans, a hatch)",
  "packages/ui-native/src/regs/KeepBlocks.tsx": "a fish's silhouette where it has no picture",
};

function sources(dir: string, out: string[] = []): string[] {
  let names: string[];
  try { names = readdirSync(dir); } catch { return out; }
  for (const f of names) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".") || f.startsWith("_")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx)$/.test(f) && !/\.test\.tsx?$/.test(f) && !/\.d\.ts$/.test(f)) out.push(p);
  }
  return out;
}
/** Comments out (a doc comment may name what the code may not use); strings kept. */
const code = (src: string) => src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\{\/\*[\s\S]*?\*\/\}/g, "")
  .replace(/(^|[^:"'`])\/\/.*$/gm, "$1");
const files = () => ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p)).sort();
const read = (f: string) => code(readFileSync(join(APP, f), "utf8"));

describe("one icon set (N1)", () => {
  it("react-native-svg is drawn by the icon table and the named data drawings only", () => {
    const users = files().filter((f) => /from\s+["']react-native-svg["']/.test(read(f)));
    expect(users.filter((f) => f !== ICONS_FILE && !(f in DRAWINGS))).toEqual([]);
    // every allowance is still needed (a drawing that stopped drawing leaves the list)
    expect(Object.keys(DRAWINGS).filter((f) => !users.includes(f))).toEqual([]);
  });

  it("no raw <svg> or svg path data in chrome: a path string lives in the icon table or a data drawing", () => {
    const pathData = /["'`]\s*[Mm]\s*-?\d[\d.]*[\s,]+-?\d[\d.]*\s*(?:[LlHhVvCcSsQqTtAaZz]|[\s,]+-?\d)/;
    const bad = files().filter((f) => f !== ICONS_FILE && !(f in DRAWINGS))
      .filter((f) => /<svg[\s>]/.test(read(f)) || pathData.test(read(f)));
    expect(bad).toEqual([]);
  });

  it("the old hand-drawn icons are gone", () => {
    const bad = files().flatMap((f) => {
      const src = read(f);
      return ["LayersIcon", "MenuIcon", "CalendarIcon"].filter((n) => new RegExp(`\\b${n}\\b`).test(src))
        .map((n) => `${f}: ${n}`);
    });
    expect(bad).toEqual([]);
  });

  it("no button draws a typed back / close glyph; a lone › only on the regs page's rows", () => {
    const glyph = /(>\s*[‹✕×←]\s*<)|(["'`][‹✕×←]["'`])|(`[‹←]\s+\$\{)|(["'`][‹←] {2}\w)/;
    const chevron = />\s*›\s*</;
    const bad = files().filter((f) => f.startsWith("packages/ui-native/") || f.startsWith("apps/"))
      .filter((f) => glyph.test(read(f)) || (!f.startsWith("packages/ui-native/src/regs/") && chevron.test(read(f))));
    expect(bad).toEqual([]);
  });

  it("no chevron drawn from a View's two borders and a turn (a fold's mark is the set's expand / collapse)", () => {
    // the pre-N1 fold mark: an 8x8 View, two 2 px borders, rotated 45 / 225 deg — square-capped, 2 px, not Lucide
    const drawn = /border(?:Right|Left|Top|Bottom)Width[^}]{0,200}border(?:Right|Left|Top|Bottom)Width[^}]{0,300}\brotate\b/;
    expect(files().filter((f) => drawn.test(read(f)))).toEqual([]);
  });

  /**
   * (T60 check 4) A TYPED TRIANGLE IS NOT A FOLD MARK EITHER: the regs page's folds (KeepBlocks' caveat, PlainRow's
   * ladder, a sheet's Fold) drew their open / closed state as the prototype's typed ▼ ▶ ▾ ▸ beside a set that already
   * has `expand` / `collapse`. The other glyphs a component may still TYPE are words from core (▲ / ▼ in
   * `WATER_MAP_WORDS`' button labels, the date pill's ▾ caret and ↗ in `WORDS`): a glyph inside a core word is data to a
   * component, so only a literal typed IN a component is refused here.
   */
  it("no component types a triangle or caret as a mark (▼ ▶ ▾ ▸ ▲ ◂ ◃ ◀ ►)", () => {
    const typed = /["'`][^"'`\n]*[▼▶▾▸▲◂◃◀►▴▵▿][^"'`\n]*["'`]|>\s*[▼▶▾▸▲◂◃◀►▴▵▿]\s*</;
    expect(files().filter((f) => f.startsWith("packages/ui-native/") || f.startsWith("apps/")).filter((f) => typed.test(read(f))))
      .toEqual([]);
  });

  it("MUTATION PIN: each glyph scan catches the spelling it refuses, and leaves a core word's glyph alone", () => {
    const glyph = /(>\s*[‹✕×←]\s*<)|(["'`][‹✕×←]["'`])|(`[‹←]\s+\$\{)|(["'`][‹←] {2}\w)/;
    const typed = /["'`][^"'`\n]*[▼▶▾▸▲◂◃◀►▴▵▿][^"'`\n]*["'`]|>\s*[▼▶▾▸▲◂◃◀►▴▵▿]\s*</;
    const pathData = /["'`]\s*[Mm]\s*-?\d[\d.]*[\s,]+-?\d[\d.]*\s*(?:[LlHhVvCcSsQqTtAaZz]|[\s,]+-?\d)/;
    expect([
      glyph.test(code(`<Pressable><Text>‹</Text></Pressable>`.replace("<Text>", ">").replace("</Text>", "<"))),
      glyph.test(code(`<Text>{"✕"}</Text>`)),
      typed.test(code("<Text>{`${open ? \"▼\" : \"▶\"} ${head}`}</Text>")),
      typed.test(code(`<Text aria-hidden>{open ? "▾ " : "▸ "}</Text>`)),
      typed.test(code(`<Text>▼</Text>`)),
      pathData.test(code(`<Path d="M15 18l-6-6 6-6" />`)),
      // a core word carrying a glyph, read by a component, is not a typed mark; nor is a comment
      typed.test(code(`<Text>{WATER_MAP_WORDS.down}</Text> /* ▼ Downstream */ // ▲ Upstream`)),
    ]).toEqual([true, true, true, true, true, true, false]);
  });

  it("every icon in the table is used somewhere (how each one draws: ui-native icons.test.tsx)", () => {
    const table = readFileSync(join(APP, ICONS_FILE), "utf8");
    const names = [...table.slice(table.indexOf("export const LUCIDE_NAME"), table.indexOf("};", table.indexOf("export const LUCIDE_NAME")))
      .matchAll(/^ {2}(\w+): "[\w-]+",$/gm)].map((m) => m[1]!);
    expect(names.length).toBeGreaterThan(10);
    const usage = files().filter((f) => f !== ICONS_FILE).map(read).join("\n");
    // the bar draws its tabs by key (TabBar `TABS`), so "map" / "conditions" / "spots" / "more" are used by name there
    expect(names.filter((n) => !new RegExp(`["'\`]${n}["'\`]`).test(usage))).toEqual([]);
  });

  it("the version is recorded once, and the credit and the licence file carry it", () => {
    expect(ICON_SET).toMatchObject({ name: "Lucide", licence: "ISC" });
    const licence = readFileSync(join(APP, "LICENSES/lucide.txt"), "utf8");
    expect(licence.split("\n")[0]).toBe(`Lucide icons (https://lucide.dev), lucide-static v${ICON_SET.version}`);
    expect(licence).toContain(ICON_SET.copyright);
    expect(licence).toContain("ISC License");
    const icons = readFileSync(join(APP, ICONS_FILE), "utf8");
    for (const [, n] of icons.matchAll(/^ {2}\w+: "([\w-]+)",$/gm)) expect(licence, n).toMatch(new RegExp(`[:,] ${n}[,.]`));
    expect(ICON_CREDIT.lines.join(" ")).toContain(`Lucide v${ICON_SET.version}`);
    expect(ICON_CREDIT.lines).toContain(ICON_SET.copyright);
    expect(icons).toContain(`lucide-static@${ICON_SET.version}`);
  });
});
