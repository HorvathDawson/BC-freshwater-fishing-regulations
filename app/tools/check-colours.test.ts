/**
 * ONE PALETTE: no colour is written outside the token files.
 *
 * `tools/check-colours.mjs` is the scanner (and `pnpm colours` the gate). This holds it two
 * ways, because a scanner that finds nothing proves nothing on its own:
 *
 *   1. the workspace, as it stands, has no colour outside the palette;
 *   2. the scanner DOES catch every form a colour has actually been written in here — a hex
 *      in a fallback, an `rgba()` scrim, a named colour on a style prop, a CSS fallback — and
 *      does NOT catch what is not a colour (a comment's history, a basemap flavour's name, a
 *      template that builds `rgb()` from a token).
 *
 * Without (2), a regex that quietly stopped matching would pass (1) forever.
 */
import { describe, expect, it } from "vitest";
// A plain .mjs gate (it runs under node as `pnpm colours`), imported for its functions.
import { ALLOW, findColours, scan } from "./check-colours.mjs";

type Hit = { line: number; text: string };
/**
 * The fixtures below are written ENCODED — `~` for the hash, `RGB`/`HSL` for the functions,
 * `_` breaking a colour name — so that this file holds no colour itself and the gate needs no
 * exception for its own test. `de()` turns them back into the real thing.
 */
const de = (s: string) =>
  s.replace(/~/g, "#").replace(/RGB/g, "rgb").replace(/HSL/g, "hsl").replace(/_/g, "");
const found = (src: string, path = "x.tsx"): string[] =>
  (findColours(de(src), path) as Hit[]).map((h) => h.text);

describe("one palette", () => {
  it("has no colour literal outside the token files", () => {
    const hits = scan() as { file: string; line: number; text: string }[];
    expect(hits.map((h) => `${h.file}:${h.line} ${h.text}`)).toEqual([]);
  });

  it("allows the palette and nothing else", () => {
    // Widening this list is the one way past the gate. It should hurt to do.
    expect([...ALLOW].sort()).toEqual([
      "packages/map/style/style.json",
      "packages/map/style/style.meta.json",
      "packages/map/style/themes/cvd.json",
      "packages/map/style/themes/dark.json",
      "packages/map/style/themes/light.json",
      "packages/map/style/tokens.json",
    ]);
  });
});

describe("the scanner catches every way a colour has been written here", () => {
  it.each([
    ["a hex fallback", `const c = t["color.line"] ?? "~D9D9D2";`, "~D9D9D2"],
    ["a short hex", `<Text style={{ color: "~fff" }} />`, "~fff"],
    ["an 8-digit hex", `shadow: "~15181CE6"`, "~15181CE6"],
    ["an rgba scrim", `backgroundColor: "RGBa(0,0,0,0.28)"`, "RGBa(0"],
    ["an hsl", `fill: "HSL(210, 40%, 50%)"`, "HSL(2"],
    ["a named colour on a style prop", `backgroundColor: on ? palette.tint : "wh_ite"`, `"wh_ite"`],
    ["a named colour in a tone", `const tone = "crim_son";`, `"crim_son"`],
  ])("%s", (_what, src, want) => {
    expect(found(src)).toContain(de(want));
  });

  it("a CSS fallback inside var()", () => {
    expect(found(`a { border: 1px solid var(--map-ink, ~15181C); }`, "c.css")).toContain(de("~15181C"));
    expect(found(`a { color: r_ed; }`, "c.css")).toContain(de("r_ed"));
  });

  it("a hex in app JSON", () => {
    expect(found(`{ "backgroundColor": "~000000" }`, "apps/mobile/app.json")).toContain(de("~000000"));
  });
});

describe("the scanner leaves alone what is not a colour", () => {
  it.each([
    ["a line comment's history", `// it was ~B33124, ΔE 1.6 from the places orange`],
    ["a block comment's history", `/* \`faint\` was ~99A0A6, 2.65:1 */ const x = 1;`],
    ["a basemap flavour's name", `const flavor = namedFlavor(dark ? "black" : "light");`],
    ["rgb() built from a token", "return `RGBa(${r}, ${g}, ${b}, ${a})`;"],
    ["transparent", `backgroundColor: on ? palette.tint : "transparent"`],
    ["a URL fragment", `const u = "https://example.org/page#abc-def";`],
    ["a word that is a colour, not used as one", `label: "Orange River"`],
  ])("%s", (_what, src) => {
    expect(found(src)).toEqual([]);
  });

  it("JSON $comment prose", () => {
    expect(found(`{ "$comment": "the light value was ~B33124", "a": 1 }`, "x.json")).toEqual([]);
  });

  it("keeps line numbers through a stripped block comment", () => {
    const hits = findColours(de(`/*\n one\n two\n*/\nconst c = "~ABCDEF";`), "x.ts") as Hit[];
    expect(hits).toEqual([{ line: 5, text: de("~ABCDEF") }]);
  });
});
