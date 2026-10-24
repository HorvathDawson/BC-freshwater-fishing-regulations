/**
 * ONE COLOURING PATHWAY (T60 check 5). A colour reaches a phone component ONE way:
 *
 *   tokens.json + themes/*.json  ──pnpm style:build──▶  style.json  ──resolveTheme / colour()──▶  theme.ts `Palette`  ──▶  components
 *
 * `tools/check-colours.mjs` (gate `pnpm colours`) already refuses a colour LITERAL anywhere outside the token files. This
 * holds the rest of the pathway, which a literal-scan cannot see:
 *
 *   one reader     the token readers (`colour(`, `translucent(`, `waterStatusColour(`, `tempBandColour(`,
 *                  `resolveTheme(`, `STYLE_META` colours) are called in `packages/ui-native/src/theme.ts` and nowhere
 *                  else in the UI — a component that read a token by name beside the palette would be a second palette
 *                  (that is how the gauge pill's border once read a token that did not exist);
 *   no derivation  no component makes a colour FROM a palette colour — a hex-alpha suffix (`${palette.ink}33`), a
 *                  `rgba(` built from parts, an `opacity` on a colour as a tint — the palette carries every tint it
 *                  needs (`scrim`, `lift`, `noAccessBand`), computed once in theme.ts;
 *   the gate       `pnpm check` still runs `colours` (the literal gate) and `test` (this): the wiring is read from
 *                  package.json, so a dropped step fails here.
 *
 * Mutation-pinned: a planted `colour("color.ui.ink")` in a component, a planted `${palette.ink}33`, and a planted
 * `rgba(${r},` are each found; `palette.ink`, `"transparent"` and a `colour(` in theme.ts are not.
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { APP, code, files, read } from "./consistency";

const THEME = "packages/ui-native/src/theme.ts";
/** A token read: the map package's colour API, by name. */
const TOKEN_READ = /\b(?:colour|translucent|waterStatusColour|tempBandColour|resolveTheme)\s*\(|\bSTYLE_META\b/;
/** A colour derived from a palette colour: a hex-alpha suffix on a template hole, or an rgb/hsl built from parts. */
const DERIVED = /\$\{\s*palette\.[\w.[\]]+\s*\}[0-9A-Fa-f]{2}\b|\b(?:rgba?|hsla?)\(\s*\$\{/;

describe("one colouring pathway (T60)", () => {
  it("the token readers are called in theme.ts and nowhere else in the UI", () => {
    const readers = files().filter((f) => TOKEN_READ.test(read(f)));
    expect(readers).toEqual([THEME]);
  });

  it("no component derives a colour from a palette colour (a tint is the palette's, computed once)", () => {
    // regs/testing is the render tests' census (it reads computed styles back as rgb), not a component
    expect(files().filter((f) => f !== THEME && !/\/testing\//.test(f) && DERIVED.test(read(f)))).toEqual([]);
  });

  it("`pnpm check` runs the literal gate and the suites", () => {
    const pkg = JSON.parse(readFileSync(join(APP, "package.json"), "utf8")) as { scripts: Record<string, string> };
    const steps = pkg.scripts.check!.split("&&").map((s) => s.trim().replace(/^pnpm run /, ""));
    expect(steps).toContain("colours");
    expect(steps).toContain("test");
    expect(pkg.scripts.colours).toBe("node tools/check-colours.mjs");
  });

  it("MUTATION PIN: a planted token read and a planted derivation are found; the palette's own use is not", () => {
    const planted = {
      read: code(`import { colour } from "@app/map"; const ink = colour("light", "color.ui.ink");`),
      meta: code(`const stops = STYLE_META.colorModes.stream;`),
      alpha: code("const faint = `${palette.ink}33`;"),
      rgba: code("const tint = `rgba(${r}, ${g}, ${b}, 0.2)`;"),
      fine: code(`const s = { color: palette.ink, backgroundColor: "transparent" }; // colour("x") in a comment`),
    };
    expect([TOKEN_READ.test(planted.read), TOKEN_READ.test(planted.meta), DERIVED.test(planted.alpha), DERIVED.test(planted.rgba),
            TOKEN_READ.test(planted.fine) || DERIVED.test(planted.fine)]).toEqual([true, true, true, true, false]);
  });
});
