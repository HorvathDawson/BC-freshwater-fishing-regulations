/**
 * The converter, and — more usefully — every filter Protomaps actually emits.
 *
 * The point of the second half is that this file is a partial reimplementation of the
 * style-spec's `convertFilter`, kept small on purpose (see the note in legacy-filter.ts).
 * "Small" is only safe while the input stays inside what it handles, so the real basemap
 * layers are enumerated here: a Protomaps upgrade that introduces an operator this does not
 * know about fails a test rather than throwing at runtime on a user's map.
 */
import { describe, expect, it } from "vitest";
import { layers as basemapLayers, namedFlavor } from "@protomaps/basemaps";
import { isLegacy, toExpression } from "./legacy-filter";

describe("legacy filters", () => {
  it("recognises legacy and expression forms that share an operator", () => {
    // The distinction is the SHAPE of the arguments, not the operator.
    expect(isLegacy(["==", "kind", "address"])).toBe(true);
    expect(isLegacy(["==", ["get", "oneway"], "yes"])).toBe(false);
    expect(isLegacy(["in", "kind", "river", "stream"])).toBe(true);
    expect(isLegacy(["in", ["get", "kind"], ["literal", ["river"]]])).toBe(false);
  });

  it("converts the forms Protomaps uses", () => {
    expect(toExpression(["==", "kind", "address"]))
      .toEqual(["==", ["get", "kind"], "address"]);
    expect(toExpression(["in", "kind", "river", "stream"]))
      .toEqual(["match", ["get", "kind"], ["river", "stream"], true, false]);
    expect(toExpression(["!in", "kind", "a"]))
      .toEqual(["!", ["match", ["get", "kind"], ["a"], true, false]]);
    expect(toExpression(["!has", "name"])).toEqual(["!", ["has", "name"]]);
    expect(toExpression(["none", ["==", "kind", "a"]]))
      .toEqual(["!", ["any", ["==", ["get", "kind"], "a"]]]);
  });

  it("leaves an expression alone", () => {
    const expr = ["all", ["in", ["get", "kind"], ["literal", ["highway"]]],
                  ["has", "shield_text"]];
    expect(toExpression(expr)).toEqual(expr);
  });

  it("converts a legacy child inside an expression parent", () => {
    // Protomaps mixes them, and `all` is legal in both syntaxes — this is the case that
    // produced the runtime error the converter exists for.
    expect(toExpression(["all", ["==", ["get", "a"], 1], ["==", "kind", "x"]]))
      .toEqual(["all", ["==", ["get", "a"], 1], ["==", ["get", "kind"], "x"]]);
  });

  /** No legacy-only operator survives, and no comparison keeps a bare string key. */
  const expectExpression = (f: unknown, id: string): void => {
    if (!Array.isArray(f) || f.length === 0) return;
    const op = f[0];
    expect(["!in", "!has", "none"].includes(op as string),
           `${id}: ${op} is legacy-only and cannot appear in an expression`).toBe(false);
    if (["==", "!=", ">", ">=", "<", "<="].includes(op as string))
      expect(Array.isArray(f[1]),
             `${id}: ${op} kept a bare key instead of a lookup`).toBe(true);
    for (const c of f.slice(1)) expectExpression(c, id);
  };

  it("handles every filter the real basemap ships", () => {
    const all = basemapLayers("basemap", namedFlavor("light"), { lang: "en" }) as
      { id: string; filter?: unknown }[];
    const KNOWN = new Set(["==", "!=", ">", ">=", "<", "<=", "in", "!in", "has", "!has",
                           "all", "any", "none"]);
    let seen = 0;
    const walk = (f: unknown, id: string) => {
      if (!Array.isArray(f) || f.length === 0) return;
      if (isLegacy(f)) {
        seen++;
        expect(KNOWN.has(f[0] as string), `${id}: unknown legacy operator ${f[0]}`)
          .toBe(true);
        // NOT "it must change": `["has", "is_tunnel"]` is valid and identical in both
        // syntaxes, so converting it is correctly a no-op. What must hold is that the
        // RESULT contains nothing MapLibre would reject inside an expression.
        expectExpression(toExpression(f), id);
      }
      if (f[0] === "all" || f[0] === "any" || f[0] === "none")
        for (const c of f.slice(1)) walk(c, id);
    };
    for (const l of all) if (l.filter) walk(l.filter, l.id);
    // If this ever reads 0, the basemap stopped using legacy filters and this whole file
    // can go — which is a better outcome than it quietly testing nothing.
    expect(seen).toBeGreaterThan(0);
  });
});
