/**
 * (T352) app/tools/sourcemaps.mjs: maps kept per build under a key a crash's `app` names, and a stack read back with them.
 */
import { describe, expect, it } from "vitest";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
// A plain .mjs tool (run under node), imported for its functions.
import { checkServedMaps, decodeMappings, frameOf, keepAndroid, keepR8, keepWeb, keyOf, MAP_REQUIRED_BYTES, symbolicate } from "./sourcemaps.mjs";

// line 1: col 0 → a.ts 1:1; col 5 → a.ts 1:6 named f. line 2: col 0 → a.ts 2:1
const MAP = { version: 3, sources: ["a.ts"], names: ["f"], mappings: "AAAA,KAAKA;AACL" };

describe("sourcemaps", () => {
  it("keys a build as a crash names it", () => {
    expect(keyOf("0.5.0 (build 8, 9813980, 2026-11-04)")).toBe("0.5.0-b8-9813980");
    expect(keyOf("0.5.0 (build 8, 98139801234abcd, 2026-11-04)")).toBe("0.5.0-b8-9813980");
    expect(keyOf("0.5.0")).toBeNull();
  });

  it("decodes v3 mappings and reads Hermes, V8 and Safari frames", () => {
    expect(decodeMappings(MAP.mappings)).toEqual([[[0, 0, 0, 0], [5, 0, 0, 5, 0]], [[0, 0, 1, 0]]]);
    expect(frameOf("at ?anon_0_midOf (address at index.android.bundle:1:2384728)"))
      .toEqual({ file: "index.android.bundle", line: 1, column: 2384728 });
    expect(frameOf("at J (index-ce90.js:466:2110)")).toEqual({ file: "index-ce90.js", line: 466, column: 2110 });
    expect(frameOf("f@https://x/_expo/static/js/web/index-1.js:2:10")).toEqual({ file: "index-1.js", line: 2, column: 10 });
    expect(frameOf("at next (native)")).toBeNull();
  });

  it("symbolicates a web stack (1-based columns) and a Hermes one (0-based), saying what it cannot read", () => {
    const root = mkdtempSync(join(tmpdir(), "sm-"));
    mkdirSync(join(root, "0.5.0-b8-abcdef1", "web", "js"), { recursive: true });
    writeFileSync(join(root, "0.5.0-b8-abcdef1", "web", "js", "index-1.js.map"), JSON.stringify(MAP));
    mkdirSync(join(root, "0.5.0-b8-abcdef1", "android"), { recursive: true });
    writeFileSync(join(root, "0.5.0-b8-abcdef1", "android", "index.android.bundle.map"), JSON.stringify(MAP));
    expect(symbolicate({ app: "0.5.0 (build 8, abcdef1, 2026-11-04)", platform: "web",
                         stack: "at g (index-1.js:1:6)\nat h (other.js:1:1)" }, root))
      .toEqual(["at g (index-1.js:1:6)\n      → a.ts:1:6 (f)", "at h (other.js:1:1)    [no map for other.js]"]);
    expect(symbolicate({ app: "0.5.0 (build 8, abcdef1, 2026-11-04)", platform: "android 37",
                         stack: "at next (native)\nat g (address at index.android.bundle:2:3)" }, root))
      .toEqual(["at next (native)", "at g (address at index.android.bundle:2:3)\n      → a.ts:2:1"]);
    expect(() => symbolicate({ app: "0.4.0 (build 7, 89ebd94, 2026-10-31)", platform: "web", stack: "" }, root))
      .toThrow("no maps kept for 0.4.0-b7-89ebd94");
  });

  it("(T1008) keeps a copy of a web export's maps and leaves them served (the map and its sourceMappingURL stay); refuses an export without any", () => {
    const exp = mkdtempSync(join(tmpdir(), "exp-")), root = mkdtempSync(join(tmpdir(), "sm-"));
    mkdirSync(join(exp, "_expo"), { recursive: true });
    const js = "code\n//# sourceMappingURL=/_expo/index-1.js.map\n//# debugId=x";
    writeFileSync(join(exp, "_expo", "index-1.js"), js);
    writeFileSync(join(exp, "_expo", "index-1.js.map"), JSON.stringify({ ...MAP, debugId: "x" }));
    keepWeb(exp, "k", root, exp, () => true);
    expect(existsSync(join(exp, "_expo", "index-1.js.map"))).toBe(true);
    expect(existsSync(join(root, "k", "web", "_expo", "index-1.js.map"))).toBe(true);
    expect(readFileSync(join(exp, "_expo", "index-1.js"), "utf8")).toBe(js);
    const empty = mkdtempSync(join(tmpdir(), "exp-"));
    expect(() => keepWeb(empty, "k", root, empty, () => true)).toThrow("no .map under");
    expect(() => keepAndroid(join(exp, "none.map"), "k", root)).toThrow("no Hermes map");
  });

  it("(T1008) a served map must belong to its bundle and show only files the repository tracks or node_modules holds", () => {
    const exp = mkdtempSync(join(tmpdir(), "exp-")), web = join(exp, "_expo", "static", "js", "web");
    mkdirSync(web, { recursive: true });
    mkdirSync(join(exp, "node_modules", "lib"), { recursive: true });
    writeFileSync(join(exp, "node_modules", "lib", "x.js"), "");
    const big = "x".repeat(MAP_REQUIRED_BYTES);
    const put = (name: string, body: string, map?: object) => {
      writeFileSync(join(web, name), body);
      if (map) writeFileSync(join(web, `${name}.map`), JSON.stringify(map));
    };
    const ok = { version: 3, debugId: "d1", sections: [{ offset: { line: 0, column: 0 }, map: {
      version: 3, sources: ["__prelude__", "\u0000polyfill:environment-variables", "/packages/core/src/a.ts", "/node_modules/lib/x.js"], mappings: "" } }] };
    const tracked = (p: string) => p === "packages/core/src/a.ts";
    put("index-1.js", `${big}\n//# sourceMappingURL=/_expo/static/js/web/index-1.js.map\n//# debugId=d1`, ok);
    put("small.js", "tiny");                                                     // under 100 KB: no map asked
    expect(checkServedMaps(exp, exp, tracked)).toEqual([]);
    put("later-2.js", `${big}\n//# debugId=d2`);                                 // a big bundle with no map
    put("later-3.js", `${big}\n//# sourceMappingURL=later-3.js.map\n//# debugId=d3`, { ...ok, debugId: "d9" });
    put("later-4.js", `${big}\n//# sourceMappingURL=gone.js.map`);
    put("later-5.js", `${big}\n//# sourceMappingURL=later-5.js.map\n//# debugId=d5`, { version: 3, debugId: "d5", file: "other.js",
      sources: ["/apps/mobile/.env.local", "/node_modules/nope/y.js"], mappings: "" });
    expect(checkServedMaps(exp, exp, tracked)).toEqual([
      "_expo/static/js/web/later-2.js: no sourceMappingURL",
      "_expo/static/js/web/later-3.js: its map's debug id d9 is not the bundle's d3",
      "_expo/static/js/web/later-4.js: names gone.js.map, which the export does not have",
      "_expo/static/js/web/later-5.js: its map is for other.js",
      "_expo/static/js/web/later-5.js: its map names 2 file(s) the repository does not track: /apps/mobile/.env.local, /node_modules/nope/y.js",
    ]);
    expect(() => keepWeb(exp, "k", mkdtempSync(join(tmpdir(), "sm-")), exp, tracked)).toThrow("the export's maps cannot be served");
  });

  it("(T749) keeps R8's mapping beside the Hermes map, and refuses a missing or empty one", () => {
    const dir = mkdtempSync(join(tmpdir(), "r8-")), root = mkdtempSync(join(tmpdir(), "sm-"));
    expect(() => keepR8(join(dir, "mapping.txt"), "k", root)).toThrow("no R8 mapping at");
    writeFileSync(join(dir, "mapping.txt"), "");
    expect(() => keepR8(join(dir, "mapping.txt"), "k", root)).toThrow("is empty");
    expect(existsSync(join(root, "k"))).toBe(false);
    writeFileSync(join(dir, "mapping.txt"), "a.B -> c:\n");
    expect(keepR8(join(dir, "mapping.txt"), "k", root)).toBe(join(root, "k", "android", "r8-mapping.txt"));
    expect(readFileSync(join(root, "k", "android", "r8-mapping.txt"), "utf8")).toBe("a.B -> c:\n");
  });
});
