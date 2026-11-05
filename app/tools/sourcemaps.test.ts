/**
 * (T352) app/tools/sourcemaps.mjs: maps kept per build under a key a crash's `app` names, and a stack read back with them.
 */
import { describe, expect, it } from "vitest";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
// A plain .mjs tool (run under node), imported for its functions.
import { decodeMappings, frameOf, keepAndroid, keepWeb, keyOf, symbolicate } from "./sourcemaps.mjs";

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

  it("keeps a web export's maps out of what is served, and refuses an export without any", () => {
    const exp = mkdtempSync(join(tmpdir(), "exp-")), root = mkdtempSync(join(tmpdir(), "sm-"));
    mkdirSync(join(exp, "_expo"), { recursive: true });
    writeFileSync(join(exp, "_expo", "index-1.js"), "code\n//# sourceMappingURL=/_expo/index-1.js.map\n//# debugId=x");
    writeFileSync(join(exp, "_expo", "index-1.js.map"), JSON.stringify(MAP));
    keepWeb(exp, "k", root);
    expect(existsSync(join(exp, "_expo", "index-1.js.map"))).toBe(false);
    expect(existsSync(join(root, "k", "web", "_expo", "index-1.js.map"))).toBe(true);
    expect(readFileSync(join(exp, "_expo", "index-1.js"), "utf8")).toBe("code\n//# debugId=x");
    expect(() => keepWeb(exp, "k", root)).toThrow("no .map under");
    expect(() => keepAndroid(join(exp, "none.map"), "k", root)).toThrow("no Hermes map");
  });
});
