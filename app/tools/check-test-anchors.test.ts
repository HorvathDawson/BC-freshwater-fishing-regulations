/**
 * (T667 work item 4) The test-anchor scanner (`check-test-anchors.mjs`, the rule is its header) finds one planted
 * offender of each kind, leaves a test on its own data alone, and has two modes: the report (exit 0, now) and the failing
 * one (`--strict` / TEST_ANCHORS_STRICT=1, switched on when the list is empty — owner D4: no baseline).
 */
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
// @ts-expect-error a .mjs tool, no types
import { KINDS, isDigest, main, realReleaseIds, report, scan, scanSource, strict, totals } from "./check-test-anchors.mjs";

type Hit = [number, string, string];
const hits = (src: string, committed: string[] = []): Hit[] => scanSource(src, "planted.test.tsx", committed);
const kinds = (src: string, committed: string[] = []): string[] => hits(src, committed).map((h) => h[1]);

/** a test block that follows the regs fixture release */
const FIXTURE = `import { openRegsFixture } from "@app/data/release/node";\n`;
const inFixture = (body: string) => `${FIXTURE}describe("on the fixture", () => { const l = openRegsFixture();\n${body}\n});`;

describe("T667 test anchors: what is found", () => {
  it("one planted offender of each kind", () => {
    const planted: Record<string, string> = {
      path: `const REPO = "."; const d = join(REPO, "data/release");`,
      pointer: `const p = join(__dirname, "../../data/release", "LATEST");`,
      env: `const REL = process.env.RELEASE_DIR;`,
      "raw-section": inFixture(`mount("conditions", 578 as SectionId, l);`),
      "raw-part": inFixture(`mount("gnis:2936", 2, l);`),
      "release-id": inFixture(`expect(l.manifest.data_release).toBe("r-303b4007a70a");`),
      digest: inFixture(`expect(l.manifest.handles).toBe("d5242c0c9f1e77aa");`),
    };
    expect(Object.keys(planted).sort()).toEqual([...KINDS].sort());
    for (const [kind, src] of Object.entries(planted)) expect(kinds(src), kind).toContain(kind);
    expect(kinds(planted.env!)).toEqual(["env"]);
    expect(kinds(planted["raw-section"]!)).toEqual(["raw-section"]);
    expect(kinds(planted["raw-part"]!)).toEqual(["raw-part"]);
  });

  it("the other spellings: sectionLine(n), keyed values, arrays, .lake(item, n), ../ paths, env[...]", () => {
    expect(hits(inFixture(`await st.sectionLine(316756);\nopen({ tapSid: 1236847, partIx: 3 });\nconst s = [9001, 9002] as SectionId[];\nawait names.lake("wbk:329504244", 1);`)))
      .toEqual([[3, "raw-section", "sectionLine(316756)"], [4, "raw-section", "tapSid: 1236847"], [4, "raw-part", "partIx: 3"],
                [5, "raw-section", "[9001, 9002] as SectionId[]"], [6, "raw-part", "lake(…, 1)"]]);
    expect(kinds(`const p = resolve(__dirname, "../../..", "data/generated/bundle/bundle.sqlite");`)).toEqual(["path"]);
    expect(kinds(`const p = "../../data/curated/waters/cuts.json";`)).toEqual(["path"]);
    expect(kinds(`const r = env["RELEASE_DIR"];`)).toEqual(["env"]);
    expect(kinds(inFixture(`expect(m.tiles).toBe("d768d6e135e62b99.c9f9604f12bf");`))).toEqual(["digest"]);
  });

  it("a committed fixture release's id typed out is found in any file", () => {
    const src = `const dir = join(root, "r-1a2b3c4d5e6f");`;
    expect(kinds(src)).toEqual([]);
    expect(hits(src, ["r-1a2b3c4d5e6f"])).toEqual([[1, "release-id", "r-1a2b3c4d5e6f"]]);
  });
});

describe("T667 test anchors: what is left alone", () => {
  it("a test on its own data: numbers, ids and digests name that data", () => {
    const own = `import { makeFixtureSource } from "@app/data/fixture";
// REPO/data/release and r-303b4007a70a and 578 as SectionId in a comment are not code
const UNNAMED = 99_999 as SectionId;
describe("own data", () => {
  it("reads data/release in its title only", () => {
    const tmp = mkdtempSync("x"); mkdirSync(join(tmp, "data/release"));
    writeFileSync(join(tmp, "data/release", "LATEST"), "r-303b4007a70a");
    open({ item: "gnis:2936", partIx: 2, tapSid: 12, handles: "d5242c0c9f1e77aa" }); mount("gnis:2936", 2, UNNAMED);
    expect(reason).toBe("no release on this machine (data/release/r-*)");
  });
});`;
    expect(hits(own)).toEqual([]);
  });

  it("a pin counts only in the block that follows a release — directly, or through a module-level name", () => {
    const mixed = `${FIXTURE}import type { SectionId } from "@app/data";
const FREE = 99_999 as SectionId;
const VEDDER = 1236847 as SectionId;
let store: unknown;
beforeAll(async () => { store = await openRegsStore(openRegsFixture()); });
async function mount(sid: SectionId, loader?: ReturnType<typeof openRegsFixture>) { return [sid, loader]; }
describe("own data", () => { it("a", () => { mount(FREE); mount(7 as SectionId); }); });
describe("the fixture, through the store", () => { it("b", () => { read(store, VEDDER, 8 as SectionId); }); });`;
    // FREE and 7 are the synthetic block's (mount names the reader only in a type); VEDDER and 8 are the fixture block's
    expect(hits(mixed)).toEqual([[4, "raw-section", "1236847 as SectionId"], [9, "raw-section", "8 as SectionId"]]);
  });

  it("0 names nothing; invented ids and digests are not pins; a name through the helper is the allowed way", () => {
    expect(hits(inFixture(`open({ sid: 0, partIx: 0 }); mount("gnis:3062", 0); const none = 0 as SectionId;`))).toEqual([]);
    expect(hits(inFixture(`const a = "r-000000000001", b = "r-aaaaaaaaaaaa", c = "0123456789abcdef", d = "0000000000000000.000000000000";`))).toEqual([]);
    expect(hits(inFixture(`const names = sectionNames(l);\nconst h = await names.stream(354088147, 0);\n` +
      `const p = await names.part("gnis:2936", "skeena_river__tidal_boundary", "skeena_river__exchamsiks_river_into_skeena_river");\n` +
      `mount("gnis:2936", p, l); await mount("conditions", h as SectionId, l);`))).toEqual([]);
    expect(realReleaseIds("r-303b4007a70a r-000000000001")).toEqual(["r-303b4007a70a"]);
    expect([isDigest("d5242c0c9f1e77aa"), isDigest("fedcba9876543210"), isDigest("aaaaaaaaaaaaaaaa")]).toEqual([true, false, false]);
  });
});

describe("T667 test anchors: report mode and failing mode", () => {
  /** a git tree of two test files, one offending */
  function tree(): string {
    const app = mkdtempSync(join(tmpdir(), "anchors-"));
    mkdirSync(join(app, "t"));
    writeFileSync(join(app, "t/bad.test.ts"), `const REPO = ".";\nconst d = join(REPO, "data/release");\n`);
    writeFileSync(join(app, "t/ok.test.ts"), `it("a", () => { expect(1).toBe(1); });\n`);
    execFileSync("git", ["init", "-q"], { cwd: app });
    return app;
  }

  it("the same tree passes the report and fails strict, naming the hit; clean, strict passes", () => {
    const app = tree(), found = scan(app);
    expect(Object.keys(found)).toEqual(["t/bad.test.ts"]);
    expect(totals(found).path).toEqual([1, 1]);
    expect(report(found, 2)).toContain("test anchors (app): 1 of 2 test files offend");
    const out: string[] = [], log = (s: string) => { out.push(s); };
    expect(main([], app, {}, log)).toBe(0);
    expect(main(["--strict"], app, {}, log)).toBe(1);
    expect(main([], app, { TEST_ANCHORS_STRICT: "1" }, log)).toBe(1);
    expect(out.join("\n")).toContain("t/bad.test.ts:2  path: data/release");
    writeFileSync(join(app, "t/bad.test.ts"), `it("b", () => { expect(2).toBe(2); });\n`);
    expect(main(["--strict"], app, {}, log)).toBe(0);
  });

  it("strict is off unless asked", () => {
    expect([strict({}, []), strict({ TEST_ANCHORS_STRICT: "0" }, []), strict({ TEST_ANCHORS_STRICT: "1" }, []), strict({}, ["--strict"])])
      .toEqual([false, false, true, true]);
  });
});
