/**
 * (T534) THE EMULATOR TESTS A BUILD IT CAN NAME. A kept test-bed APK carries a sidecar stamp (commit, dirty tree, libmaplibre
 * build id); session.sh refuses a stale / dirty / unstamped one before the heavy lock unless a reason is given; the default
 * build is the patched MapLibre; the report header prints the stamp. Pure functions + the CLI session.sh calls + the
 * script's own order (a mutation of any line below fails here, in seconds, without an emulator).
 */
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { describe, expect, it } from "vitest";
// @ts-expect-error plain .mjs, no types
import { makeStamp, readTree, staleProblem, stampLine } from "./testbed/lib/build-stamp.mjs";

const root = resolve(__dirname, "../..");
const read = (p: string) => readFileSync(resolve(root, p), "utf8");
const code = (p: string) => read(p).split("\n").filter((l) => !/^\s*#/.test(l)).join("\n");
const CLI = resolve(__dirname, "testbed/lib/build-stamp.mjs");
const HEAD = "a".repeat(40);
const clean = { commit: HEAD, dirty_files: [] as string[] };
const stamp = (tree = clean, over = {}) => ({ ...makeStamp({ tree, maplibre: "patched", buildId: "7bf3dc29" }), ...over });

describe("staleProblem", () => {
  it("a clean build of HEAD is fine", () => expect(staleProblem(stamp(), HEAD)).toBeNull());
  it("another commit is refused, naming both", () => {
    const p = staleProblem(stamp({ commit: "b".repeat(40), dirty_files: [] }), HEAD);
    expect(p).toMatch(/built from bbbbbbbbb, HEAD is aaaaaaaaa/);
  });
  it("a dirty tree is refused, naming paths", () => {
    expect(staleProblem(stamp({ commit: HEAD, dirty_files: ["app/x.ts"] }), HEAD)).toMatch(/dirty.*app\/x\.ts/);
  });
  it("no stamp is a refusal, never a pass", () => expect(staleProblem(null, HEAD)).toMatch(/no stamp/));
  it("a reason lifts it; blank does not", () => {
    expect(staleProblem(null, HEAD, "emulator-only change")).toBeNull();
    expect(staleProblem(null, HEAD, "  ")).not.toBeNull();
  });
});

describe("makeStamp / stampLine", () => {
  it("records dirtiness from the tree", () => {
    expect(stamp({ commit: HEAD, dirty_files: ["a", "b"] }).dirty).toBe(true);
    expect(stamp().dirty).toBe(false);
  });
  it("stock needs a reason; a build id is required", () => {
    expect(() => makeStamp({ tree: clean, maplibre: "stock", buildId: "aab47813" })).toThrow(/reason/);
    expect(() => makeStamp({ tree: clean, maplibre: "patched", buildId: "" })).toThrow(/build id/);
    expect(() => makeStamp({ tree: clean, maplibre: "other", buildId: "x" })).toThrow(/patched or stock/);
  });
  it("the header line quotes commit and build id; unstamped says so", () => {
    expect(stampLine(stamp())).toMatch(/commit aaaaaaaaa clean .*7bf3dc29 \(patched, T420 lean\)/);
    expect(stampLine(stamp({ commit: HEAD, dirty_files: ["x"] }))).toMatch(/DIRTY \(1 path/);
    expect(stampLine(makeStamp({ tree: clean, maplibre: "stock", buildId: "aab47813", stockReason: "bisect" }))).toMatch(/STOCK: bisect/);
    expect(stampLine(null)).toMatch(/UNSTAMPED/);
  });
  it("readTree lists the dirty paths git reports", () => {
    const git = (a: string[]) => (a[0] === "rev-parse" ? `${HEAD}\n` : " M app/a.ts\n?? app/b.ts\n");
    expect(readTree(git)).toEqual({ commit: HEAD, dirty_files: ["app/a.ts", "app/b.ts"] });
  });
});

describe("the CLI session.sh calls (kept APK check)", () => {
  const head = readTree().commit;
  const run = (stampObj: unknown, ...extra: string[]) => {
    const d = mkdtempSync(join(tmpdir(), "stamp-"));
    const apk = join(d, "a.apk");
    writeFileSync(apk, "x");
    if (stampObj) writeFileSync(`${apk}.stamp.json`, JSON.stringify(stampObj));
    return spawnSync("node", [CLI, "check", apk, ...extra], { encoding: "utf8" });
  };
  it("stale APK: exit 1, the commit named", () => {
    const r = run(stamp({ commit: "c".repeat(40), dirty_files: [] }));
    expect(r.status).toBe(1);
    expect(r.stderr).toMatch(/built from ccccccccc/);
  });
  it("current clean APK: exit 0 and the build line", () => {
    const r = run(stamp({ commit: head, dirty_files: [] }));
    expect(r.status).toBe(0);
    expect(r.stdout).toMatch(/libmaplibre build id 7bf3dc29/);
  });
  it("the reason escape works and is printed", () => {
    const r = run(null, "--allow-stale", "only a scenario file changed");
    expect(r.status).toBe(0);
    expect(r.stdout).toMatch(/STALE APK ALLOWED — only a scenario file changed/);
  });
  it("an empty reason is refused", () => expect(run(null, "--allow-stale", "").status).toBe(2));
});

describe("session.sh and run.mjs", () => {
  const sh = code("app/tools/testbed/session.sh");
  it("the stale check runs before the heavy pre-check exit (before the lock waits)", () => {
    const check = sh.indexOf("lib/build-stamp.mjs\" check");
    const pre = sh.indexOf("HEAVY_PRECHECK");
    expect(check).toBeGreaterThan(-1);
    expect(check).toBeLessThan(pre);
  });
  it("the default build is the patched MapLibre; stock needs --stock-maplibre=<reason>", () => {
    expect(sh).toMatch(/export MLN_LOCAL_MAVEN=\$\{MLN_LOCAL_MAVEN:-\$MLN_DEFAULT\}/);
    expect(sh).toMatch(/MLN_DEFAULT="\$ROOT\/cache\/maplibre\/fslean-m2"/);
    expect(sh).toMatch(/--stock-maplibre\) echo .*needs a reason/);
    expect(sh).toMatch(/STOCK_MLN:\+-stock/);   // a stock APK never takes the default name
  });
  it("the build is stamped with HEAD, checks the libmaplibre build id against the AAR, and writes the sidecar", () => {
    expect(sh).toMatch(/export EXPO_PUBLIC_GIT_SHA=\$\(git -C "\$ROOT" rev-parse HEAD\)/);
    expect(sh).toMatch(/MLN_GOT == "\$MLN_WANT"/);
    expect(sh).toMatch(/build-stamp\.mjs" write "\$APK"/);
  });
  it("run.mjs and the report print the build at the top", () => {
    expect(read("app/tools/testbed/run.mjs")).toMatch(/build under test: \$\{run\.build\}/);
    expect(read("app/tools/testbed/lib/report.mjs")).toMatch(/Build: \$\{run\.build/);
  });
});
