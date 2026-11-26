/** T1163: the built-plist check, against fixture plists made from what the app config yields (never a second list). */
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
// @ts-expect-error plain .mjs tool
import { bareProject, expectedFromConfig, expectedFromInfoPlist, judge, minimalEnv, parsePlist, readPlist } from "./check-built-plist.mjs";

type Expected = { modes: string[]; identifiers: string[]; strings: Record<string, string> };
type Drop = { mode?: string; string?: string; identifiers?: boolean; change?: string };
const esc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;");
const plist = (e: Expected, drop: Drop = {}): string =>
  `<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n<plist version="1.0"><dict>\n`
  + `<key>CFBundleVersion</key><string>44</string><key>UIRequiresFullScreen</key><true/>\n`
  + `<key>UIBackgroundModes</key><array>${e.modes.filter((m) => m !== drop.mode).map((m) => `<string>${m}</string>`).join("")}</array>\n`
  + (drop.identifiers ? "" : `<key>BGTaskSchedulerPermittedIdentifiers</key><array>${e.identifiers.map((m) => `<string>${m}</string>`).join("")}</array>\n`)
  + Object.entries(e.strings).filter(([k]) => k !== drop.string)
      .map(([k, v]) => `<key>${k}</key><string>${esc(k === drop.change ? `${v} (changed)` : v)}</string>`).join("\n")
  + `\n</dict></plist>\n`;

const made: string[] = [];
const dir = () => { const d = mkdtempSync(join(tmpdir(), "plist-")); made.push(d); return d; };
const file = (text: string) => { const f = join(dir(), "Info.plist"); writeFileSync(f, text); return f; };
afterAll(() => { for (const d of made) rmSync(d, { recursive: true, force: true }); });

describe("check-built-plist", () => {
  const project = bareProject();
  made.push(project);
  const expected: Expected = expectedFromConfig(minimalEnv(), project);
  const built = (drop?: Drop) => readPlist(file(plist(expected, drop)));

  it("the app config declares modes, a task identifier and usage strings to check", () => {
    expect(expected.modes.length).toBeGreaterThan(0);
    expect(expected.identifiers.length).toBeGreaterThan(0);
    expect(Object.keys(expected.strings).length).toBeGreaterThan(0);
  });
  it("a complete plist passes", () => expect(judge(expected, built())).toEqual([]));
  it("a plist missing a declared background mode fails, naming it", () => {
    for (const m of expected.modes) expect(judge(expected, built({ mode: m }))).toEqual([`UIBackgroundModes lacks "${m}"`]);
  });
  it("a plist missing a task identifier fails, naming it", () => {
    expect(judge(expected, built({ identifiers: true })))
      .toEqual(expected.identifiers.map((i) => `BGTaskSchedulerPermittedIdentifiers lacks "${i}"`));
  });
  it("a plist missing a usage string fails, naming it", () => {
    for (const k of Object.keys(expected.strings)) expect(judge(expected, built({ string: k }))).toEqual([`${k} is missing`]);
  });
  it("a plist with a different usage string fails, naming it", () => {
    const k = Object.keys(expected.strings)[0];
    expect(judge(expected, built({ change: k }))).toEqual([`${k} differs from the app config: "${expected.strings[k]} (changed)"`]);
  });
  it("garbage is not a plist, and says so", () => {
    expect(() => parsePlist("hello")).toThrow(/not a plist/);
    expect(() => parsePlist('<plist version="1.0"><array><string>a</string></array></plist>')).toThrow(/not a plist/);
  });
  it("a config with nothing to check is refused, never passed", () => {
    const ok = { UIBackgroundModes: ["processing"], BGTaskSchedulerPermittedIdentifiers: ["x.y"], NSCameraUsageDescription: "c" };
    expect(expectedFromInfoPlist(ok)).toEqual({ modes: ["processing"], identifiers: ["x.y"], strings: { NSCameraUsageDescription: "c" } });
    expect(() => expectedFromInfoPlist({ ...ok, UIBackgroundModes: [] })).toThrow(/no ios.infoPlist.UIBackgroundModes: nothing to check/);
    expect(() => expectedFromInfoPlist(undefined)).toThrow(/nothing to check/);
    expect(() => expectedFromInfoPlist({ ...ok, NSCameraUsageDescription: undefined })).toThrow(/no usage strings: nothing to check/);
    expect(() => expectedFromInfoPlist({ ...ok, BGTaskSchedulerPermittedIdentifiers: undefined }))
      .toThrow(/"processing" but no BGTaskSchedulerPermittedIdentifiers/);
  });

  it("the command exits 0 on a complete plist, and 1 naming a dropped mode", () => {
    const run = (f: string) => spawnSync("node", ["tools/check-built-plist.mjs", f],
      { cwd: process.cwd(), env: minimalEnv(), encoding: "utf8" });
    const ok = run(file(plist(expected)));
    expect([ok.status, ok.stdout.startsWith("built plist ok")]).toEqual([0, true]);
    const m = expected.modes[0];
    const bad = run(file(plist(expected, { mode: m })));
    expect(bad.status).toBe(1);
    expect(bad.stderr).toMatch(new RegExp(`FAIL .*Info\\.plist: UIBackgroundModes lacks "${m}"`));
    const junk = run(file("not a plist"));
    expect([junk.status, junk.stderr.includes("not a plist")]).toEqual([1, true]);
  });
});

describe("the TestFlight build script", () => {
  it("runs the check on the archive's Info.plist, after the archive guard and before the unsigned early exit", () => {
    const sh = readFileSync(new URL("../../deploy/staging/build-ios-testflight.sh", import.meta.url), "utf8");
    const call = 'node "$REPO/app/tools/check-built-plist.mjs" "$(ls -d "$ARCHIVE"/Products/Applications/*.app | head -1)/Info.plist" || exit 1';
    expect(sh.split("\n").filter((l) => l === call)).toHaveLength(1);
    expect(sh.indexOf('echo "xcodebuild archive failed')).toBeLessThan(sh.indexOf(call));
    expect(sh.indexOf(call)).toBeLessThan(sh.indexOf("if has --unsigned; then\n  APP="));
  });
});
