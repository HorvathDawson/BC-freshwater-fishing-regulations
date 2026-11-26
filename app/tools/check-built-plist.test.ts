/** T1163: the built-plist check, against fixture plists made from what the app config yields (never a second list). */
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
// @ts-expect-error plain .mjs tool
import { expectedFromConfig, judge, readPlist } from "./check-built-plist.mjs";

type Expected = { modes: string[]; identifiers: string[]; strings: Record<string, string> };
const esc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;");
const plist = (e: Expected, drop: { mode?: string; string?: string } = {}): string =>
  `<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n<plist version="1.0"><dict>\n`
  + `<key>CFBundleVersion</key><string>44</string><key>UIRequiresFullScreen</key><true/>\n`
  + `<key>UIBackgroundModes</key><array>${e.modes.filter((m) => m !== drop.mode).map((m) => `<string>${m}</string>`).join("")}</array>\n`
  + `<key>BGTaskSchedulerPermittedIdentifiers</key><array>${e.identifiers.map((m) => `<string>${m}</string>`).join("")}</array>\n`
  + Object.entries(e.strings).filter(([k]) => k !== drop.string).map(([k, v]) => `<key>${k}</key><string>${esc(v)}</string>`).join("\n")
  + `\n</dict></plist>\n`;
const build = (e: Expected, drop?: { mode?: string; string?: string }) => {
  const f = join(mkdtempSync(join(tmpdir(), "plist-")), "Info.plist");
  writeFileSync(f, plist(e, drop));
  return readPlist(f);
};

describe("check-built-plist", () => {
  const expected: Expected = expectedFromConfig();
  it("the app config declares modes and usage strings to check", () => {
    expect(expected.modes.length).toBeGreaterThan(0);
    expect(Object.keys(expected.strings).length).toBeGreaterThan(0);
  });
  it("a complete plist passes", () => expect(judge(expected, build(expected))).toEqual([]));
  it("a plist missing a declared background mode fails, naming it", () => {
    for (const m of expected.modes) expect(judge(expected, build(expected, { mode: m }))).toEqual([`UIBackgroundModes lacks "${m}"`]);
  });
  it("a plist missing a usage string fails, naming it", () => {
    for (const k of Object.keys(expected.strings)) expect(judge(expected, build(expected, { string: k }))).toEqual([`${k} is missing`]);
  });
});
