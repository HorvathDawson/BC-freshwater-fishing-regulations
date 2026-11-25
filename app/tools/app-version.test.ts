/**
 * (T251) ONE SOURCE OF TRUTH for the app's version: app.json's semver (package.json repeats it) and build number
 * (android.versionCode = ios.buildNumber), stamped into the bundle by version-stamp.js and read by `resolveAppVersion`.
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { parseSemver, resolveAppVersion, versionLine, versionTag } from "@app/core";

// @ts-expect-error -- plain CommonJS run by Metro's config (no types)
import { versionStamp as stamp } from "../apps/mobile/version-stamp.js";

const ROOT = resolve(process.cwd(), "apps/mobile");     // vitest runs from app/
const read = (f: string) => JSON.parse(readFileSync(resolve(ROOT, f), "utf8"));
const app = read("app.json").expo, pkg = read("package.json");
const versionStamp = stamp as (app: unknown, env: Record<string, string | undefined>, sha: () => string,
                                today?: Date) => Record<string, string>;

describe("the app's version", () => {
  it("app.json is the semver; package.json and the two build numbers agree with it", () => {
    expect(parseSemver(app.version)).not.toBeNull();
    expect(pkg.version).toBe(app.version);
    expect(Number.isInteger(app.android.versionCode)).toBe(true);
    expect(app.ios.buildNumber).toBe(String(app.android.versionCode));
  });

  it("the stamp: app.json's version and build, EAS's commit before git's, today; the environment wins", () => {
    const day = new Date("2026-10-28T12:00:00Z");
    expect(versionStamp(app, {}, () => "abcdef1234567890", day)).toEqual({
      EXPO_PUBLIC_APP_VERSION: app.version, EXPO_PUBLIC_APP_BUILD_NUMBER_ANDROID: String(app.android.versionCode),
      EXPO_PUBLIC_APP_BUILD_NUMBER_IOS: String(app.ios.buildNumber),
      EXPO_PUBLIC_GIT_SHA: "abcdef1234567890", EXPO_PUBLIC_BUILD_DATE: "2026-10-28" });
    expect(versionStamp(app, { EAS_BUILD_GIT_COMMIT_HASH: "1234567aaa" }, () => "abcdef1", day).EXPO_PUBLIC_GIT_SHA)
      .toBe("1234567aaa");
    expect(versionStamp(app, { EXPO_PUBLIC_APP_VERSION: "9.9.9" }, () => "", day).EXPO_PUBLIC_APP_VERSION).toBe("9.9.9");
  });

  it("what the stamp sets is what About says", () => {
    const s = versionStamp(app, {}, () => "abcdef1234567890", new Date("2026-10-28T12:00:00Z"));
    const v = resolveAppVersion({ version: s.EXPO_PUBLIC_APP_VERSION, build: s.EXPO_PUBLIC_APP_BUILD_NUMBER_ANDROID,
                                  sha: s.EXPO_PUBLIC_GIT_SHA, date: s.EXPO_PUBLIC_BUILD_DATE });
    expect(versionLine(v)).toBe(`${app.version} (build ${app.android.versionCode}, abcdef1, 2026-10-28)`);
  });

  // (T1101) the iPhone upload sets ios.buildNumber in its build worktree only (build-ios-testflight.sh), so the two differ
  // there (build 45 on the iPhone, versionCode 44 in git): each platform's bundle must say its own
  it("the stamp carries each platform's own build number (an iPhone build 45 with versionCode 44: 45 for the iPhone, 44 for Android)", () => {
    const twin = { ...app, android: { ...app.android, versionCode: 44 }, ios: { ...app.ios, buildNumber: "45" } };
    const s = versionStamp(twin, {}, () => "abcdef1234567890", new Date("2026-11-24T12:00:00Z"));
    const on = (build: string | undefined) => resolveAppVersion({ version: s.EXPO_PUBLIC_APP_VERSION, sha: s.EXPO_PUBLIC_GIT_SHA, date: s.EXPO_PUBLIC_BUILD_DATE, build });
    expect(versionLine(on(s.EXPO_PUBLIC_APP_BUILD_NUMBER_IOS))).toContain("build 45,");
    expect(versionTag(on(s.EXPO_PUBLIC_APP_BUILD_NUMBER_IOS))).toContain("+45.");   // the User-Agent / X-App-Version a data request carries
    expect(versionLine(on(s.EXPO_PUBLIC_APP_BUILD_NUMBER_ANDROID))).toContain("build 44,");
    // a pinned number wins per platform
    expect(versionStamp(twin, { EXPO_PUBLIC_APP_BUILD_NUMBER_IOS: "99" }, () => "", new Date()).EXPO_PUBLIC_APP_BUILD_NUMBER_IOS).toBe("99");
  });

  it("the app's one build read picks per platform from the two stamped variables (config.ts cannot be imported here)", () => {
    const cfg = readFileSync(resolve(ROOT, "src/config.ts"), "utf8");
    expect(cfg).toMatch(/build: Platform\.select\(\{ ios: process\.env\.EXPO_PUBLIC_APP_BUILD_NUMBER_IOS, default: process\.env\.EXPO_PUBLIC_APP_BUILD_NUMBER_ANDROID \}\)/);
    expect(cfg).not.toMatch(/EXPO_PUBLIC_APP_BUILD_NUMBER\b(?!_)/);
  });
});
