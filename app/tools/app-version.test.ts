/**
 * (T251) ONE SOURCE OF TRUTH for the app's version: app.json's semver (package.json repeats it) and build number
 * (android.versionCode = ios.buildNumber), stamped into the bundle by version-stamp.js and read by `resolveAppVersion`.
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { parseSemver, platformBuild, resolveAppVersion, versionLine, versionTag } from "@app/core";

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
  it("each platform shows its own build number (an iPhone build 45 with versionCode 44 says 45 on the iPhone, 44 on Android)", () => {
    const twin = { ...app, android: { ...app.android, versionCode: 44 }, ios: { ...app.ios, buildNumber: "45" } };
    const s = versionStamp(twin, {}, () => "abcdef1234567890", new Date("2026-11-24T12:00:00Z"));
    const on = (os: string) => resolveAppVersion({ version: s.EXPO_PUBLIC_APP_VERSION, sha: s.EXPO_PUBLIC_GIT_SHA, date: s.EXPO_PUBLIC_BUILD_DATE,
      build: platformBuild(os, { ios: s.EXPO_PUBLIC_APP_BUILD_NUMBER_IOS, android: s.EXPO_PUBLIC_APP_BUILD_NUMBER_ANDROID }) });
    expect(versionLine(on("ios"))).toContain("build 45,");
    expect(versionTag(on("ios"))).toContain("+45.");                       // the User-Agent / X-App-Version a data request carries
    expect(versionLine(on("android"))).toContain("build 44,");
    expect(versionLine(on("web"))).toContain("build 44,");                 // the web is unchanged: the versionCode
    // a pinned number wins per platform, and a platform with no number says none (never the other's)
    expect(versionStamp(twin, { EXPO_PUBLIC_APP_BUILD_NUMBER_IOS: "99" }, () => "", new Date()).EXPO_PUBLIC_APP_BUILD_NUMBER_IOS).toBe("99");
    const bare = versionStamp({ version: "1.0.0", android: { versionCode: 7 } }, {}, () => "", new Date());
    expect(platformBuild("ios", { ios: bare.EXPO_PUBLIC_APP_BUILD_NUMBER_IOS || undefined, android: bare.EXPO_PUBLIC_APP_BUILD_NUMBER_ANDROID })).toBeUndefined();
  });

  it("the app's one build read goes through platformBuild by Platform.OS, from the two stamped variables", () => {
    const cfg = readFileSync(resolve(ROOT, "src/config.ts"), "utf8");
    expect(cfg).toMatch(/platformBuild\(Platform\.OS,\s*\{\s*ios: process\.env\.EXPO_PUBLIC_APP_BUILD_NUMBER_IOS,\s*android: process\.env\.EXPO_PUBLIC_APP_BUILD_NUMBER_ANDROID/);
    expect(cfg).not.toMatch(/EXPO_PUBLIC_APP_BUILD_NUMBER\b(?!_)/);
  });
});
