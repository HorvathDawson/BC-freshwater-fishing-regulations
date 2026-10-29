/**
 * (T251) ONE SOURCE OF TRUTH for the app's version: app.json's semver (package.json repeats it) and build number
 * (android.versionCode = ios.buildNumber), stamped into the bundle by version-stamp.js and read by `resolveAppVersion`.
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { parseSemver, resolveAppVersion, versionLine } from "@app/core";

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
      EXPO_PUBLIC_APP_VERSION: app.version, EXPO_PUBLIC_APP_BUILD_NUMBER: String(app.android.versionCode),
      EXPO_PUBLIC_GIT_SHA: "abcdef1234567890", EXPO_PUBLIC_BUILD_DATE: "2026-10-28" });
    expect(versionStamp(app, { EAS_BUILD_GIT_COMMIT_HASH: "1234567aaa" }, () => "abcdef1", day).EXPO_PUBLIC_GIT_SHA)
      .toBe("1234567aaa");
    expect(versionStamp(app, { EXPO_PUBLIC_APP_VERSION: "9.9.9" }, () => "", day).EXPO_PUBLIC_APP_VERSION).toBe("9.9.9");
  });

  it("what the stamp sets is what About says", () => {
    const s = versionStamp(app, {}, () => "abcdef1234567890", new Date("2026-10-28T12:00:00Z"));
    const v = resolveAppVersion({ version: s.EXPO_PUBLIC_APP_VERSION, build: s.EXPO_PUBLIC_APP_BUILD_NUMBER,
                                  sha: s.EXPO_PUBLIC_GIT_SHA, date: s.EXPO_PUBLIC_BUILD_DATE });
    expect(versionLine(v)).toBe(`${app.version} (build ${app.android.versionCode}, abcdef1, 2026-10-28)`);
  });
});
