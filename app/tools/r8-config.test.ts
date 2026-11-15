/**
 * (T749) R8 BEHIND ONE FLAG. `CANIFISHTHIS_R8=1` at prebuild turns the Android code shrinker on for release builds
 * (plugins/with-no-minify.js) and appends plugins/r8-keep-rules.pro to android/app/proguard-rules.pro. With the flag
 * off the build must be today's (T250: R8 off), so merging the trial changes nothing until it is switched on.
 *
 * Pinned through the real `expo config --type introspect` (every plugin's gradle.properties mod run, as prebuild runs
 * it): flag absent = flag "0" byte for byte, minify "false"; flag "1" differs ONLY in that property, now "true"; a
 * production build with the flag is refused (app.config.js); any other value is refused, never read as off.
 */
import { execFileSync, spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

// @ts-expect-error -- plain CommonJS config plugin run by `expo prebuild` (no types)
import plugin from "../apps/mobile/plugins/with-no-minify.js";

const MOBILE = resolve(process.cwd(), "apps/mobile");      // vitest runs from app/
const EXPO = resolve(process.cwd(), "node_modules/.bin/expo");
type Prop = { type: string; key?: string; value?: string };

function introspect(extra: Record<string, string | undefined>) {
  const env: NodeJS.ProcessEnv = { ...process.env, ...extra };
  for (const k of ["CANIFISHTHIS_R8", "EXPO_PUBLIC_APP_ENV", "EAS_BUILD_PROFILE"]) if (!(k in extra)) delete env[k];
  for (const [k, v] of Object.entries(extra)) if (v === undefined) delete env[k];
  return execFileSync(EXPO, ["config", "--type", "introspect", "--json"], { cwd: MOBILE, env, encoding: "utf8" });
}
const props = (json: string): Prop[] => JSON.parse(json)._internal.modResults.android.gradleProperties;
const minify = (ps: Prop[]) => ps.filter((p) => p.type === "property" && p.key === plugin.KEY);

describe("T749: R8 is off unless CANIFISHTHIS_R8=1", () => {
  const off = introspect({ CANIFISHTHIS_R8: undefined });

  it("flag absent: minify false, once; no resource shrinking", () => {
    expect(minify(props(off))).toEqual([{ type: "property", key: plugin.KEY, value: "false" }]);
    expect(props(off).some((p) => p.key === "android.enableShrinkResourcesInReleaseBuilds")).toBe(false);
  });

  it("flag \"0\" and \"\" are the absent flag, byte for byte", () => {
    expect(introspect({ CANIFISHTHIS_R8: "0" })).toBe(off);
    expect(introspect({ CANIFISHTHIS_R8: "" })).toBe(off);
  });

  it("flag \"1\": the same config but minify true", () => {
    const on = introspect({ CANIFISHTHIS_R8: "1" });
    expect(minify(props(on))).toEqual([{ type: "property", key: plugin.KEY, value: "true" }]);
    expect(on.replace('"key":"android.enableMinifyInReleaseBuilds","value":"true"',
      '"key":"android.enableMinifyInReleaseBuilds","value":"false"')).toBe(off);
  });

  it("refuses any other value, and the flag in a production build", () => {
    expect(() => plugin.r8Enabled({ CANIFISHTHIS_R8: "true" })).toThrow(/must be unset, "0" or "1"/);
    const prod = spawnSync(EXPO, ["config", "--type", "introspect", "--json"], {
      cwd: MOBILE, encoding: "utf8",
      env: { ...process.env, CANIFISHTHIS_R8: "1", EXPO_PUBLIC_APP_ENV: "production", EXPO_PUBLIC_CONTACT: "support@canifishthis.ca" },
    });
    expect(prod.status).not.toBe(0);
    expect(prod.stderr).toMatch(/CANIFISHTHIS_R8 \(R8 on, T749\) is a trial/);
  });
});

describe("T749: a release build with the flag keeps R8's mapping, or stops", () => {
  const sh = readFileSync(resolve(process.cwd(), "../deploy/staging/build-apk.sh"), "utf8");
  const code = sh.split("\n").filter((l) => !l.trimStart().startsWith("#")).join("\n");

  it("parses as bash", () => {
    expect(spawnSync("bash", ["-n", resolve(process.cwd(), "../deploy/staging/build-apk.sh")]).status).toBe(0);
  });

  it("keep_map keeps r8-mapping.txt when the flag is 1, through the refusing keep-r8 (no `|| true`, no skip)", () => {
    const body = /keep_map\(\) \{\n([\s\S]*?)\n\}/.exec(code)?.[1] ?? "";
    expect(body).toContain('if [[ ${CANIFISHTHIS_R8:-} == 1 ]]; then node "$REPO/app/tools/sourcemaps.mjs" keep-r8 "$R8_MAP"');
    expect(body).not.toMatch(/\|\| *true|2>\/dev\/null/);
    expect(code).toContain("R8_MAP=$M/android/app/build/outputs/mapping/release/mapping.txt");
  });

  it("the mapping is kept before the AAB is committed, copied or uploaded, and before the APK is copied", () => {
    const play = code.indexOf("keep_map\n  AAB=");
    expect(play).toBeGreaterThan(0);
    expect(play).toBeLessThan(code.indexOf("commit_bump\n  cp"));
    expect(play).toBeLessThan(code.indexOf('play-upload.py" --aab'));
    const apk = code.lastIndexOf("keep_map\n");
    expect(apk).toBeLessThan(code.indexOf('cp "$APKF" "$OUT/$NAME"'));
  });

  it("the precheck refuses a bad flag value and the flag with --production, before the heavy lock", () => {
    const lock = code.indexOf("exec scripts/heavy.sh build-apk");
    const pre = code.indexOf("case ${CANIFISHTHIS_R8:-} in");
    expect(pre).toBeGreaterThan(0);
    expect(pre).toBeLessThan(lock);
    expect(code).toMatch(/1\) if \[\[ -n \$PROD \]\]; then bad "CANIFISHTHIS_R8=1 is a trial/);
    expect(code).toMatch(/\*\) bad "CANIFISHTHIS_R8 must be unset, 0 or 1/);
  });
});

describe("T749: the keep rules reach proguard-rules.pro", () => {
  const rules = readFileSync(plugin.KEEP_FILE, "utf8");
  const template = "# Add project specific ProGuard rules here.\n-keep class com.facebook.react.turbomodule.** { *; }\n";

  it("appended once, after the template's own rules", () => {
    const once = plugin.appendKeepRules(template);
    expect(once.startsWith(template)).toBe(true);
    expect(once).toContain(`${plugin.MARK}\n${rules}`);
    expect(plugin.appendKeepRules(once)).toBe(once);
  });

  it("holds the T250 fix and every rule has its reason above it", () => {
    expect(rules).toContain("-keep class * extends androidx.room.RoomDatabase { <init>(); }");
    const lines = rules.split("\n");
    lines.forEach((l, i) => {
      if (!/^-keep/.test(l)) return;
      let j = i - 1;
      while (j >= 0 && /^-keep/.test(lines[j])) j--;      // a group of rules shares the comment above it
      expect(lines[j], `rule at line ${i + 1} has no reason above it`).toMatch(/^#/);
    });
  });

  it("minify follows the flag in the gradle.properties mod", () => {
    const tmpl: Prop[] = [{ type: "property", key: plugin.KEY, value: "true" }, { type: "property", key: "x", value: "1" }];
    expect(plugin.setMinify(tmpl, false)).toEqual([{ type: "property", key: "x", value: "1" }, { type: "property", key: plugin.KEY, value: "false" }]);
    expect(minify(plugin.setMinify(tmpl, true))).toEqual([{ type: "property", key: plugin.KEY, value: "true" }]);
  });
});
