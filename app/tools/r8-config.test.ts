/**
 * (T1003) R8 IS ON FOR EVERY ANDROID RELEASE BUILD, production included (plugins/with-r8.js): minify on in
 * gradle.properties and plugins/r8-keep-rules.pro appended to android/app/proguard-rules.pro. The T749 trial flag
 * (CANIFISHTHIS_R8) is gone and refused; the one escape, CANIFISHTHIS_NO_R8="<reason>", is a test-bed comparison build:
 * app.config.js refuses it in production and deploy/staging/build-apk.sh refuses it for every build it makes.
 *
 * Pinned through the real `expo config --type introspect` (every plugin's gradle.properties mod run, as prebuild runs
 * it). MUTATION-CHECKED: putting T749's production refusal back (app.config.js) fails "a production config is made with
 * R8 on"; putting the precheck's back (build-apk.sh) fails "the precheck refuses R8 off, never R8 on".
 * (T978 coupling) every production config built here gets a pinned MapLibre stand-in (`repoOf(pin.MLN_VERSION)`), or the
 * pinned plugin refuses it before R8 is reached.
 */
import { execFileSync, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { describe, expect, it } from "vitest";

// @ts-expect-error -- plain CommonJS config plugin run by `expo prebuild` (no types)
import plugin from "../apps/mobile/plugins/with-r8.js";
// @ts-expect-error -- plain CommonJS config plugin (T978)
import pinned from "../apps/mobile/plugins/with-pinned-maplibre.js";

const MOBILE = resolve(process.cwd(), "apps/mobile");      // vitest runs from app/
const EXPO = resolve(process.cwd(), "node_modules/.bin/expo");
type Prop = { type: string; key?: string; value?: string };

/**
 * THE SPAWNED CONFIG SEES ONLY WHAT ITS CASE SETS, plus what a process needs to run at all. It must not inherit the
 * caller's build variables: a real `deploy/staging/build-apk.sh --play` exports MLN_LOCAL_MAVEN, EXPO_PUBLIC_*, … before
 * its quick tests run, and app.config.js reads every one of them. 2026-11-15: inside that build (and nowhere else) the
 * production case was refused for MLN_LOCAL_MAVEN before it reached the R8 refusal, and the build stopped on this test.
 */
const RUNS_A_PROCESS = ["PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "USER", "SHELL"];
function envFor(extra: Record<string, string | undefined>): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const k of RUNS_A_PROCESS) if (process.env[k] !== undefined) env[k] = process.env[k];
  for (const [k, v] of Object.entries(extra)) if (v !== undefined) env[k] = v;
  return env;
}
function introspect(extra: Record<string, string | undefined>) {
  return execFileSync(EXPO, ["config", "--type", "introspect", "--json"], { cwd: MOBILE, env: envFor(extra), encoding: "utf8" });
}
const config = (env: Record<string, string | undefined>) =>
  spawnSync(EXPO, ["config", "--type", "introspect", "--json"], { cwd: MOBILE, encoding: "utf8", env: envFor(env) });
const props = (json: string): Prop[] => JSON.parse(json)._internal.modResults.android.gradleProperties;
const minify = (ps: Prop[]) => ps.filter((p) => p.type === "property" && p.key === plugin.KEY);
const ON = [{ type: "property", key: plugin.KEY, value: "true" }];
const PROD = { EXPO_PUBLIC_APP_ENV: "production", EXPO_PUBLIC_CONTACT: "support@canifishthis.ca" };
const pin = pinned.readPin();
/** a stand-in of the pinned MapLibre maven repo (T978): an empty AAR under the pinned version's path */
function repoOf(version: string) {
  const repo = mkdtempSync(join(tmpdir(), "t893-m2-"));
  const dir = join(repo, "org", "maplibre", "gl", "android-sdk-vulkan", version);
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, `android-sdk-vulkan-${version}.aar`), "");
  return repo;
}

describe("T1003: R8 is on for every release build, production included", () => {
  const on = introspect({});

  it("no variable: minify true, once; no resource shrinking", () => {
    expect(minify(props(on))).toEqual(ON);
    expect(props(on).some((p) => p.key === "android.enableShrinkResourcesInReleaseBuilds")).toBe(false);
  });

  it("a production config is made with R8 on (the T749 refusal is gone)", () => {
    const prod = config({ ...PROD, MLN_LOCAL_MAVEN: repoOf(pin.MLN_VERSION) });
    expect(prod.stderr).not.toMatch(/CANIFISHTHIS_R8|is a trial/);
    expect(prod.status).toBe(0);
    expect(minify(props(prod.stdout))).toEqual(ON);
    expect(readFileSync(resolve(MOBILE, "app.config.js"), "utf8")).not.toMatch(/is a trial|r8Enabled/);
  });

  it("the keep rules are appended whenever R8 is on (a dangerous mod), and not with the escape", () => {
    const base = () => ({ name: "x", slug: "x", _internal: { projectRoot: MOBILE } });
    const was = process.env.CANIFISHTHIS_NO_R8;
    try {
      delete process.env.CANIFISHTHIS_NO_R8;
      expect(typeof plugin(base()).mods?.android?.dangerous).toBe("function");
      process.env.CANIFISHTHIS_NO_R8 = "a comparison";
      expect(plugin(base()).mods?.android?.dangerous).toBeUndefined();
    } finally {
      if (was === undefined) delete process.env.CANIFISHTHIS_NO_R8; else process.env.CANIFISHTHIS_NO_R8 = was;
    }
  });

  it("the removed trial flag is refused, any value (\"0\" would not turn R8 off)", () => {
    for (const v of ["1", "0", ""]) {
      expect(() => plugin.r8OffReason({ CANIFISHTHIS_R8: v })).toThrow(/CANIFISHTHIS_R8 was removed \(T1003\)/);
    }
    const r = config({ CANIFISHTHIS_R8: "1" });
    expect(r.status).not.toBe(0);
    expect(r.stderr).toMatch(/CANIFISHTHIS_R8 was removed \(T1003\)/);
  });
});

describe("T1003: the one escape, R8 off for a test-bed comparison build", () => {
  it("CANIFISHTHIS_NO_R8=<reason>: the same config but minify false", () => {
    const off = introspect({ CANIFISHTHIS_NO_R8: "a size comparison" });
    expect(minify(props(off))).toEqual([{ type: "property", key: plugin.KEY, value: "false" }]);
    expect(off.replace('"key":"android.enableMinifyInReleaseBuilds","value":"false"',
      '"key":"android.enableMinifyInReleaseBuilds","value":"true"')).toBe(introspect({}));
  });

  it("a blank reason is refused, and the escape in a production config", () => {
    expect(() => plugin.r8OffReason({ CANIFISHTHIS_NO_R8: " " })).toThrow(/CANIFISHTHIS_NO_R8 needs a reason/);
    expect(plugin.r8OffReason({ CANIFISHTHIS_NO_R8: "why" })).toBe("why");
    expect(plugin.r8OffReason({})).toBeNull();
    const prod = config({ ...PROD, MLN_LOCAL_MAVEN: repoOf(pin.MLN_VERSION), CANIFISHTHIS_NO_R8: "a comparison" });
    expect(prod.status).not.toBe(0);
    expect(prod.stderr).toMatch(/CANIFISHTHIS_NO_R8 \(R8 off\) is for a test-bed comparison build only/);
  });

  it("the test bed's --no-r8 needs a reason and names its own APK", () => {
    const sh = readFileSync(resolve(process.cwd(), "tools/testbed/session.sh"), "utf8");
    expect(sh).toContain('--no-r8=*) NO_R8=${1#--no-r8=}; [[ -n ${NO_R8// /} ]] || { echo "session.sh: --no-r8= needs a reason" >&2; exit 2; } ;;');
    expect(sh).toContain("${NO_R8:+-nor8}");
    expect(sh).toContain("export CANIFISHTHIS_NO_R8=$NO_R8");
  });
});

describe("T749: the config runs see only their own variables", () => {
  it("nothing a build exported reaches a spawned config (MLN_LOCAL_MAVEN, EXPO_PUBLIC_*, EAS_*, the R8 escape)", () => {
    const set = { MLN_LOCAL_MAVEN: "/a/local/maplibre", EXPO_PUBLIC_APP_ENV: "staging", EXPO_PUBLIC_MAP_LAB: "1",
                  EXPO_PUBLIC_ENV_SWITCH: "1", EAS_BUILD_PROFILE: "production", CANIFISHTHIS_NO_R8: "x" };
    const was = Object.fromEntries(Object.keys(set).map((k) => [k, process.env[k]]));
    try {
      Object.assign(process.env, set);                       // as inside build-apk.sh --play
      expect(Object.keys(envFor({})).filter((k) => !RUNS_A_PROCESS.includes(k))).toEqual([]);
      expect(envFor({ CANIFISHTHIS_NO_R8: "y", EXPO_PUBLIC_CONTACT: undefined })).toMatchObject({ CANIFISHTHIS_NO_R8: "y" });
      expect(Object.keys(envFor({ CANIFISHTHIS_NO_R8: "y" })).filter((k) => !RUNS_A_PROCESS.includes(k))).toEqual(["CANIFISHTHIS_NO_R8"]);
    } finally {
      for (const [k, v] of Object.entries(was)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
    }
  });
});

describe("T1003: every release build keeps R8's mapping, checks R8, and sends the mapping to Play", () => {
  const sh = readFileSync(resolve(process.cwd(), "../deploy/staging/build-apk.sh"), "utf8");
  const code = sh.split("\n").filter((l) => !l.trimStart().startsWith("#")).join("\n");

  it("parses as bash", () => {
    expect(spawnSync("bash", ["-n", resolve(process.cwd(), "../deploy/staging/build-apk.sh")]).status).toBe(0);
  });

  it("keep_map always keeps r8-mapping.txt, through the refusing keep-r8 (no flag, no `|| true`, no skip)", () => {
    const body = /keep_map\(\) \{\n([\s\S]*?)\n\}/.exec(code)?.[1] ?? "";
    expect(body).toContain('  node "$REPO/app/tools/sourcemaps.mjs" keep-r8 "$R8_MAP" "$VER-b$CODE-${SHA:0:7}"');
    expect(body).not.toMatch(/\|\| *true|2>\/dev\/null|if \[\[/);
    expect(code).toContain("R8_MAP=$R8_DIR/mapping.txt");
    expect(code).not.toMatch(/CANIFISHTHIS_R8:-/);
  });

  it("mapping kept and R8 checked before the AAB is committed, copied or uploaded, and before the APK is copied", () => {
    const play = code.indexOf("keep_map\n  AAB=");
    const check = code.indexOf('R8_SCORES=$(r8_check --aab "$AAB")');
    expect(play).toBeGreaterThan(0);
    expect(check).toBeGreaterThan(play);
    expect(check).toBeLessThan(code.indexOf("commit_bump\n  cp"));
    expect(check).toBeLessThan(code.indexOf('play-upload.py" --aab'));
    const apk = code.lastIndexOf("keep_map\nr8_check ||");
    expect(apk).toBeGreaterThan(0);
    expect(apk).toBeLessThan(code.indexOf('cp "$APKF" "$OUT/$NAME"'));
  });

  it("the Play upload sends the kept mapping with the AAB", () => {
    expect(code).toContain('play-upload.py" --aab "$OUT/$NAME" --mapping "$KEPT_R8_MAP"');
    expect(code).toContain("KEPT_R8_MAP=$REPO/data/sourcemaps/$VER-b$CODE-${SHA:0:7}/android/r8-mapping.txt");
  });

  it("the precheck refuses R8 off and the removed flag, before the heavy lock, and never R8 on in production", () => {
    const lock = code.indexOf("exec scripts/heavy.sh build-apk");
    const pre = code.indexOf("if [[ -n ${CANIFISHTHIS_NO_R8+set} ]]; then");
    expect(pre).toBeGreaterThan(0);
    expect(pre).toBeLessThan(lock);
    expect(code).toMatch(/bad "CANIFISHTHIS_NO_R8 \(R8 off\) is refused: every build here has R8 on/);
    expect(code).toMatch(/elif \[\[ -n \$\{CANIFISHTHIS_R8\+set\} \]\]; then\n *bad "CANIFISHTHIS_R8 was removed/);
    expect(code).not.toMatch(/refused for --production \(app\.config\.js/);
    expect(code).not.toMatch(/is a trial/);
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

  it("(T1027) keeps WorkManager's InputMerger constructors (made by name before every run; without it no worker ran)", () => {
    expect(rules.split("\n")).toContain("-keep class * extends androidx.work.InputMerger { <init>(); }");
  });

  it("no rule of ours turns an R8 pass off", () => {
    expect(rules.split("\n").filter((l) => /^\s*-dont(optimize|shrink|obfuscate)\b/.test(l))).toEqual([]);
  });

  it("minify follows setMinify in the gradle.properties mod", () => {
    const tmpl: Prop[] = [{ type: "property", key: plugin.KEY, value: "true" }, { type: "property", key: "x", value: "1" }];
    expect(plugin.setMinify(tmpl, false)).toEqual([{ type: "property", key: "x", value: "1" }, { type: "property", key: plugin.KEY, value: "false" }]);
    expect(minify(plugin.setMinify(tmpl, true))).toEqual(ON);
  });
});

describe("T893/T978: every production Android config carries the pinned upstream MapLibre", () => {
  it("production with the pinned repo is made, and MapLibre's plugin is told the pinned version", () => {
    const prod = config({ ...PROD, MLN_LOCAL_MAVEN: repoOf(pin.MLN_VERSION) });
    expect(prod.stderr).not.toMatch(/MLN_LOCAL_MAVEN|STOCK/);
    expect(prod.status).toBe(0);
    expect(prod.stdout).toContain(pin.MLN_VERSION);
  });
  it("production WITHOUT it (a bare prebuild, an EAS production build) is refused, the fix named", () => {
    for (const env of [PROD, { EAS_BUILD_PROFILE: "production", EXPO_PUBLIC_CONTACT: "support@canifishthis.ca" }]) {
      const r = config(env);
      expect(r.status).not.toBe(0);
      expect(r.stderr).toMatch(/production Android\): this Android build would use the STOCK MapLibre/);
      expect(r.stderr).toContain(`MLN_LOCAL_MAVEN=<repo>/cache/maplibre/${pin.MLN_VERSION}`);
    }
  });
  it("production with another MapLibre repo (the old lean AAR, an experiment) is refused", () => {
    const r = config({ ...PROD, MLN_LOCAL_MAVEN: repoOf("13.6.1-fslean") });
    expect(r.status).not.toBe(0);
    expect(r.stderr).toMatch(new RegExp(`holds MapLibre 13\\.6\\.1-fslean, not the pinned ${pin.MLN_VERSION.replace(/\./g, "\\.")}`));
  });
  it("a dev / staging config without it is made, with a warning (chosen: never block `pnpm android`)", () => {
    expect(config({}).status).toBe(0);
    const p = pinned.maplibreProblem({ production: false, repo: null, version: null, pin });
    expect(p).toMatchObject({ refuse: false });
    expect(p.message).toMatch(/STOCK MapLibre/);
    expect(pinned.maplibreProblem({ production: true, repo: null, version: null, pin })).toMatchObject({ refuse: true });
    expect(pinned.maplibreProblem({ production: true, repo: "/r", version: pin.MLN_VERSION, pin })).toBeNull();
  });
  it("app.config.js adds the check to every config (an Android mod: the iPhone prebuild never meets it)", () => {
    expect(readFileSync(resolve(MOBILE, "app.config.js"), "utf8")).toMatch(/\["\.\/plugins\/with-pinned-maplibre", \{ production \}\]/);
    expect(readFileSync(resolve(MOBILE, "plugins/with-pinned-maplibre.js"), "utf8")).toMatch(/return withGradleProperties\(config/);
  });
});
