// @vitest-environment node
/**
 * (T1154 L4) ONE MAP ENGINE PER BUILD, AND THE DEFAULT BUILD IS TODAY'S. The resolved Expo config (`app.config.js` over
 * `app.json`, as Expo hands it in) is pinned here as it was BEFORE the engine switch existed: every plugin, in order, with
 * its options, and everything else passed through from app.json. Each case evaluates the config in a fresh Node process
 * that sees only the variables the case sets (app.config.js reads them when it loads), so nothing of the caller's
 * environment decides the answer. No prebuild runs: the config's mods are not applied here (`r8-config.test.ts` runs
 * the one Android mod that matters to MapLibre, `with-pinned-maplibre`).
 */
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterAll, describe, expect, it } from "vitest";

const APP = resolve(__dirname, "..");
const MOBILE = resolve(APP, "apps/mobile");
const read = (p: string) => readFileSync(resolve(APP, p), "utf8");
const appJson = JSON.parse(read("apps/mobile/app.json")).expo;

/** What a process needs to run at all; nothing else of the caller's environment reaches the config (r8-config.test.ts). */
const RUNS_A_PROCESS = ["PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "USER", "SHELL"];
function inNode(script: string, extra: Record<string, string>) {
  const env: NodeJS.ProcessEnv = {};
  for (const k of RUNS_A_PROCESS) if (process.env[k] !== undefined) env[k] = process.env[k];
  Object.assign(env, extra);
  return spawnSync(process.execPath, ["-e", script], { cwd: MOBILE, env, encoding: "utf8" });
}
const CONFIG = "process.stdout.write(JSON.stringify(require('./app.config.js')({ config: require('./app.json').expo })))";
function resolvedConfig(extra: Record<string, string>) {
  const r = inNode(CONFIG, extra);
  expect(r.stderr).toBe("");
  expect(r.status).toBe(0);
  return JSON.parse(r.stdout);
}

/** the pinned MapLibre's version (app/tools/maplibre-android/pin.env) and a stand-in of its local maven repo */
const PINNED = /^MLN_VERSION=(\S+)$/m.exec(read("tools/maplibre-android/pin.env"))![1]!;
const scratch: string[] = [];
afterAll(() => { for (const d of scratch) rmSync(d, { recursive: true, force: true }); });
function repoOf(version: string) {
  const repo = mkdtempSync(join(tmpdir(), "t1154-m2-"));
  scratch.push(repo);
  const dir = join(repo, "org", "maplibre", "gl", "android-sdk-vulkan", version);
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, `android-sdk-vulkan-${version}.aar`), "");
  return repo;
}

/** the splash ground per theme and the launcher icon's ground, read where app.config.js reads them */
const ground = (theme: string) => JSON.parse(read(`packages/map/style/themes/${theme}.json`)).values["color.ui.card"];
const iconGround = read("apps/mobile/assets/icon/icon-ground.txt").trim();
/** app.json's own entry for a plugin (undefined when app.json does not list it, which fails the comparison) */
const listed = (name: string) => appJson.plugins.find((p: unknown) => (Array.isArray(p) ? p[0] : p) === name);

/** TODAY'S RESOLVED CONFIG (taken before T1154 L4): app.json, the icon's ground, and this plugin list in this order. */
function today({ production, local }: { production: boolean; local: string | null }) {
  return {
    ...appJson,
    android: { ...appJson.android, adaptiveIcon: { ...appJson.android.adaptiveIcon, backgroundColor: iconGround } },
    plugins: [
      listed("expo-image-picker"),
      listed("expo-location"),
      ["@maplibre/maplibre-react-native", { android: { nativeVariant: "vulkan", ...(local ? { nativeVersion: local } : {}) } }],
      listed("expo-notifications"),
      "expo-background-task",
      "./plugins/with-upload-signing",
      "./plugins/with-r8",
      "./plugins/with-gradle-memory",
      "./plugins/with-backup-rules",
      "./plugins/with-optional-portrait",
      ["expo-splash-screen", {
        backgroundColor: ground("light"), image: "./assets/splash/splash-light.png", imageWidth: 192, resizeMode: "contain",
        dark: { backgroundColor: ground("dark"), image: "./assets/splash/splash-dark.png" },
      }],
      ...(local ? ["./plugins/with-local-maplibre"] : []),
      ["./plugins/with-pinned-maplibre", { production }],
    ],
  };
}

describe("the resolved Expo config is today's", () => {
  const CONTACT = { EXPO_PUBLIC_CONTACT: "support@canifishthis.ca" };
  it("a bare config (`pnpm android`, a bare prebuild): no local MapLibre, not production", () => {
    expect(resolvedConfig({})).toEqual(today({ production: false, local: null }));
  });
  it("a test-bed / staging Android config: the pinned local MapLibre handed to the binding's plugin", () => {
    expect(resolvedConfig({ MLN_LOCAL_MAVEN: repoOf(PINNED) })).toEqual(today({ production: false, local: PINNED }));
  });
  it("a production config (build-apk.sh): the contact, the pinned local MapLibre", () => {
    expect(resolvedConfig({ ...CONTACT, EXPO_PUBLIC_APP_ENV: "production", MLN_LOCAL_MAVEN: repoOf(PINNED) }))
      .toEqual(today({ production: true, local: PINNED }));
  });
  it("an EAS production profile (the iPhone's): production, no local MapLibre", () => {
    expect(resolvedConfig({ ...CONTACT, EAS_BUILD_PROFILE: "production" })).toEqual(today({ production: true, local: null }));
  });
});
