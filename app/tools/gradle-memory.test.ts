/**
 * (T1058) THE GRADLE DAEMON'S MEMORY (plugins/with-gradle-memory.js): every prebuild writes
 * `org.gradle.jvmargs=-Xmx2048m -XX:MaxMetaspaceSize=1024m` into android/gradle.properties, once, in place of the
 * template's 512m Metaspace (the 0.9.0 production build's daemon expired at 512m as the build ended and its client hung).
 * Pinned through the real `expo config --type introspect` (every plugin's gradle.properties mod run, as prebuild runs it).
 */
import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, symlinkSync } from "node:fs";
import { join, resolve } from "node:path";
import { describe, expect, it } from "vitest";

// @ts-expect-error -- plain CommonJS config plugin run by `expo prebuild` (no types)
import plugin from "../apps/mobile/plugins/with-gradle-memory.js";

const MOBILE = resolve(process.cwd(), "apps/mobile");      // vitest runs from app/
const EXPO = resolve(process.cwd(), "node_modules/.bin/expo");
type Prop = { type: string; key?: string; value?: string };
/** only what a process needs: a build's exported variables must not reach the config (r8-config.test.ts, T749) */
const env = Object.fromEntries(["PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "USER", "SHELL"]
  .filter((k) => process.env[k] !== undefined).map((k) => [k, process.env[k]]));

describe("T1058: the daemon's Metaspace in the generated gradle.properties", () => {
  it("prebuild writes the jvmargs once, with Metaspace 1024m and the heap at 2048m", () => {
    // (review of #155) introspect builds on an EXISTING android/gradle.properties, which a checkout that ran prebuild
    // has (with the 1024m line already in it): read through a copy of the project with no android/ (the template's)
    const root = mkdtempSync(join(MOBILE, "..", ".t1058-"));
    try {
      for (const name of readdirSync(MOBILE)) {
        if (name !== "android" && name !== "ios") symlinkSync(join(MOBILE, name), join(root, name));
      }
      expect(existsSync(join(root, "android"))).toBe(false);
      check(execFileSync(EXPO, ["config", "--type", "introspect", "--json"], { cwd: root, env, encoding: "utf8" }));
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
  function check(json: string) {
    const props: Prop[] = JSON.parse(json)._internal.modResults.android.gradleProperties;
    const jvm = props.filter((p) => p.type === "property" && p.key === "org.gradle.jvmargs");
    expect(jvm).toEqual([{ type: "property", key: "org.gradle.jvmargs", value: "-Xmx2048m -XX:MaxMetaspaceSize=1024m" }]);
  }

  it("a template value is replaced, never duplicated, and the rest is kept", () => {
    const tpl: Prop[] = [{ type: "comment", value: "x" },
      { type: "property", key: "org.gradle.jvmargs", value: "-Xmx2048m -XX:MaxMetaspaceSize=512m" },
      { type: "property", key: "org.gradle.parallel", value: "true" }];
    const out: Prop[] = plugin.setJvmargs(tpl);
    expect(out.filter((p) => p.key === plugin.KEY)).toEqual([{ type: "property", key: plugin.KEY, value: plugin.JVMARGS }]);
    expect(out.filter((p) => p.key !== plugin.KEY)).toEqual([tpl[0], tpl[2]]);
    expect(plugin.setJvmargs(out)).toEqual(out);
  });

  it("the plugin is in app.json's list (prebuild runs only what is listed)", () => {
    const app = JSON.parse(readFileSync(resolve(MOBILE, "app.json"), "utf8"));
    expect(app.expo.plugins).toContain("./plugins/with-gradle-memory");
  });
});
