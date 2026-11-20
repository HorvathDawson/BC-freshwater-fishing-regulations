/**
 * (T913) Play refused review on the implied, required `android.hardware.screen.portrait` (the portrait lock). The plugin
 * (`apps/mobile/plugins/with-optional-portrait.js`) declares it optional; this pins the declaration WHOLE and that app.json
 * applies the plugin, so a removed plugin or a `required="true"` fails here, not at Play's review.
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

// @ts-expect-error -- plain CommonJS run by `expo prebuild` (no types)
import { setManifest } from "../apps/mobile/plugins/with-optional-portrait.js";

describe("with-optional-portrait (T913)", () => {
  it("declares the portrait feature optional, beside the features already declared", () => {
    const other = { $: { "android:name": "android.hardware.camera", "android:required": "false" } };
    expect(setManifest({ application: [], "uses-feature": [other] })["uses-feature"]).toEqual([
      other,
      { $: { "android:name": "android.hardware.screen.portrait", "android:required": "false" } },
    ]);
    expect(setManifest({ application: [] })["uses-feature"]).toEqual([
      { $: { "android:name": "android.hardware.screen.portrait", "android:required": "false" } },
    ]);
  });

  it("refuses a manifest that already declares it (one owner)", () => {
    const m = { "uses-feature": [{ $: { "android:name": "android.hardware.screen.portrait" } }] };
    expect(() => setManifest(m)).toThrow(/already declares android.hardware.screen.portrait/);
  });

  // (T931 E) a prebuilt android/ already holds the plugin's line, and `expo config --type introspect` re-reads it:
  // its OWN line, exactly and alone, is accepted unchanged; any other declaration is still refused
  it("accepts its own line, alone, unchanged; refuses it with another attribute, required, or twice", () => {
    const own = { $: { "android:name": "android.hardware.screen.portrait", "android:required": "false" } };
    const other = { $: { "android:name": "android.hardware.camera", "android:required": "false" } };
    const m = { application: [], "uses-feature": [other, own] };
    expect(setManifest(m)).toBe(m);
    const required = { $: { "android:name": "android.hardware.screen.portrait", "android:required": "true" } };
    const extra = { $: { ...own.$, "tools:node": "replace" } };
    for (const fs of [[required], [extra], [own, own], [own, required]]) {
      expect(() => setManifest({ "uses-feature": fs })).toThrow(/already declares android.hardware.screen.portrait/);
    }
  });

  it("app.json applies the plugin while it locks portrait", () => {
    const app = JSON.parse(readFileSync(resolve(__dirname, "../apps/mobile/app.json"), "utf8")).expo;
    expect(app.orientation).toBe("portrait");
    expect(app.plugins).toContain("./plugins/with-optional-portrait");
  });
});
