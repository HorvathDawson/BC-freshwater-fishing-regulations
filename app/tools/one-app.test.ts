/**
 * One app shell (T70, audit wave 1 item 1.6; AGENTS rule 26). Desktop is a layout class in @app/ui-native (T45:
 * `layoutClassFor`, `useLayoutClass`), not a second app with its own DOM components: `apps/web` was an empty
 * placeholder for that second app, and `formFactorFor`, its code split, had no caller. Both are gone; this pins that
 * the workspace and the boundary rules do not grow them back unnoticed.
 */
import { describe, expect, it } from "vitest";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const APP = new URL("..", import.meta.url).pathname;

describe("one app shell", () => {
  it("apps/ holds only the Expo app (native + mobile/desktop web)", () => {
    expect(readdirSync(join(APP, "apps")).filter((n) => !n.startsWith("."))).toEqual(["mobile"]);
  });

  it("layers.json names a layer for every boundary, and none for a second web app", () => {
    const spec = JSON.parse(readFileSync(join(APP, "layers.json"), "utf8"));
    expect(Object.keys(spec.layers)).toEqual([
      "packages/core", "packages/data", "packages/ui", "packages/map", "apps/mobile", "packages/ui-native",
      "packages/data/src/bundle/drivers", "packages/data/src/release/drivers", "packages/data/src/spots",
    ]);
  });
});
