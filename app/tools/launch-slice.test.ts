/**
 * (T741) The boot's two paint calls (packages/data/src/release/boot.ts): the phone's held-set open asks with
 * `paintSliceFor(o)` (40 ms slices at a launch, 8 ms when an update is applied with the map on screen), the web's first
 * paint keeps the default (its turns are a MessageChannel's, no timer floor). Here in tools/ because it reads the source,
 * which packages/data's own tests may not (layers.json); the behaviour half is `launchSlice.test.ts` beside boot.ts.
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

describe("(T741) the boot asks for the launch slice on the phone's path only", () => {
  it("loader.paint() on the web's first paint, loader.paint(paintSliceFor(o)) on the held-set open", () => {
    // code lines only (a doc comment names `loader.paint()` too)
    const code = readFileSync(new URL("../packages/data/src/release/boot.ts", import.meta.url), "utf8").split("\n")
      .filter((l) => !/^\s*(\*|\/\/|\/\*\*)/.test(l)).join("\n");
    const calls = [...code.matchAll(/loader\.paint\(([^)]*\)?)\)/g)].map((c) => c[1]);
    expect(calls).toEqual(["", "paintSliceFor(o)"]);
  });
});
