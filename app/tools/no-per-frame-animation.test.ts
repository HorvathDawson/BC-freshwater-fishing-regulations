import { describe, expect, it } from "vitest";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

/**
 * Lives in tools/ rather than beside the components: it reads source off disk, and a
 * component package that may import node:fs is a component package that can import
 * anything. The boundary checker caught exactly that, which is the checker working.
 *
 * This asserts the ONE invariant that holds across every phone component, and nothing
 * about how any single one is written — an earlier version matched literal lines out of
 * FishSpinner.tsx and broke the moment the component was rewritten, without a single
 * thing about the app getting worse. A test that fails on a rewrite is a test that
 * discourages rewrites.
 */
const DIR = fileURLToPath(new URL("../packages/ui-native/src", import.meta.url));
/** Comments explain the bug being avoided and name it; the assertions are about CODE. */
const sources = readdirSync(DIR)
  .filter((f) => /\.tsx?$/.test(f) && !f.endsWith(".test.tsx") && !f.endsWith(".test.ts"))
  .map((f) => [f, readFileSync(join(DIR, f), "utf8")
    .replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "")] as const);

describe("the phone components cannot stutter", () => {
  it("has sources to check", () => expect(sources.length).toBeGreaterThan(3));

  it("asks the JS thread for no frames, anywhere", () => {
    // The archived FishLoader drove a canvas from requestAnimationFrame, so it froze on
    // exactly the thread that was busy — a loader that stops when there is something to
    // wait for reads as a hung app. Its replacement bakes those frames instead.
    for (const [name, src] of sources)
      expect(src, name).not.toMatch(/requestAnimationFrame|setInterval|getContext/);
  });

  it("hands every loop to the platform", () => {
    const all = sources.flatMap(([, s]) => s.match(/useNativeDriver:\s*(true|false)/g) ?? []);
    expect(all.length).toBeGreaterThan(0);
    expect(all.every((d) => d.endsWith("true"))).toBe(true);
  });

  it("drives only transform and opacity — the two a platform driver can run without JS", () => {
    for (const [name, src] of sources) {
      const animated = src.match(/^\s*(\w+):\s*\w+\.interpolate\(/gm) ?? [];
      for (const line of animated) {
        const prop = line.trim().split(":")[0]!;
        expect(["transform", "opacity", "rotate", "translateX", "translateY", "scale"],
               `${name}: ${prop}`).toContain(prop);
      }
    }
  });
});
