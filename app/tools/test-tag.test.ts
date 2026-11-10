/**
 * (T595) THE TEST TAG IS SENT BY EVERY TEST-BED / EMULATOR / SIMULATOR BUILD, from one place (`testbed/lib/test-tag.sh`),
 * and by no build an owner's phone or a store gets. A run without it filed its reports as real errors (11 "Failed to bind
 * to the service" kinds, a `/data/local/tmp` SecurityException).
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const root = resolve(__dirname, "../..");
const read = (p: string) => readFileSync(resolve(root, p), "utf8");
const code = (p: string) => read(p).split("\n").filter((l) => !/^\s*#/.test(l)).join("\n");

describe("the test tag", () => {
  it("the one place exports it", () => {
    expect(code("app/tools/testbed/lib/test-tag.sh")).toMatch(/export EXPO_PUBLIC_TEST_REPORTS=1/);
  });
  it.each(["app/tools/testbed/session.sh", "deploy/staging/build-ios-sim.sh"])(
    "%s sources it unconditionally and sets no tag of its own", (p) => {
      const c = code(p);
      const at = c.indexOf("lib/test-tag.sh");
      expect(at).toBeGreaterThan(-1);
      expect(c.slice(0, at).split("\n").pop()).toMatch(/^\s*source "\$\w+\/app\/tools\/testbed\/$/);   // not inside an `if [[ … ]]; then` line
      expect(c).not.toMatch(/EXPO_PUBLIC_TEST_REPORTS/);
    });
  it.each(["deploy/staging/build-apk.sh", "deploy/staging/build-ios-testflight.sh"])(
    "%s (an owner's or a store's build) never tags", (p) => {
      expect(code(p)).not.toMatch(/export[^\n]*EXPO_PUBLIC_TEST_REPORTS=1|EXPO_PUBLIC_TEST_REPORTS=1 /);
    });
});
