/**
 * (T1211 review) A WATER'S OWN MAP VIEW DRAWS EVERY CLASS: the main map's isolate (the key's tap, T377) is never handed to
 * it. `waterMapStatus` (WaterModeHost) drops `only`; this holds that the water's view is handed that, never `mapStatus`
 * itself (the function is tested in packages/ui-native/src/T1211.map-key.test.tsx).
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

describe("the water's Map view and the main map's isolate", () => {
  it("is handed the day's status whole (`waterMapStatus`), never the isolate", () => {
    const host = readFileSync(new URL("../packages/ui-native/src/shell/WaterModeHost.tsx", import.meta.url), "utf8");
    expect(host).toContain("status: waterMapStatus(mapStatus)");
    expect(host).not.toMatch(/status: mapStatus\b/);
  });
});
