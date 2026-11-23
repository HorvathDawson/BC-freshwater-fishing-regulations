/**
 * (T1007) CHROME'S "`StorageType.persistent` is deprecated" ON canifishthis.ca IS NOT OURS: reproduced 2026-11-22 in a clean
 * Chromium (no extensions, DevTools protocol `Audits`), the `PersistentQuotaType` deprecation's source is Cloudflare's own
 * `/cdn-cgi/challenge-platform/scripts/jsd/main.js` (its JavaScript Detections, injected by Cloudflare into every HTML page of
 * the zone: same origin, so Lighthouse files it "1st party"). This pins that OUR code never names the old quota API: a
 * storage need goes through `navigator.storage` (`persist()` / `estimate()`).
 */
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const APP = fileURLToPath(new URL("..", import.meta.url));
const SPELLINGS = "webkitPersistentStorage|webkitTemporaryStorage|webkitStorageInfo|StorageType\\.(persistent|temporary)|" +
  "(window|self|globalThis)\\.(PERSISTENT|TEMPORARY)\\b|requestQuota *\\(";
const OLD = new RegExp(SPELLINGS);

describe("(T1007) the deprecated quota API", () => {
  it("is named nowhere in the app's own code (packages, apps, workers, tools)", () => {
    const r = spawnSync("git", ["grep", "-l", "-E", SPELLINGS, "--", "packages", "apps", "workers", "tools", ":!tools/storage-api.test.ts"],
                        { cwd: APP, encoding: "utf8" });
    expect(r.status, r.stderr).toBe(1);   // git grep: 1 = no match (0 = a match, 2+ = it failed)
    expect(r.stdout).toBe("");
  });
  it("the pattern catches each old spelling (and not navigator.storage)", () => {
    for (const s of ["navigator.webkitPersistentStorage.requestQuota(1, f)", "window.webkitStorageInfo", "StorageType.persistent", "window.PERSISTENT"])
      expect(OLD.test(s), s).toBe(true);
    expect(OLD.test("await navigator.storage.persist(); navigator.storage.estimate()")).toBe(false);
  });
});
