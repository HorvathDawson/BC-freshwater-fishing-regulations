// @vitest-environment jsdom
/** (T1163) the browser's `here()`: a refusal or a timeout is the environment's (shown, never reported); other errors are not. */
import { describe, expect, it, vi } from "vitest";
import { isEnvironment } from "@app/core";

const fail = (code: number) => ({ code, message: `geo ${code}`, PERMISSION_DENIED: 1, POSITION_UNAVAILABLE: 2, TIMEOUT: 3 });

async function hereAfter(code: number) {
  vi.resetModules();
  Object.defineProperty(navigator, "geolocation", { configurable: true, value: {
    getCurrentPosition: (_ok: unknown, err: (e: unknown) => void) => err(fail(code)), watchPosition: () => 0, clearWatch: () => {},
  } });
  const { locate } = await import("./locate.web");
  return locate!.here().catch((e: unknown) => e);
}

describe("locate.web here()", () => {
  it("code 1 (refused) and code 3 (timeout) are the environment's, and keep their code", async () => {
    for (const code of [1, 3]) {
      const e = await hereAfter(code);
      expect([isEnvironment(e), (e as { code: number }).code]).toEqual([true, code]);
    }
  });
  it("code 2 (position unavailable) is left as it came", async () => {
    expect(isEnvironment(await hereAfter(2))).toBe(false);
  });
});
