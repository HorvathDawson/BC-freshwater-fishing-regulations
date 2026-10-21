import { describe, expect, it, vi } from "vitest";

describe("NATIVE_DRIVER", () => {
  it("is off on the web (no native animated module) and on for iOS and Android", async () => {
    for (const [os, want] of [["web", false], ["ios", true], ["android", true]] as const) {
      vi.resetModules();
      vi.doMock("react-native", () => ({ Platform: { OS: os } }));
      expect((await import("./motion")).NATIVE_DRIVER, os).toBe(want);
    }
    vi.doUnmock("react-native");
  });
});
