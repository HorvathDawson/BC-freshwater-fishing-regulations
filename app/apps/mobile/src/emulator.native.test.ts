/** (T603 item 5) An Android emulator is told from a phone by the build's own fingerprint and model. */
import { beforeEach, describe, expect, it, vi } from "vitest";

const platform = vi.hoisted(() => ({ OS: "android", constants: {} as Record<string, string> }));
vi.mock("react-native", () => ({ Platform: platform }));

import { onEmulator } from "./emulator.native";

describe("onEmulator (native)", () => {
  beforeEach(() => { platform.OS = "android"; platform.constants = {}; });

  it.each([
    ["google/sdk_gphone64_arm64/emu64a:15/AE3A/1:userdebug/dev-keys", "sdk_gphone64_arm64"],
    ["Android/sdk_google_phone_x86/generic_x86:9/PSR1/1:userdebug/test-keys", "Android SDK built for x86"],
  ])("an emulator image (%s)", (Fingerprint, Model) => {
    platform.constants = { Fingerprint, Model };
    expect(onEmulator()).toBe(true);
  });

  it("a real phone is not one", () => {
    platform.constants = { Fingerprint: "google/oriole/oriole:14/AP2A/1:user/release-keys", Model: "Pixel 6" };
    expect(onEmulator()).toBe(false);
  });

  it("iOS is never reported here (the simulator build tags itself)", () => {
    platform.OS = "ios";
    platform.constants = { Fingerprint: "generic" };
    expect(onEmulator()).toBe(false);
  });
});
