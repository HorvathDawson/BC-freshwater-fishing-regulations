// @vitest-environment node
/** T1163: the app config declares the background modes, usage strings and privacy reasons the built plist is checked for. */
import { describe, expect, it } from "vitest";
// @ts-expect-error plain .mjs tool
import { expectedFromConfig } from "../../../tools/check-built-plist.mjs";
import app from "../app.json";

describe("the app config", () => {
  const e = expectedFromConfig() as { modes: string[]; identifiers: string[]; strings: Record<string, string> };
  it("declares background fetch, processing and remote notifications, and the task identifier", () => {
    expect([...e.modes].sort()).toEqual(["fetch", "processing", "remote-notification"]);
    expect(e.identifiers).toEqual(["com.expo.modules.backgroundtask.processing"]);
  });
  it("declares the camera, photo and location usage strings, whole", () => {
    expect(e.strings).toEqual({
      NSLocationWhenInUseUsageDescription: "Allow Can I Fish This? to centre the map on where you are and to pin a spot where you are standing. It stays on this device.",
      NSPhotoLibraryUsageDescription: "Allow Can I Fish This? to add your photos to a spot you saved. They stay on this device.",
      NSCameraUsageDescription: "Allow Can I Fish This? to take a photo for a spot you saved. It stays on this device.",
    });
  });
  it("lists the plugins that add the modes, and the disk-space privacy reason", () => {
    const names = app.expo.plugins.map((p) => (Array.isArray(p) ? p[0] : p));
    expect(names).toEqual(expect.arrayContaining(["expo-background-task", "expo-notifications", "expo-location", "expo-image-picker"]));
    const disk = app.expo.ios.privacyManifests.NSPrivacyAccessedAPITypes.find((t) => t.NSPrivacyAccessedAPIType === "NSPrivacyAccessedAPICategoryDiskSpace");
    expect(disk?.NSPrivacyAccessedAPITypeReasons).toEqual(["E174.1"]);
  });
});
