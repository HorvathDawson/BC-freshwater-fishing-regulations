/** (T723 G8) Directions opens the platform's maps link through the one `openLink`. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { Linking } from "react-native";
import { openDirections } from "./openLink";

afterEach(() => vi.restoreAllMocks());

describe("openDirections", () => {
  it("iOS opens Apple Maps; Android opens Google Maps (unchanged)", async () => {
    const open = vi.spyOn(Linking, "openURL").mockResolvedValue(true);
    await openDirections({ lat: 49.25, lon: -123.1 }, "ios");
    await openDirections({ lat: 49.25, lon: -123.1 }, "android");
    expect(open.mock.calls.map((c) => c[0])).toEqual([
      "http://maps.apple.com/?daddr=49.25,-123.1",
      "https://www.google.com/maps/dir/?api=1&destination=49.25,-123.1",
    ]);
  });
});
