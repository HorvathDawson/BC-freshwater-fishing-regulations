/** (T723 G8) "Directions" goes to Apple Maps on an iPhone and to Google Maps everywhere else (Android unchanged). */
import { describe, expect, it } from "vitest";
import { directionsUrl } from "./spotWords";

describe("directionsUrl", () => {
  it("iOS: the Apple Maps universal link", () => {
    expect(directionsUrl("ios", 49.25, -123.1)).toBe("http://maps.apple.com/?daddr=49.25,-123.1");
  });
  it("Android and web: the Google Maps link, byte for byte what the app opened before", () => {
    const g = "https://www.google.com/maps/dir/?api=1&destination=49.25,-123.1";
    expect([directionsUrl("android", 49.25, -123.1), directionsUrl("web", 49.25, -123.1)]).toEqual([g, g]);
  });
});
