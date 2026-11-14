/** (T723 G8) Directions opens the platform's maps link through the one `openLink`. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { Linking } from "react-native";
import { setFailureSink } from "@app/core";
import { openDirections, openLink, setLinkFailedSink } from "./openLink";

afterEach(() => vi.restoreAllMocks());

describe("openLink (T723 G9): a refused open is reported AND shown", () => {
  it("hands the sink the whole line, and still reports", async () => {
    vi.spyOn(Linking, "openURL").mockRejectedValue(new Error("no handler"));
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const lines: string[] = [];
    const undo = setLinkFailedSink((l) => lines.push(l));
    const reported: unknown[] = [];
    const unsink = setFailureSink((e) => reported.push(e));
    await openLink("mailto:a@b.ca?subject=x");
    await openLink("https://www2.gov.bc.ca/x.pdf");
    undo(); unsink();
    expect(lines).toEqual(["Could not open an email draft. Is a mail app set up on this phone?", "Could not open www2.gov.bc.ca."]);
    expect(reported).toHaveLength(2);
  });
});

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
