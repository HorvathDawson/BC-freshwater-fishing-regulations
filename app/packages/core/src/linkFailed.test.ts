/** (T723 G9) the line a refused link says: the whole sentence, per kind of link. */
import { describe, expect, it } from "vitest";
import { linkFailedLine } from "./linkFailed";

describe("linkFailedLine", () => {
  it("names the mail draft, the directions, or the page's host", () => {
    expect(linkFailedLine("mailto:a@b.ca?subject=x")).toBe("Could not open an email draft. Is a mail app set up on this phone?");
    expect(linkFailedLine("http://maps.apple.com/?daddr=1,2")).toBe("Could not open directions in a maps app.");
    expect(linkFailedLine("https://www.google.com/maps/dir/?api=1&destination=1,2")).toBe("Could not open directions in a maps app.");
    expect(linkFailedLine("https://www2.gov.bc.ca/assets/x.pdf?y=1")).toBe("Could not open www2.gov.bc.ca.");
  });
  it("a link of no known shape still says so", () => {
    expect(linkFailedLine("tel:123")).toBe("Could not open that link.");
  });
});
