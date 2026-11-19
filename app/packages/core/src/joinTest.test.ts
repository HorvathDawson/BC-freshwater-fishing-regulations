/** T901: Join the test: the mailto, the voice, and which stores still offer it. */
import { describe, expect, it } from "vitest";
import { joinStores, joinTestAsk } from "./joinTest";
import { WEB_SPOTS_WORDS } from "./spotWords";

describe("join the test (T901)", () => {
  it("mailto to the contact with the subject and the account ask; null where no address is set", () => {
    const a = joinTestAsk("android", "support@canifishthis.ca");
    expect(a.mailto).toContain("mailto:support@canifishthis.ca?subject=Join%20the%20Android%20test&body=");
    expect(joinTestAsk("iphone", "support@canifishthis.ca").subject).toBe("Join the iPhone test");
    expect(joinTestAsk("android", null).mailto).toBeNull();
    expect(joinTestAsk("android", "https://example.com/contact").mailto).toBeNull();
  });
  it("speaks as I, never we", () => {
    expect(JSON.stringify(WEB_SPOTS_WORDS.join)).not.toMatch(/\bwe\b|\bour\b/i);
  });
  it("a store drops out once its page is public", () => {
    expect(joinStores(false, false)).toEqual(["android", "iphone"]);
    expect(joinStores(true, false)).toEqual(["iphone"]);
    expect(joinStores(true, true)).toEqual([]);
  });
});
