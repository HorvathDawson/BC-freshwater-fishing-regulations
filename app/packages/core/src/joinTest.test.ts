/** T901: Join the test: the mailto, the voice, and which stores still offer it. */
import { describe, expect, it } from "vitest";
import { getAppBlock, joinTestAsk } from "./joinTest";
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
  const C = "support@canifishthis.ca", P = "https://play.google.com/x", A = "https://apps.apple.com/app/id1";
  it("both coming: one line naming both, a lead, and a Join button per platform", () => {
    const b = getAppBlock(null, null, C, false);
    expect(b.coming).toBe("Coming soon to Google Play and the App Store.");
    expect(b.lead).toBe("Want to try it early?");
    expect(b.slots.map((s) => [s.kind, s.store, s.label])).toEqual([["join", "android", "Join the Android test"], ["join", "iphone", "Join the iPhone test"]]);
  });
  it("a live store: its join becomes its store button in the same spot; the line names only the other", () => {
    const b = getAppBlock(P, null, C, false);
    expect(b.coming).toBe("Coming soon to the App Store.");
    expect(b.slots.map((s) => [s.kind, s.store, s.label])).toEqual([["store", "android", "Google Play"], ["join", "iphone", "Join the iPhone test"]]);
    expect(getAppBlock(null, A, C, false).coming).toBe("Coming soon to Google Play.");
  });
  it("both live: no line, no lead, two store buttons", () => {
    const b = getAppBlock(P, A, C, false);
    expect([b.coming, b.lead, b.slots.map((s) => s.label)]).toEqual([null, null, ["Google Play", "App Store"]]);
  });
  it("no contact and no Report: no join button (never a dead one); with Report: both", () => {
    expect(getAppBlock(null, null, null, false).slots).toEqual([]);
    expect(getAppBlock(null, null, null, true).slots).toHaveLength(2);
  });
});
