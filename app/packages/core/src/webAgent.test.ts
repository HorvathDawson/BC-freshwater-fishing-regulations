import { describe, expect, it } from "vitest";
import { webPlatformLine } from "./webAgent";

const CHROME = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const GOOGLEBOT = "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.204 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";

describe("T1005 webPlatformLine", () => {
  it("a reader's browser is plain web", () => {
    expect(webPlatformLine(CHROME, false)).toBe("web");
    expect(webPlatformLine(null, undefined)).toBe("web");
  });
  it("a search engine's renderer and Lighthouse are crawlers", () => {
    expect(webPlatformLine(GOOGLEBOT, false)).toBe("web (crawler)");
    expect(webPlatformLine(`${CHROME} Chrome-Lighthouse`, false)).toBe("web (crawler)");
    expect(webPlatformLine("Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)", false)).toBe("web (crawler)");
  });
  it("a driven browser (webdriver, headless Chrome) is automated", () => {
    expect(webPlatformLine(CHROME, true)).toBe("web (automated)");
    expect(webPlatformLine(CHROME.replace("Chrome/", "HeadlessChrome/"), false)).toBe("web (automated)");
  });
  it("every line starts with web (tools/sourcemaps.mjs picks the web's maps by that)", () => {
    for (const l of [webPlatformLine(GOOGLEBOT, true), webPlatformLine(CHROME, true), webPlatformLine(CHROME, false)])
      expect(l.startsWith("web")).toBe(true);
  });
});
