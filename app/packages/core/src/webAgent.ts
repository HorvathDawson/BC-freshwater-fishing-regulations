/**
 * (T1005) WHO IS RUNNING THE WEB APP, for the platform line a crash report carries ("web", "web (crawler)", "web
 * (automated)"). Measured 2026-11-22: one install (a916f6f81cd61f75) sent 11 of production's 20 defect kinds (202 of
 * their 220 reports, and most of the network / expected ones), on every web version 0.8.1 → 0.8.5. Each of its reports
 * is stamped 00:00:0x UTC of its day by a pinned clock (every other install stamps the real time); its install id is
 * the same across days (a seeded Math.random); and it read data releases production had stopped serving (resources
 * from the renderer's own cache). That is a search engine's page renderer, not a reader. Its errors are still sent,
 * nothing is dropped (T595), but named, so the crash server can class them apart from a reader's.
 */
const CRAWLER = /\b(Googlebot|Google-InspectionTool|GoogleOther|Storebot-Google|AdsBot-Google|bingbot|BingPreview|DuckDuckBot|YandexBot|Baiduspider|Applebot|facebookexternalhit|Chrome-Lighthouse|crawler|spider)\b/i;

/** The web's platform line, from the browser's user agent and `navigator.webdriver`. */
export function webPlatformLine(ua: string | null | undefined, webdriver: boolean | null | undefined): string {
  if (ua && CRAWLER.test(ua)) return "web (crawler)";
  if (webdriver === true || (ua ?? "").includes("HeadlessChrome")) return "web (automated)";
  return "web";
}
