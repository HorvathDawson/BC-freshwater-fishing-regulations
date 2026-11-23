import { webPlatformLine } from "@app/core";

/** (T436) the web's platform line; the phone twin is platformLine.native.ts. (T1005) A crawler's or a driven browser's
 *  run says so ("web (crawler)", "web (automated)", core `webPlatformLine`), so its crash reports are told from a reader's. */
export function platformLine(): string {
  const nav = typeof navigator === "undefined" ? null : navigator;
  return webPlatformLine(nav?.userAgent, nav?.webdriver);
}
