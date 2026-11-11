/**
 * T625 / T514 — the site's half of "a shared link opens the installed app" and the share page's "Open in the app" switch.
 * The file Android verifies (`/.well-known/assetlinks.json`) comes out of the real site build; the intent filter in
 * app.json names the hosts the file is served from, and the same hosts `parseAppLink` reads.
 */
import { readFileSync } from "node:fs";
import { beforeAll, describe, expect, it } from "vitest";
import { APP_LINK_HOSTS, APP_PACKAGE, PLAY_SIGNING_SHA256 } from "@app/core";
import { openRegsFixture } from "@app/data/release/node";
import { buildAll, type Built } from "./build";
import { TODAY } from "./fixture";
import { assetLinksFile, headersFile } from "./hosting";
import { openInAppControl } from "./gauges";

let built: Built;
beforeAll(async () => { built = await buildAll({ loader: openRegsFixture(), on: TODAY, gauges: null }); }, 300_000);

describe("T625 assetlinks.json", () => {
  it("the site build writes it, naming the app's package and the Play app-signing fingerprint the owner recorded", () => {
    const text = built.files.get(".well-known/assetlinks.json")!;
    expect(text).toBe(assetLinksFile());
    expect(JSON.parse(text)).toEqual([{
      relation: ["delegate_permission/common.handle_all_urls"],
      target: { namespace: "android_app", package_name: "ca.canifishthis.app",
                sha256_cert_fingerprints: ["A0:73:C5:A9:63:D3:20:2E:9D:18:FA:95:A8:11:AD:51:1F:6F:C4:BB:74:CF:84:27:71:23:EC:7E:24:BF:A8:DC"] },
    }]);
    expect(PLAY_SIGNING_SHA256).toMatch(/^([0-9A-F]{2}:){31}[0-9A-F]{2}$/);
  });

  it("is served as JSON with no redirect (a _headers entry; the staging and production hosts get the same file)", () => {
    expect(headersFile()).toMatch(/\/\.well-known\/assetlinks\.json\n {2}Content-Type: application\/json/);
    expect(built.files.get("_headers")).toBe(headersFile());
    // no redirect rule can catch it: the removed-water redirects are /water and /region paths only
    expect(built.files.get("_redirects")!).not.toContain(".well-known");
  });

  it("app.json's intent filter is autoVerify, for exactly the hosts the site serves it from, on the pages the app reads", () => {
    const app = JSON.parse(readFileSync(new URL("../../apps/mobile/app.json", import.meta.url), "utf8")).expo;
    expect(app.android.package).toBe(APP_PACKAGE);
    const [f, ...more] = app.android.intentFilters;
    expect(more).toEqual([]);
    expect(f).toMatchObject({ action: "VIEW", autoVerify: true, category: ["BROWSABLE", "DEFAULT"] });
    const data = f.data as { scheme: string; host: string; pathPrefix?: string; path?: string }[];
    expect([...new Set(data.map((d) => d.host))].sort()).toEqual([...APP_LINK_HOSTS].sort());
    for (const h of APP_LINK_HOSTS) {
      expect(data.filter((d) => d.host === h).map((d) => d.pathPrefix ?? d.path).sort()).toEqual(["/", "/water/"]);
    }
    expect(data.every((d) => d.scheme === "https")).toBe(true);
  });
});

describe("T514 share page 'Open in the app'", () => {
  it("is the greyed 'coming soon' until a listing address is set; one value turns it into the listing's link", () => {
    expect(openInAppControl(null)).toBe(`<span class="app soon" aria-disabled="true">Open in the app · coming soon</span>`);
    expect(openInAppControl("https://play.google.com/store/apps/details?id=ca.canifishthis.app"))
      .toBe(`<a class="app" href="https://play.google.com/store/apps/details?id=ca.canifishthis.app">Open in the app</a>`);
    expect(openInAppControl()).toContain("coming soon");     // the shipped default: the config value is null
  });
});
