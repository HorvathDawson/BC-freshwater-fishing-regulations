/**
 * T625 / T514 — the site's half of "a shared link opens the installed app" and the share page's "Open in the app" switch.
 * The file Android verifies (`/.well-known/assetlinks.json`) comes out of the real site build; the intent filter in
 * app.json names the hosts the file is served from, and the same hosts `parseAppLink` reads.
 */
import { readFileSync } from "node:fs";
import { beforeAll, describe, expect, it } from "vitest";
import { APP_LINK_HOSTS, APP_PACKAGE, APP_STORE_URL, appStoreLink, playStoreLink, PLAY_SIGNING_SHA256, PLAY_STORE_URL } from "@app/core";
import { openRegsFixture } from "@app/data/release/node";
import { buildAll, readIosTeamId, type Built } from "./build";
import { TODAY } from "./fixture";
import { appSiteAssociationFile, assetLinksFile, headersFile } from "./hosting";
import { openInAppControl } from "./gauges";

let built: Built;
beforeAll(async () => { built = await buildAll({ loader: openRegsFixture(), on: TODAY, gauges: null, bare: true }); }, 300_000);

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

describe("T724 apple-app-site-association", () => {
  const TEAM = "ABCDE12345";   // a test value, never a real account
  it("with a Team ID the site build writes it: appID <team>.<bundle>, the same pages as the Android filter, JSON headers", async () => {
    const withIos = await buildAll({ loader: openRegsFixture(), on: TODAY, gauges: null, iosTeamId: TEAM, bare: true });
    const text = withIos.files.get(".well-known/apple-app-site-association")!;
    expect(text).toBe(appSiteAssociationFile(TEAM));
    expect(JSON.parse(text)).toEqual({ applinks: { details: [{ appIDs: ["ABCDE12345.ca.canifishthis.app"], components: [{ "/": "/water/*" }, { "/": "/" }] }] } });
    expect(headersFile()).toMatch(/\/\.well-known\/apple-app-site-association\n {2}Content-Type: application\/json/);
    expect(withIos.files.get("_redirects")!).not.toContain(".well-known");
    // Android is untouched by the iOS file
    expect(withIos.files.get(".well-known/assetlinks.json")).toBe(assetLinksFile());
  }, 300_000);

  it("without a Team ID no file is written (never a made-up one); a malformed id is refused", () => {
    expect(built.files.has(".well-known/apple-app-site-association")).toBe(false);
    expect(() => appSiteAssociationFile("TEAMID")).toThrow(/not an Apple Team ID/);
  });

  it("the Team ID is read from CIFT_IOS_TEAM_ID or the owner's file; none -> null, bad -> throws", () => {
    expect(readIosTeamId({ CIFT_IOS_TEAM_ID: " ABCDE12345\n" }, "/nonexistent")).toBe("ABCDE12345");
    expect(readIosTeamId({}, "/nonexistent/ios-team-id")).toBeNull();
    expect(() => readIosTeamId({ CIFT_IOS_TEAM_ID: "nope" }, "/nonexistent")).toThrow(/not 10 letters\/digits/);
  });

  it("app.json declares associatedDomains for exactly the hosts the file serves from", () => {
    const app = JSON.parse(readFileSync(new URL("../../apps/mobile/app.json", import.meta.url), "utf8")).expo;
    expect([...app.ios.associatedDomains].sort()).toEqual(APP_LINK_HOSTS.map((h) => `applinks:${h}`).sort());
  });
});

describe("T514 / T901 share page 'Get the app' block", () => {
  const C = "support@canifishthis.ca", A = "https://apps.apple.com/app/id6835000455";
  it("neither public (the shipped default): one plain line, a lead, and a mailto button per platform; no span, no disabled control", () => {
    const html = openInAppControl(null, null, C, null);
    expect(html).toContain(`<p class="small coming">Coming soon to Google Play and the App Store.</p>`);
    expect(html).toContain(`<p class="small joins">Want to try it early?</p>`);
    expect(html).toContain(`<a class="app" href="mailto:${C}?subject=Join%20the%20Android%20test&amp;body=`);
    expect(html).toContain(`<a class="app" href="mailto:${C}?subject=Join%20the%20iPhone%20test&amp;body=`);
    expect(html).toContain("The%20email%20on%20my%20Google%20account%20is");
    expect(html).toContain("The%20email%20on%20my%20Apple%20ID%20is");
    expect(html).not.toContain("<span");
    expect(appStoreLink()).toBeNull(); expect(playStoreLink()).toBeNull();      // both pages 404 today
  });
  it("no contact: the line, and no button", () => {
    expect(openInAppControl(null, null, null, null)).toBe(`<p class="small coming">Coming soon to Google Play and the App Store.</p>`);
  });
  it("a store going live: its link in the join button's spot, the line and the lead name only what is left", () => {
    const html = openInAppControl(PLAY_STORE_URL, null, C, null);
    expect(html).toContain(`<a class="app" href="${PLAY_STORE_URL}">Google Play</a>`);
    expect(html).toContain("Coming soon to the App Store.");
    expect(html).not.toContain("Android test");
    expect(html).toContain("Join the iPhone test");
  });
  it("both live: two store links, no line", () => {
    expect(openInAppControl(PLAY_STORE_URL, A, C, null)).toBe(
      `<div class="stores"><a class="app" href="${PLAY_STORE_URL}">Google Play</a><a class="app" href="${A}">App Store</a></div>`);
  });
  it("the addresses are core's: Play by package, the App Store by the Apple ID", () => {
    expect(PLAY_STORE_URL).toBe(`https://play.google.com/store/apps/details?id=${APP_PACKAGE}`);
    expect(APP_STORE_URL).toMatch(/^https:\/\/apps\.apple\.com\/app\/id\d+$/);
  });
  it("(T921) the default iPhone join is the TestFlight invite, a plain link with no email card", () => {
    const html = openInAppControl(null, null, C);
    expect(html).toContain(`<a class="app" href="https://testflight.apple.com/join/x7uVRt8j">Join the iPhone test</a>`);
    expect(html).not.toContain(`id="join-iphone"`);
    expect(html).toContain(`id="join-android"`);
  });
});
