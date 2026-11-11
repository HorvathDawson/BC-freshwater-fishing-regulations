/**
 * (T385, owner 2026-11-04: "in the app we should have a share button on a river that sends the website link") SHARE A
 * WATER: the words of the water header's Share control, and the link it sends — the PRODUCTION web origin and the page's
 * own reload address (`@app/ui` `linkSearch`, the address bar's function), so the link opens the same water, part, view
 * and day for whoever gets it.
 *
 * (T624, owner 2026-11-10: "yes make preview right") WHERE IT OPENS: the WEBSITE, in a browser. Today no link opens the
 * installed app (the phone has a custom scheme only; Android app links are T625). A water that has a page is shared as
 * that page, `/water/<slug>` (`shareWaterUrl`), whose preview card is the water's own; only a water with no page falls
 * back to the app's start link `/?regs=…`, whose preview is the generic home card, and the Share SAYS so
 * (`SHARE_WORDS.noPage`).
 */
import { APP_NAME } from "./menu/words";

/**
 * The public site (production): every shared link and the privacy page are on it, whatever build shares it (T404: one
 * site for every environment; the staging site is noindex and not for anglers). The one place core spells a host
 * (`tools/no-env-hosts.test.ts`).
 */
export const SITE_ORIGIN = "https://canifishthis.ca";

/**
 * (T625) ANDROID APP LINKS: the hosts whose `/water/<slug>` pages (and the plain `/` start link) open the installed app,
 * the app's package, and the SHA-256 of the key a Play install is signed with (Play app signing; public information,
 * recorded in the T625 ticket by the owner 2026-11-11). Three places must agree, and `appLinks.test.ts` holds them to it:
 * the site build writes `/.well-known/assetlinks.json` from these (tools/seo `assetLinksFile`), `apps/mobile/app.json`
 * declares the intent filter for the same hosts, and `@app/ui` `parseAppLink` reads the same hosts.
 */
export const APP_LINK_HOSTS: readonly string[] = ["canifishthis.ca", "staging.canifishthis.ca"];
export const APP_PACKAGE = "ca.canifishthis.app";
export const PLAY_SIGNING_SHA256 = "A0:73:C5:A9:63:D3:20:2E:9D:18:FA:95:A8:11:AD:51:1F:6F:C4:BB:74:CF:84:27:71:23:EC:7E:24:BF:A8:DC";

/**
 * (T514) WHERE A SHARE PAGE'S "Open in the app" GOES. null: the Play listing is not public yet, so the page offers the
 * website and a greyed "Open in the app · coming soon" (today). When the listing is public (T244/T371) this is its
 * address (`https://play.google.com/store/apps/details?id=ca.canifishthis.app`) and the one edit turns the button on;
 * a phone with the app installed opens the app from the page's own link (T625), one without lands on the listing.
 */
export const OPEN_IN_APP_URL: string | null = null;

/** The link a page's address makes: the site, then the start link's query ("?regs=…"; "" is the map). */
export const shareUrl = (search: string): string => `${SITE_ORIGIN}/${search}`;

/** (T624) The link to a water's own page: the site, then `/water/<slug>` and the query the page reads (`@app/ui` `waterPageAddress`). */
export const shareWaterUrl = (address: string): string => `${SITE_ORIGIN}${address}`;

export const SHARE_WORDS = {
  /** the control's name, said aloud */
  label: "Share this water",
  /** the share sheet's title (Android's chooser, the browser's) */
  title: APP_NAME,
  /** the web without a share sheet: the link went to the clipboard */
  copied: "Link copied",
  /** the share sheet or the clipboard failed (not a dismissed sheet: that says nothing) */
  failed: "This link could not be shared. Try again.",
  /** (T624) a water with no page of its own was shared as the map's link: said beside "Link copied" or alone */
  noPage: "This water has no page of its own, so the link opens it on the map.",
  /** a page with no address of its own to send (an unnamed reach in a release that cannot place it) */
  none: "This page has no link to share.",
} as const;

/** (T530) The words of the Share on a Conditions gauge card; the sheet's title and the "Link copied"/failure words are the water's. */
export const GAUGE_SHARE_WORDS = {
  label: "Share this gauge",
} as const;
