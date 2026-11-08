/**
 * (T385, owner 2026-11-04: "in the app we should have a share button on a river that sends the website link") SHARE A
 * WATER: the words of the water header's Share control, and the link it sends — the PRODUCTION web origin and the page's
 * own reload address (`@app/ui` `linkSearch`, the address bar's function), so the link opens the same water, part, view
 * and day for whoever gets it, in a browser or the app.
 */
import { APP_NAME } from "./menu/words";

/**
 * The public site (production): every shared link and the privacy page are on it, whatever build shares it (T404: one
 * site for every environment; the staging site is noindex and not for anglers). The one place core spells a host
 * (`tools/no-env-hosts.test.ts`).
 */
export const SITE_ORIGIN = "https://canifishthis.ca";

/** The link a page's address makes: the site, then the start link's query ("?regs=…"; "" is the map). */
export const shareUrl = (search: string): string => `${SITE_ORIGIN}/${search}`;

export const SHARE_WORDS = {
  /** the control's name, said aloud */
  label: "Share this water",
  /** the share sheet's title (Android's chooser, the browser's) */
  title: APP_NAME,
  /** the web without a share sheet: the link went to the clipboard */
  copied: "Link copied",
  /** the share sheet or the clipboard failed (not a dismissed sheet: that says nothing) */
  failed: "This link could not be shared. Try again.",
  /** a page with no address of its own to send (an unnamed reach in a release that cannot place it) */
  none: "This page has no link to share.",
} as const;
