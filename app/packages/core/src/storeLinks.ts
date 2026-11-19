/**
 * (T901, owner 2026-11-19: "the web app get the app should link to both google play and apple depending on button you
 * click") THE TWO STORE ADDRESSES, in one place. The web's "Get the app" buttons (WebSpotsNotice) and the website's
 * "Open in the app" (tools/seo gauges.ts) read these; nothing else spells a store URL.
 *
 * The App Store page is NOT public yet (the iPhone app is in TestFlight only; `https://apps.apple.com/app/id6835000455`
 * answered 404 on 2026-11-19). While `APP_STORE_PUBLIC` is false the App Store button is a greyed "coming soon" and
 * never links. TO TURN IT ON: set `APP_STORE_PUBLIC = true` (this one line) once the page loads; the button then links.
 * The Apple ID comes from App Store Connect (GET /v1/apps, bundle id ca.canifishthis.app).
 */
export const PLAY_STORE_URL = "https://play.google.com/store/apps/details?id=ca.canifishthis.app";
export const APP_STORE_URL = "https://apps.apple.com/app/id6835000455";
export const APP_STORE_PUBLIC = false;

/** The App Store address to link to, or null while the page is not public (the button says the iPhone app is coming). */
export const appStoreLink = (): string | null => (APP_STORE_PUBLIC ? APP_STORE_URL : null);
