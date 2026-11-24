/**
 * (T901, owner 2026-11-19: "the web app get the app should link to both google play and apple depending on button you
 * click") THE TWO STORE ADDRESSES, in one place. The web's "Get the app" buttons (WebSpotsNotice) and the website's
 * "Open in the app" (tools/seo gauges.ts) read these; nothing else spells a store URL.
 *
 * Neither store page is public yet (`PLAY_PUBLIC` is the same switch for Play; `tools/check-store-pages.mjs` curls both and
 * fails when a flag and its page disagree). The App Store page is NOT public yet (the iPhone app is in TestFlight only; `https://apps.apple.com/app/id6835000455`
 * answered 404 on 2026-11-19). While `APP_STORE_PUBLIC` is false the App Store button is a greyed "coming soon" and
 * never links. TO TURN IT ON: set `APP_STORE_PUBLIC = true` (Play: `PLAY_PUBLIC = true`) (this one line) once the page loads; the button then links.
 * The Apple ID comes from App Store Connect (GET /v1/apps, bundle id ca.canifishthis.app).
 */
export const PLAY_STORE_URL = "https://play.google.com/store/apps/details?id=ca.canifishthis.app";
export const APP_STORE_URL = "https://apps.apple.com/app/id6835000455";
export const APP_STORE_PUBLIC = false;
/** Play's page 404s to the public while the app is in closed testing (checked 2026-11-19). Both flags go true at launch. */
export const PLAY_PUBLIC = false;

/**
 * (T921, owner 2026-11-19: "in the webapp where it says join apple trial we could have this link") THE PUBLIC TESTFLIGHT
 * INVITE: anyone with the link joins the iPhone test, no email needed. While the App Store page is not public, "Join the
 * iPhone test" opens it (`getAppBlock`). Null: back to the email ask.
 */
export const TESTFLIGHT_URL: string | null = "https://testflight.apple.com/join/x7uVRt8j";

/** The App Store address to link to, or null while the page is not public (the button says the iPhone app is coming). */
export const appStoreLink = (): string | null => (APP_STORE_PUBLIC ? APP_STORE_URL : null);

/** The Play address to link to, or null while the listing is not public (the button says the Android app is coming). */
export const playStoreLink = (): string | null => (PLAY_PUBLIC ? PLAY_STORE_URL : null);

/**
 * (T244, T1039) THE STORE CONTACT every app build is made with (`EXPO_PUBLIC_CONTACT` in apps/mobile's `export:web` and the
 * store build scripts): "Join the Android test" emails it. The public gauge page's "Get the app" block is the web app's own
 * (ui-native `GetAppBlock`), so it uses this too; `tools/seo/gauges.test.ts` holds the web export's value to it.
 */
export const STORE_CONTACT = "support@canifishthis.ca";
