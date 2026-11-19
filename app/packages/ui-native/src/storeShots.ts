/**
 * (T319, owner 2026-11-19: "can all images be generated with the webapp?") THE STORE-SHOTS WEB BUILD draws the phone's
 * screens where the website draws its own: the Spots tab and a spot (the website shows "get the app"), and the Help page's
 * "Offline and updates" lines (the website's say it needs a signal). Only a web export built with
 * `EXPO_PUBLIC_STORE_SHOTS=1` turns it on (`apps/mobile` config), for `app/tools/store-shots.mjs` to photograph; the site
 * that is deployed never does, and the phone app is the phone already.
 */
let on = false;
/** (the app shell, once at start) a store-shots build draws the phone's screens on the web */
export const drawPhoneScreensOnWeb = (yes: boolean) => { on = yes; };
/** does this web build draw the phone's screens? */
export const phoneScreensOnWeb = (): boolean => on;
