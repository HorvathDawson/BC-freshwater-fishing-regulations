// WHICH SCENES NEED THE STORE-SHOTS BUILD (T1072): the ONE place that says so. A scene listed here is drawn only by a web export built with
// EXPO_PUBLIC_STORE_SHOTS=1 (the phone's own Spots and Help screens, `@app/ui-native` storeShots.ts); the deployed websites draw their own
// "get the app" notice and website Help lines instead. Every other scene is the same screen on a deployed site, so with `--web <url>` it is
// shot from that site and no export is built. play-shots.mjs picks each scene's site by this table; store-shots.mjs builds / serves the
// export only when a scene needing it is asked for. T1076's guarantee stands: a scene listed here is never shot from a site without the marker.
export const NEEDS_STORE_BUILD = {
  '07-saved-spot': "the phone's Spots tab with the seeded spot (the website shows a \"get the app\" notice there)",
  '20-help': "the app's Help page with its Offline and updates lines (the website's Help carries the website's lines)",
};
export const needsBuild = (scene) => Object.hasOwn(NEEDS_STORE_BUILD, scene);
