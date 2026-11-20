/**
 * (T913) THE PORTRAIT LOCK IS NOT A HARDWARE REQUIREMENT. Applied by `expo prebuild` (android/ is generated, never edited).
 *
 * app.json `orientation: "portrait"` gives the activity `screenOrientation="portrait"`, and Google Play reads that as an
 * implied, REQUIRED `android.hardware.screen.portrait`. Play then refuses to send a release for review ("Required hardware
 * feature declarations are restricting your app's availability", owner 2026-11-19 on 0.8.2 build 37) and would hide the
 * app from devices without that feature. The app still runs in portrait everywhere; it only must not demand the feature to
 * install. So the manifest declares the feature explicitly as optional, which overrides the implied one.
 *
 * A manifest that already declares the feature in any OTHER way (another plugin, a template change) is refused here, at
 * prebuild: two owners of one declaration would leave which one Play reads to merge order. Its OWN line, exactly as it
 * writes it and alone, is accepted unchanged (T931 E): a prebuilt `android/` already holds it, and `expo config --type
 * introspect` re-reads that manifest (app/tools/r8-config.test.ts threw on its own declaration).
 */
const { withAndroidManifest } = require("expo/config-plugins");

const FEATURE = "android.hardware.screen.portrait";
const OWN = { "android:name": FEATURE, "android:required": "false" };

/** Is `f` this plugin's own declaration, attribute for attribute? */
function isOwn(f) {
  const a = f?.$ ?? {};
  return Object.keys(a).length === Object.keys(OWN).length && Object.entries(OWN).every(([k, v]) => a[k] === v);
}

/** The manifest with the portrait feature declared optional; refuses a manifest that already declares it otherwise. */
function setManifest(manifest) {
  const features = manifest["uses-feature"] ?? [];
  const mine = features.filter((f) => f?.$?.["android:name"] === FEATURE);
  if (mine.length === 1 && isOwn(mine[0])) return manifest;
  if (mine.length) {
    throw new Error(`with-optional-portrait: the manifest already declares ${FEATURE}; one owner only (T913)`);
  }
  return { ...manifest, "uses-feature": [...features, { $: { ...OWN } }] };
}

module.exports = function withOptionalPortrait(config) {
  return withAndroidManifest(config, (c) => {
    c.modResults.manifest = setManifest(c.modResults.manifest);
    return c;
  });
};
module.exports.setManifest = setManifest;
module.exports.FEATURE = FEATURE;
