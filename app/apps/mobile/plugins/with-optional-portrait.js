/**
 * (T913) THE PORTRAIT LOCK IS NOT A HARDWARE REQUIREMENT. Applied by `expo prebuild` (android/ is generated, never edited).
 *
 * app.json `orientation: "portrait"` gives the activity `screenOrientation="portrait"`, and Google Play reads that as an
 * implied, REQUIRED `android.hardware.screen.portrait`. Play then refuses to send a release for review ("Required hardware
 * feature declarations are restricting your app's availability", owner 2026-11-19 on 0.8.2 build 37) and would hide the
 * app from devices without that feature. The app still runs in portrait everywhere; it only must not demand the feature to
 * install. So the manifest declares the feature explicitly as optional, which overrides the implied one.
 *
 * A manifest that already declares the feature (another plugin, a template change) is refused here, at prebuild: two
 * owners of one declaration would leave which one Play reads to merge order.
 */
const { withAndroidManifest } = require("expo/config-plugins");

const FEATURE = "android.hardware.screen.portrait";

/** The manifest with the portrait feature declared optional; refuses a manifest that already declares it. */
function setManifest(manifest) {
  const features = manifest["uses-feature"] ?? [];
  if (features.some((f) => f?.$?.["android:name"] === FEATURE)) {
    throw new Error(`with-optional-portrait: the manifest already declares ${FEATURE}; one owner only (T913)`);
  }
  return { ...manifest, "uses-feature": [...features, { $: { "android:name": FEATURE, "android:required": "false" } }] };
}

module.exports = function withOptionalPortrait(config) {
  return withAndroidManifest(config, (c) => {
    c.modResults.manifest = setManifest(c.modResults.manifest);
    return c;
  });
};
module.exports.setManifest = setManifest;
module.exports.FEATURE = FEATURE;
