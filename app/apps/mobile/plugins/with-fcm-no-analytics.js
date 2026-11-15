/**
 * (T739) FIREBASE FOR PUSH DELIVERY ONLY. Added by app.config.js only with EXPO_PUBLIC_FCM=1 (the owner's
 * google-services.json in the build). The app links firebase-messaging alone (expo-notifications' dependency) and no
 * firebase-analytics; these manifest flags keep Analytics collection off even if a later library brings it in:
 *   firebase_analytics_collection_deactivated  true   (Analytics never collects; cannot be turned on at run time)
 *   google_analytics_adid_collection_enabled   false  (no advertising id)
 * Applied by `expo prebuild` (android/ is generated, never edited).
 */
const { AndroidConfig, withAndroidManifest } = require("expo/config-plugins");

const FLAGS = { firebase_analytics_collection_deactivated: "true", google_analytics_adid_collection_enabled: "false" };

module.exports = function withFcmNoAnalytics(config) {
  return withAndroidManifest(config, (c) => {
    const app = AndroidConfig.Manifest.getMainApplicationOrThrow(c.modResults);
    for (const [name, value] of Object.entries(FLAGS)) AndroidConfig.Manifest.addMetaDataItemToMainApplication(app, name, value);
    return c;
  });
};
