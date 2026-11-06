/**
 * (T250) R8 STAYS OFF IN RELEASE BUILDS, as under Expo SDK 57. SDK 58's prebuild template writes
 * `android.enableMinifyInReleaseBuilds=true` into android/gradle.properties (SDK 57's left it unset: off), and the first
 * SDK 58 APK died at start on the emulator: R8 had stripped what WorkManager's Room database is made from by reflection
 * ("Unable to get provider androidx.startup.InitializationProvider … Failed to create an instance of class
 * androidx.work.impl.WorkDatabase"; WorkManager is expo-background-task's). Shrinking the app's Java/Kotlin is a separate
 * decision with its own keep rules and device walk (MapLibre's patched classes, pdfium, the bg-download module), not part
 * of an SDK bump. Applied by `expo prebuild` (android/ is generated, never edited).
 */
const { withGradleProperties } = require("expo/config-plugins");

const KEY = "android.enableMinifyInReleaseBuilds";

module.exports = function withNoMinify(config) {
  return withGradleProperties(config, (c) => {
    const items = c.modResults.filter((p) => !(p.type === "property" && p.key === KEY));
    items.push({ type: "property", key: KEY, value: "false" });
    c.modResults = items;
    return c;
  });
};
