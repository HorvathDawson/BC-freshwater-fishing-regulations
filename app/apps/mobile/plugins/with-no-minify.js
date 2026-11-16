/**
 * (T250) R8 STAYS OFF IN RELEASE BUILDS, as under Expo SDK 57. SDK 58's prebuild template writes
 * `android.enableMinifyInReleaseBuilds=true` into android/gradle.properties (SDK 57's left it unset: off), and the first
 * SDK 58 APK died at start on the emulator: R8 had stripped what WorkManager's Room database is made from by reflection
 * ("Unable to get provider androidx.startup.InitializationProvider … Failed to create an instance of class
 * androidx.work.impl.WorkDatabase"; WorkManager is expo-background-task's). Shrinking the app's Java/Kotlin is a separate
 * decision with its own keep rules and device walk (MapLibre's patched classes, pdfium, the bg-download module), not part
 * of an SDK bump. Applied by `expo prebuild` (android/ is generated, never edited).
 *
 * (T749 TRIAL) `CANIFISHTHIS_R8=1` at prebuild turns R8 ON for release builds instead (minify only: resource shrinking
 * stays off, see r8-keep-rules.pro) and appends `r8-keep-rules.pro` (each rule with its reason) to
 * android/app/proguard-rules.pro. Unset, "" or "0": exactly the T250 behaviour above (`tools/r8-config.test.ts` pins
 * that the introspected config is the same as with the variable absent). Any other value is refused, never guessed.
 * app.config.js refuses it in a production build until T749's phone check is recorded.
 */
const { readFileSync, writeFileSync } = require("node:fs");
const { join } = require("node:path");
const { withGradleProperties, withDangerousMod } = require("expo/config-plugins");

const KEY = "android.enableMinifyInReleaseBuilds";
const FLAG = "CANIFISHTHIS_R8";
const KEEP_FILE = join(__dirname, "r8-keep-rules.pro");
const MARK = "# (T749) canifishthis R8 keep rules (plugins/r8-keep-rules.pro)";

/** true: R8 on; false: off (T250). A value that is neither is refused (no silent fallback). */
function r8Enabled(env = process.env) {
  const v = env[FLAG];
  if (v === undefined || v === "" || v === "0") return false;
  if (v === "1") return true;
  throw new Error(`with-no-minify: ${FLAG} must be unset, "0" or "1" (got ${JSON.stringify(v)})`);
}

/** gradle.properties items with the minify key set to on/off (any template value replaced, never duplicated) */
function setMinify(items, on) {
  const out = items.filter((p) => !(p.type === "property" && p.key === KEY));
  out.push({ type: "property", key: KEY, value: on ? "true" : "false" });
  return out;
}

/** proguard-rules.pro with the keep rules appended once (a second prebuild pass does not add them twice) */
function appendKeepRules(src, rules = readFileSync(KEEP_FILE, "utf8")) {
  if (src.includes(MARK)) return src;
  return `${src.replace(/\n*$/, "\n")}\n${MARK}\n${rules}`;
}

module.exports = function withNoMinify(config) {
  const on = r8Enabled();
  config = withGradleProperties(config, (c) => {
    c.modResults = setMinify(c.modResults, on);
    return c;
  });
  if (!on) return config;
  return withDangerousMod(config, ["android", (c) => {
    const f = join(c.modRequest.platformProjectRoot, "app", "proguard-rules.pro");
    writeFileSync(f, appendKeepRules(readFileSync(f, "utf8")));
    return c;
  }]);
};
Object.assign(module.exports, { r8Enabled, setMinify, appendKeepRules, KEY, FLAG, KEEP_FILE, MARK });
