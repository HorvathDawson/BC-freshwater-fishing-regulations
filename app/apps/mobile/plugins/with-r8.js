/**
 * (T1003) R8 IS ON FOR EVERY ANDROID RELEASE BUILD, production included: it shrinks, optimizes and obfuscates the
 * Java/Kotlin (Play scores all three, "DEX code optimization"; with R8 off it scored Obfuscation 1%, under its 25% line,
 * owner 2026-11-22). This plugin writes `android.enableMinifyInReleaseBuilds=true` into android/gradle.properties (any
 * template value replaced, never duplicated) and appends `r8-keep-rules.pro` (each rule with its reason) to
 * android/app/proguard-rules.pro. Resource shrinking stays off (r8-keep-rules.pro says why). Applied by `expo prebuild`
 * (android/ is generated, never edited).
 *
 * History: T250 kept R8 off (SDK 58's template turned it on and the first APK died at start: R8 stripped WorkManager's
 * Room database; keep rule 1 is that fix); T749 proved it on the emulator benches and Play build 33 behind a trial flag,
 * `CANIFISHTHIS_R8`, which production refused. That flag is gone: any value is refused (it would read as a switch that
 * no longer exists, and "0" would not turn R8 off).
 *
 * THE ONE ESCAPE: `CANIFISHTHIS_NO_R8="<reason>"` builds with R8 off, for a test-bed comparison build only (the reason is
 * printed). app.config.js refuses it in a production config and deploy/staging/build-apk.sh refuses it for every build
 * it makes (each can reach a phone), as `--stock-maplibre`. A set-but-blank value is refused (no reason given).
 *
 * (T1027) LIBRARY RULES R8 SKIPS: for the libraries in IGNORED_RULES (each shipped one whole-package `{ *; }` keep rule
 * that froze it from shrinking, optimization and renaming) the release build type gets AGP's
 * `optimization { keepRules { ignoreFrom(...) } }`, and r8-keep-rules.pro (rules 8-10) restates only what each reaches by
 * name, each walked on the emulator (the T1027 ticket). Written into android/app/build.gradle with the keep rules (R8
 * on); a template without the release `minifyEnabled` line is refused, never skipped.
 */
const { readFileSync, writeFileSync } = require("node:fs");
const { join } = require("node:path");
const { withAppBuildGradle, withGradleProperties, withDangerousMod } = require("expo/config-plugins");

const KEY = "android.enableMinifyInReleaseBuilds";
const GONE = "CANIFISHTHIS_R8";
const ESCAPE = "CANIFISHTHIS_NO_R8";
const KEEP_FILE = join(__dirname, "r8-keep-rules.pro");
const MARK = "# (T749, T1003) canifishthis R8 keep rules (plugins/r8-keep-rules.pro)";
/** (T1027) external dependencies (group:artifact) whose consumer keep rules R8 skips; r8-keep-rules.pro rules 8-10 */
const IGNORED_RULES = [
  "host.exp.exponent:expo.modules.notifications",   // rule 8
  "host.exp.exponent:expo.modules.taskmanager",     // rule 9
  "host.exp.exponent:expo.modules.backgroundtask",  // rule 9
  "io.legere:pdfiumandroid",                        // rule 10
];
const GRADLE_MARK = "// (T1027) plugins/with-r8.js: these libraries' own keep rules are skipped (r8-keep-rules.pro restates them)";
const RELEASE_MINIFY = /^([ \t]*)minifyEnabled enableMinifyInReleaseBuilds[ \t]*$/m;

/**
 * The escape's reason, or null when R8 is on (the default). Refuses the removed flag (any value) and a blank reason:
 * never a guess, never a silent off.
 */
function r8OffReason(env = process.env) {
  if (env[GONE] !== undefined) {
    throw new Error(`with-r8: ${GONE} was removed (T1003): R8 is on for every release build. Unset it; a test-bed ` +
      `comparison build with R8 off is ${ESCAPE}="<reason>"`);
  }
  const v = env[ESCAPE];
  if (v === undefined) return null;
  if (!v.trim()) throw new Error(`with-r8: ${ESCAPE} needs a reason: ${ESCAPE}="<why this build has R8 off>"`);
  return v;
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

/**
 * app/build.gradle with `optimization { keepRules { ignoreFrom(...) } }` under the release build type's minifyEnabled
 * line, once. Refused when the template has no such line (the libraries' rules would silently come back).
 */
function ignoreLibraryRules(src, ids = IGNORED_RULES) {
  if (src.includes(GRADLE_MARK)) return src;
  const m = RELEASE_MINIFY.exec(src);
  if (!m) throw new Error("with-r8: android/app/build.gradle has no release `minifyEnabled enableMinifyInReleaseBuilds` " +
    "line to put the T1027 ignoreFrom under: the Expo template changed; place it by hand in plugins/with-r8.js");
  const at = m.index + m[0].length;
  const list = ids.map((i) => `'${i}'`).join(", ");
  return `${src.slice(0, at)}\n${m[1]}${GRADLE_MARK}\n${m[1]}optimization { keepRules { ignoreFrom(${list}) } }${src.slice(at)}`;
}

module.exports = function withR8(config) {
  const off = r8OffReason();
  if (off !== null) console.warn(`with-r8: R8 OFF for this build (${ESCAPE}, test-bed comparison only): ${off}`);
  config = withGradleProperties(config, (c) => {
    c.modResults = setMinify(c.modResults, off === null);
    return c;
  });
  if (off !== null) return config;
  config = withAppBuildGradle(config, (c) => {
    c.modResults.contents = ignoreLibraryRules(c.modResults.contents);
    return c;
  });
  return withDangerousMod(config, ["android", (c) => {
    const f = join(c.modRequest.platformProjectRoot, "app", "proguard-rules.pro");
    writeFileSync(f, appendKeepRules(readFileSync(f, "utf8")));
    return c;
  }]);
};
Object.assign(module.exports, { r8OffReason, setMinify, appendKeepRules, ignoreLibraryRules, KEY, GONE, ESCAPE, KEEP_FILE,
  MARK, IGNORED_RULES, GRADLE_MARK });
