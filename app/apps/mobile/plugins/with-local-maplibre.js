/**
 * (T420) TEST-BED ONLY: build the app against a locally built MapLibre Native Android SDK (the feature-state patch,
 * app/tools/maplibre-patch/README.md) instead of the stock Maven Central one. Loaded by app.config.js ONLY when
 * MLN_LOCAL_MAVEN is set (a production build with it set is refused there), so every other build is untouched.
 *
 * MLN_LOCAL_MAVEN = the local Maven repo build.sh publishes (`<work dir>/m2`). It must hold exactly one version of
 * org.maplibre.gl:android-sdk-vulkan. This plugin adds the repo to every project's repositories; app.config.js hands
 * that version (`localVersion`) to @maplibre/maplibre-react-native's own plugin as `android.nativeVersion` (its plugin
 * rewrites every `org.maplibre.reactnative.*` gradle property, so a property set here would be dropped).
 */
const { existsSync, readdirSync } = require("node:fs");
const { join, isAbsolute } = require("node:path");
const { withProjectBuildGradle } = require("expo/config-plugins");

const ARTIFACT = ["org", "maplibre", "gl", "android-sdk-vulkan"];
const MARK = "// with-local-maplibre (MLN_LOCAL_MAVEN)";

function localVersion(repo) {
  if (!isAbsolute(repo)) throw new Error(`with-local-maplibre: MLN_LOCAL_MAVEN must be an absolute path (got ${repo})`);
  const dir = join(repo, ...ARTIFACT);
  if (!existsSync(dir)) throw new Error(`with-local-maplibre: no ${ARTIFACT.join(".")} in ${repo} (run app/tools/maplibre-patch/build.sh)`);
  const versions = readdirSync(dir, { withFileTypes: true }).filter((d) => d.isDirectory()).map((d) => d.name);
  if (versions.length !== 1) throw new Error(`with-local-maplibre: ${dir} must hold exactly one version, has [${versions.join(", ")}]`);
  const [version] = versions;
  if (!existsSync(join(dir, version, `android-sdk-vulkan-${version}.aar`))) throw new Error(`with-local-maplibre: no AAR in ${join(dir, version)}`);
  return version;
}

module.exports = function withLocalMaplibre(config) {
  const repo = process.env.MLN_LOCAL_MAVEN;
  if (!repo) throw new Error("with-local-maplibre: loaded without MLN_LOCAL_MAVEN (app.config.js adds it only when set)");
  localVersion(repo);
  return withProjectBuildGradle(config, (c) => {
    if (c.modResults.contents.includes(MARK)) return c;
    // added to every project's repositories; the local version exists only there (Maven Central has no such version)
    c.modResults.contents += `\n${MARK}\nallprojects {\n  repositories {\n    maven { url = uri(${JSON.stringify(`file://${repo}`)}) }\n  }\n}\n`;
    return c;
  });
};
module.exports.localVersion = localVersion;
