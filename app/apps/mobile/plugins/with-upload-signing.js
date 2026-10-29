/**
 * (T267) Google Play UPLOAD-key signing for release builds, applied by `expo prebuild` (android/ is generated, never edited).
 *
 * When the environment names a properties file in CANIFISHTHIS_UPLOAD_PROPERTIES (build-apk.sh --play sets it to
 * ~/.canifishthis/upload-keystore.properties: storeFile, storePassword, keyAlias, keyPassword, storeType), the release
 * build type signs with that key. Without it the release build keeps Expo's debug key, so the sideloaded staging APK
 * still installs over the previous one. The key and its password live outside the repo and are never committed.
 * A template change that moves the anchors throws here, at prebuild, rather than silently shipping the debug key to Play.
 */
const { withAppBuildGradle } = require("expo/config-plugins");

const MARK = "// (T267) canifishthis upload signing";
const HEADER = `${MARK}
def canifishthisUpload = System.getenv('CANIFISHTHIS_UPLOAD_PROPERTIES')
def canifishthisUploadProps = new Properties()
if (canifishthisUpload) {
    def f = new File(canifishthisUpload)
    if (!f.isFile()) throw new GradleException("CANIFISHTHIS_UPLOAD_PROPERTIES names a missing file: " + canifishthisUpload)
    f.withInputStream { canifishthisUploadProps.load(it) }
}
`;
const UPLOAD_CONFIG = `
        if (canifishthisUpload) {
            upload {
                storeFile file(canifishthisUploadProps['storeFile'])
                storePassword canifishthisUploadProps['storePassword']
                keyAlias canifishthisUploadProps['keyAlias']
                keyPassword canifishthisUploadProps['keyPassword']
                if (canifishthisUploadProps['storeType']) storeType canifishthisUploadProps['storeType']
            }
        }`;

function apply(src) {
  if (src.includes(MARK)) return src;
  const release = /(release \{\n(?:\s*\/\/[^\n]*\n)*\s*)signingConfig signingConfigs\.debug/;
  if (!/^android \{/m.test(src) || !/signingConfigs \{/.test(src) || !release.test(src)) {
    throw new Error("with-upload-signing: app/build.gradle anchors not found (Expo template changed); update the plugin");
  }
  return src
    .replace(/^android \{/m, `${HEADER}\nandroid {`)
    .replace(/signingConfigs \{/, `signingConfigs {${UPLOAD_CONFIG}`)
    .replace(release, "$1signingConfig canifishthisUpload ? signingConfigs.upload : signingConfigs.debug");
}

module.exports = function withUploadSigning(config) {
  return withAppBuildGradle(config, (c) => {
    if (c.modResults.language !== "groovy") throw new Error("with-upload-signing: expected a Groovy app/build.gradle");
    c.modResults.contents = apply(c.modResults.contents);
    return c;
  });
};
module.exports.apply = apply;
