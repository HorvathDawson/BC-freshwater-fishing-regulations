/**
 * (T978, owner 2026-11-22: "every Android build uses the pinned build") THE ANDROID CONFIG SAYS WHICH MAPLIBRE IT GETS.
 * Every shipped Android build carries the pinned upstream MapLibre (app/tools/maplibre-android/pin.env) through
 * MLN_LOCAL_MAVEN, which deploy/staging/build-apk.sh and the test bed set. Anything else — a bare `expo prebuild`, `pnpm
 * android`, an EAS build — reached the binding's STOCK SDK from Maven Central without a word (T512: ~6 fps on a Pixel).
 *
 * Runs as an Android mod, so only an Android prebuild (EAS, `expo prebuild --platform android`, `expo run:android`) meets
 * it; the iPhone build never does (its MapLibre is the iOS pod's, T893). Then:
 *   - PRODUCTION (EXPO_PUBLIC_APP_ENV=production or the EAS production profile) without the pinned repo: REFUSED, the fix
 *     named. A production Android build is made by deploy/prod/build-apk.sh, which sets it.
 *   - any other build: a WARNING, never a stop (chosen: `pnpm android` in a fresh checkout has no built AAR, and refusing
 *     would block everyday dev; everything that ships goes through build-apk.sh, which refuses on its own).
 */
const { readFileSync } = require("node:fs");
const { join } = require("node:path");
const { withGradleProperties } = require("expo/config-plugins");

const PIN = join(__dirname, "../../../tools/maplibre-android/pin.env");

/** { MLN_COMMIT, MLN_VERSION } from pin.env */
function readPin(file = PIN) {
  const out = {};
  for (const line of readFileSync(file, "utf8").split("\n")) {
    const m = /^([A-Z_]+)=(\S+)$/.exec(line);
    if (m) out[m[1]] = m[2];
  }
  if (!out.MLN_COMMIT || !out.MLN_VERSION) throw new Error(`with-pinned-maplibre: ${file} names no MLN_COMMIT / MLN_VERSION`);
  return out;
}

/** null when the build carries the pin, else { refuse: bool, message }. `version` = the local repo's (null: none set). */
function maplibreProblem({ production, repo, version, pin }) {
  const fix = `set MLN_LOCAL_MAVEN=<repo>/cache/maplibre/${pin.MLN_VERSION} (build-apk.sh does; get it with `
    + "bash app/tools/maplibre-android/build.sh <work dir>, which downloads the stored build when "
    + "maplibre-android.lock.json records it as published; until then: "
    + "scripts/heavy.sh mln-aar 30 -- bash app/tools/maplibre-android/build.sh --compile <work dir>)";
  if (!repo) {
    return { refuse: production, message: `this Android build would use the STOCK MapLibre from Maven Central, not the pinned `
      + `upstream ${pin.MLN_VERSION} (T978): ${fix}` };
  }
  if (version !== pin.MLN_VERSION) {
    return { refuse: production, message: `MLN_LOCAL_MAVEN=${repo} holds MapLibre ${version}, not the pinned ${pin.MLN_VERSION} `
      + `(app/tools/maplibre-android/pin.env, T978): ${fix}` };
  }
  return null;
}

module.exports = function withPinnedMaplibre(config, { production } = {}) {
  // gradle.properties: an Android mod `expo config --type introspect` runs too (app/tools/r8-config.test.ts reads it)
  return withGradleProperties(config, (c) => {
    const repo = process.env.MLN_LOCAL_MAVEN || null;
    const version = repo ? require("./with-local-maplibre").localVersion(repo) : null;
    const problem = maplibreProblem({ production: !!production, repo, version, pin: readPin() });
    if (problem?.refuse) throw new Error(`app.config.js (production Android): ${problem.message}`);
    if (problem) console.warn(`[with-pinned-maplibre] WARNING: ${problem.message}`);
    return c;
  });
};
module.exports.readPin = readPin;
module.exports.maplibreProblem = maplibreProblem;
module.exports.PIN = PIN;
