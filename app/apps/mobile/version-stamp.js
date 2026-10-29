/**
 * (T251) THE VERSION STAMP — run by metro.config.js when Metro starts (the dev server, `expo export`, an EAS build's
 * bundling), before any file is transformed, so babel-preset-expo inlines the values like any `EXPO_PUBLIC_*`:
 *
 *   EXPO_PUBLIC_APP_VERSION       app.json `expo.version` — THE semver (package.json repeats it; a test holds them equal)
 *   EXPO_PUBLIC_APP_BUILD_NUMBER  app.json `android.versionCode` (= `ios.buildNumber`, the same test)
 *   EXPO_PUBLIC_GIT_SHA           EAS's `EAS_BUILD_GIT_COMMIT_HASH`, else `git rev-parse HEAD`; unset outside a checkout
 *   EXPO_PUBLIC_BUILD_DATE        today (UTC), the day the bundle was made
 *
 * A value already in the environment wins (a CI pins its own). `@app/core` `resolveAppVersion` reads them at run time.
 * Plain CommonJS: Metro's config is loaded by Node before any TypeScript exists.
 */
const { execSync } = require("node:child_process");

function gitSha(cwd) {
  try { return execSync("git rev-parse HEAD", { cwd, stdio: ["ignore", "pipe", "ignore"] }).toString().trim(); }
  catch { return ""; }
}

/** The stamp's variables for `app` (app.json's `expo`), given the environment and a way to read the commit. */
function versionStamp(app, env, readSha, today = new Date()) {
  return {
    EXPO_PUBLIC_APP_VERSION: env.EXPO_PUBLIC_APP_VERSION || app.version,
    EXPO_PUBLIC_APP_BUILD_NUMBER: env.EXPO_PUBLIC_APP_BUILD_NUMBER || String(app.android?.versionCode ?? ""),
    EXPO_PUBLIC_GIT_SHA: env.EXPO_PUBLIC_GIT_SHA || env.EAS_BUILD_GIT_COMMIT_HASH || readSha(),
    EXPO_PUBLIC_BUILD_DATE: env.EXPO_PUBLIC_BUILD_DATE || today.toISOString().slice(0, 10),
  };
}

/** Set the stamp on `process.env` (Metro's transform workers inherit it). */
function stampVersion(projectRoot) {
  const app = require("./app.json").expo;
  const s = versionStamp(app, process.env, () => gitSha(projectRoot));
  for (const [k, v] of Object.entries(s)) if (v) process.env[k] = v;
  return s;
}

module.exports = { versionStamp, stampVersion };
