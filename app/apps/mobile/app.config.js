// Dynamic part of the Expo config; app.json is the rest (Expo hands it in as `config`).
//
// THE NATIVE SPLASH (T244): expo-splash-screen's plugin wants a literal ground colour per theme. The app's colours live
// in the theme files and nowhere else (tools/check-colours.mjs), so the ground is read from them here: the boot screen's
// own ground, `color.ui.card`, per theme. The picture is the loading fish's frame 0 (`pnpm splash`, tools/build-splash.mjs)
// at the size the boot screen draws it, so the hand-off to the live FishSpinner has nothing to jump between.
const { readFileSync } = require("node:fs");
const { join } = require("node:path");

const ground = (theme) =>
  JSON.parse(readFileSync(join(__dirname, "../../packages/map/style/themes", `${theme}.json`), "utf8")).values["color.ui.card"];

// The launcher icon's ground: the icon's own colour (assets/icon/icon-ground.txt, written by tools/build-icons.mjs from the
// chosen mark), not the boot screen's `color.ui.card` — the two are separate since the round-4 mark (owner 2026-11-08).
const iconGround = readFileSync(join(__dirname, "assets/icon/icon-ground.txt"), "utf8").trim();

// (T86) a production build names a contact (the app's Contact page, the store listing), or it is not made: config.ts
// reads a missing EXPO_PUBLIC_CONTACT as null and the app would ship with no way to reach anyone.
const production = process.env.EXPO_PUBLIC_APP_ENV === "production" || process.env.EAS_BUILD_PROFILE === "production";
if (production && !process.env.EXPO_PUBLIC_CONTACT?.trim()) {
  throw new Error("app.config.js: a production build needs EXPO_PUBLIC_CONTACT (support@canifishthis.ca; eas.json sets it)");
}

// (T420) the map lab is a staging/test-bed tool (COORD-068): a production build carrying its flag is not made
if (production && process.env.EXPO_PUBLIC_MAP_LAB) {
  throw new Error("app.config.js: EXPO_PUBLIC_MAP_LAB is for staging and test-bed builds only; unset it for production");
}

// (T420) a locally built MapLibre (app/tools/maplibre-patch) is for test-bed builds only: production stays on the stock SDK
if (production && process.env.MLN_LOCAL_MAVEN) {
  throw new Error("app.config.js: MLN_LOCAL_MAVEN (a locally built MapLibre) is for test-bed builds only; unset it for production");
}

// (T420) with MLN_LOCAL_MAVEN set, MapLibre's own plugin is told the local SDK version (with-local-maplibre.js adds the repo)
const localMaplibre = process.env.MLN_LOCAL_MAVEN ? require("./plugins/with-local-maplibre").localVersion(process.env.MLN_LOCAL_MAVEN) : null;
const withLocalMaplibreVersion = (plugins) => !localMaplibre ? plugins : plugins.map((p) =>
  Array.isArray(p) && p[0] === "@maplibre/maplibre-react-native"
    ? [p[0], { ...p[1], android: { ...p[1]?.android, nativeVersion: localMaplibre } }] : p);

module.exports = ({ config }) => ({
  ...config,
  // the adaptive icon's solid background: the icon's own ground (the splash below keeps the boot screen's `color.ui.card`)
  android: { ...config.android, adaptiveIcon: { ...config.android?.adaptiveIcon, backgroundColor: iconGround } },
  plugins: [
    ...withLocalMaplibreVersion(config.plugins ?? []),
    ["expo-splash-screen", {
      backgroundColor: ground("light"),
      image: "./assets/splash/splash-light.png",
      imageWidth: 110,
      resizeMode: "contain",
      dark: { backgroundColor: ground("dark"), image: "./assets/splash/splash-dark.png" },
    }],
    // (T420) opt-in only: unset (every normal and store build), the app uses the stock MapLibre from Maven Central
    ...(process.env.MLN_LOCAL_MAVEN ? ["./plugins/with-local-maplibre"] : []),
  ],
});
