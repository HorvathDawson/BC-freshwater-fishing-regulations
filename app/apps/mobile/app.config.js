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

module.exports = ({ config }) => ({
  ...config,
  // (T244) the adaptive icon's solid background: the ground the boot screen paints (the icon is the favicon, `pnpm`-less: tools/build-icons.mjs)
  android: { ...config.android, adaptiveIcon: { ...config.android?.adaptiveIcon, backgroundColor: ground("light") } },
  plugins: [
    ...(config.plugins ?? []),
    ["expo-splash-screen", {
      backgroundColor: ground("light"),
      image: "./assets/splash/splash-light.png",
      imageWidth: 110,
      resizeMode: "contain",
      dark: { backgroundColor: ground("dark"), image: "./assets/splash/splash-dark.png" },
    }],
  ],
});
