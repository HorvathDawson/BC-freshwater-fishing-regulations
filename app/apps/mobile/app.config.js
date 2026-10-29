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

module.exports = ({ config }) => ({
  ...config,
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
