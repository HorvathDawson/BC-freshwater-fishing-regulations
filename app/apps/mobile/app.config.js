// Dynamic part of the Expo config; app.json is the rest (Expo hands it in as `config`).
//
// THE NATIVE SPLASH (T244): expo-splash-screen's plugin wants a literal ground colour per theme. The app's colours live
// in the theme files and nowhere else (tools/check-colours.mjs), so the ground is read from them here: the boot screen's
// own ground, `color.ui.card`, per theme. The picture is the boot loader's frame 0 (`pnpm splash`, tools/build-splash.mjs):
// since T843 the coho (`CohoLoader`, 192 dp wide, `COHO_WIDTH`), at the size the boot screen draws it, so the hand-off to
// the live loader has nothing to jump between.
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

// (T420) the map lab is a staging/test-bed tool (COORD-068: git show caf593bb6:handoff/PLAN/COORD-068.md): a production build carrying its flag is not made
if (production && process.env.EXPO_PUBLIC_MAP_LAB) {
  throw new Error("app.config.js: EXPO_PUBLIC_MAP_LAB is for staging and test-bed builds only; unset it for production");
}

// (T420, T893, T978) every Android build carries a locally built MapLibre, production too: the pinned upstream commit with
// no patch (app/tools/maplibre-android/pin.env, owner 2026-11-22); build-apk.sh refuses any other AAR and checks the
// APK/AAB carries its build id; plugins/with-pinned-maplibre.js refuses a production Android config without it

// (T749) R8 (CANIFISHTHIS_R8=1, plugins/with-no-minify.js) is a trial until its phone check is recorded: not in production
if (production && require("./plugins/with-no-minify").r8Enabled()) {
  throw new Error("app.config.js: CANIFISHTHIS_R8 (R8 on, T749) is a trial for test-bed and Play internal builds; unset it for production");
}

// (T420) with MLN_LOCAL_MAVEN set, MapLibre's own plugin is told the local SDK version (with-local-maplibre.js adds the repo)
const localMaplibre = process.env.MLN_LOCAL_MAVEN ? require("./plugins/with-local-maplibre").localVersion(process.env.MLN_LOCAL_MAVEN) : null;
const withLocalMaplibreVersion = (plugins) => !localMaplibre ? plugins : plugins.map((p) =>
  Array.isArray(p) && p[0] === "@maplibre/maplibre-react-native"
    ? [p[0], { ...p[1], android: { ...p[1]?.android, nativeVersion: localMaplibre } }] : p);

// (T739) No Android push (owner 2026-11-16 12:08, on Firebase: "we dont want this right now"): no `google-services.json`,
// no Firebase config here; push is iPhone only (APNs, `src/pushToken.ios.ts`).

module.exports = ({ config }) => ({
  ...config,
  // the adaptive icon's solid background: the icon's own ground (the splash below keeps the boot screen's `color.ui.card`)
  android: { ...config.android, adaptiveIcon: { ...config.android?.adaptiveIcon, backgroundColor: iconGround } },
  plugins: [
    ...withLocalMaplibreVersion(config.plugins ?? []),
    ["expo-splash-screen", {
      backgroundColor: ground("light"),
      image: "./assets/splash/splash-light.png",
      imageWidth: 192,
      resizeMode: "contain",
      dark: { backgroundColor: ground("dark"), image: "./assets/splash/splash-dark.png" },
    }],
    // (T420, T978) set by build-apk.sh / session.sh for every Android build (the pinned upstream repo); unset (a bare
    // `expo prebuild`, `pnpm android`, an EAS build), the binding's stock MapLibre from Maven Central …
    ...(process.env.MLN_LOCAL_MAVEN ? ["./plugins/with-local-maplibre"] : []),
    // … which an Android PRODUCTION config now refuses, naming the fix, and any other Android config warns about
    // (T978 review: "every Android build uses the pinned build"); an Android mod, so the iPhone build never meets it
    ["./plugins/with-pinned-maplibre", { production }],
  ],
});
