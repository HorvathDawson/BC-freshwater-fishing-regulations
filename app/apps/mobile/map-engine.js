/**
 * (T1154) THE PHONE MAP'S ENGINE, ONE PER BUILD: `EXPO_PUBLIC_MAP_ENGINE`, unset = `maplibre`. The engines are named
 * HERE and nowhere else (`MAP_ENGINES`), each a folder `packages/map/src/native/<engine>/`. Read by:
 *   - app.config.js, when Expo evaluates the config (`expo config`, a prebuild, an EAS build);
 *   - metro.config.js, when Metro starts (every bundle: web, Android, iOS).
 * A value that names no engine FAILS each of them with the same message (AGENTS W46: never a fallback to maplibre).
 * Plain CommonJS: Expo's and Metro's configs are loaded by Node before any TypeScript exists.
 */

/** The phone map's engines (T1155 would add "mapbox"). */
const MAP_ENGINES = ["maplibre"];
/** The engine of a build that names none. */
const DEFAULT_MAP_ENGINE = "maplibre";

/** The build's engine from its environment; an engine this app does not have throws, named. */
function mapEngine(env = process.env) {
  const value = env.EXPO_PUBLIC_MAP_ENGINE;
  if (value === undefined) return DEFAULT_MAP_ENGINE;
  if (!MAP_ENGINES.includes(value)) {
    throw new Error(`EXPO_PUBLIC_MAP_ENGINE="${value}" is not a map engine of this app; the engines are: `
      + `${MAP_ENGINES.join(", ")} (app/apps/mobile/map-engine.js, T1154)`);
  }
  return value;
}

module.exports = { MAP_ENGINES, DEFAULT_MAP_ENGINE, mapEngine };
