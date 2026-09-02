/**
 * Weather, from Open-Meteo — the service the Riffle prototype used.
 *
 * No key, no account, free for non-commercial use, and it has an ARCHIVE endpoint as well
 * as a current one. That second part is what makes the offline story work: a spot pinned
 * with no signal records `null`, and the same function can fill it in later from the date
 * it was pinned. A backfilled reading is honest as long as it says it was backfilled.
 *
 * https://open-meteo.com — CC-BY-4.0, attribution carried in the Layers panel.
 */
import type { SpotWeather, WeatherSample } from "./model";
import type { WeatherSource } from "./capture";

const CURRENT = "https://api.open-meteo.com/v1/forecast";
const ARCHIVE = "https://archive-api.open-meteo.com/v1/archive";

/** The fields Riffle recorded, which are the ones a person actually asks about. */
const FIELDS = "temperature_2m,relative_humidity_2m,precipitation,weather_code," +
               "wind_speed_10m,wind_direction_10m,pressure_msl,cloud_cover";

const day = (d: Date) => d.toISOString().slice(0, 10);

/** Yesterday, in UTC. The archive lags real time; asking it for today returns nothing. */
const isHistoric = (when: Date) => Date.now() - when.getTime() > 36 * 3600_000;

export function openMeteo(fetchImpl: typeof fetch = fetch): WeatherSource {
  return {
    async at(lat, lon, when): Promise<SpotWeather | null> {
      try {
        const historic = isHistoric(when);
        // BOTH ENDPOINTS ARE ASKED FOR `hourly`, not just the archive. The cloud window
        // needs the hour either side of the visit, and `current` is a single instant — so
        // a spot pinned this morning could carry a temperature but no window, which is the
        // one combination that would make the panel lie by omission. One request either
        // way; `current` rides along when it exists because it is fresher than the hour.
        const range = `&start_date=${day(when)}&end_date=${day(when)}&hourly=${FIELDS}`;
        const url = historic
          ? `${ARCHIVE}?latitude=${lat}&longitude=${lon}${range}`
          : `${CURRENT}?latitude=${lat}&longitude=${lon}${range}&current=${FIELDS}`;

        const res = await fetchImpl(url);
        if (!res.ok) return null;
        const body = await res.json() as {
          current?: Record<string, number | string>;
          hourly?: Record<string, (number | null)[] | string[]>;
        };

        const h = body.hourly;
        const times = (h?.time ?? []) as string[];
        const want = when.toISOString().slice(0, 13);
        // -1 when the day came back without the hour asked for, which `cloudAt` reads as
        // "no window". Defaulting to index 0 would silently report midnight's sky.
        const i = times.findIndex((t) => t.slice(0, 13) === want);
        const col = (k: string) => (h?.[k] as (number | null)[] | undefined);
        // One hour of the series, or null when that hour is off the end of what came back.
        // Never clamped to a neighbour: "we do not know" and "the same as 08:00" are
        // different claims, and only one of them is true.
        const sample = (n: number): WeatherSample | null => {
          if (i < 0 || n < 0 || n >= times.length) return null;
          const v = (k: string) => col(k)?.[n] ?? null;
          return {
            at: times[n] ?? null, tempC: v("temperature_2m"), cloudPct: v("cloud_cover"),
            pressureHpa: v("pressure_msl"), humidityPct: v("relative_humidity_2m"),
          };
        };
        const win = i < 0 ? null
          : { before: sample(i - 1), at: sample(i), after: sample(i + 1) };
        const cloud = { window: win };

        // A live spot: `current` is the better instant, the window comes from the hours.
        if (!historic && body.current) return { ...read(body.current, String(body.current.time ?? ""), false), ...cloud };

        if (!h || i < 0) return null;
        const pick = (k: string) => col(k)?.[i] ?? null;
        return {
          tempC: pick("temperature_2m"), windKph: pick("wind_speed_10m"),
          windDir: pick("wind_direction_10m"), rain3h: pick("precipitation"),
          pressureHpa: pick("pressure_msl"), code: pick("weather_code"),
          at: times[i] ?? null,
          ...cloud,
          // Says so. A value fetched days later is fine; claiming it was live is not.
          backfilled: true,
        };
      } catch {
        // Offline, blocked, rate-limited — all the same answer: we do not know yet, and
        // the spot records that rather than a plausible temperature.
        return null;
      }
    },
  };
}

function read(c: Record<string, number | string>, at: string,
              backfilled: boolean): SpotWeather {
  const n = (k: string) => (typeof c[k] === "number" ? (c[k] as number) : null);
  return {
    tempC: n("temperature_2m"), windKph: n("wind_speed_10m"),
    windDir: n("wind_direction_10m"), rain3h: n("precipitation"),
    pressureHpa: n("pressure_msl"), code: n("weather_code"),
    // The caller overwrites this from the hourly series; `current` is a single instant.
    window: null,
    at: at || null, backfilled,
  };
}

export const OPEN_METEO_ATTRIBUTION =
  "Weather from Open-Meteo.com (CC BY 4.0)";
