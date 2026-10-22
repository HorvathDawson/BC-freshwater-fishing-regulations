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
import type { DailyWeather, SpotWeather, WeatherSample } from "./model";
import type { WeatherSource } from "./capture";

const CURRENT = "https://api.open-meteo.com/v1/forecast";
const ARCHIVE = "https://archive-api.open-meteo.com/v1/archive";

/** The fields Riffle recorded, which are the ones a person actually asks about. */
const FIELDS = "temperature_2m,relative_humidity_2m,precipitation,weather_code," +
               "wind_speed_10m,wind_direction_10m,pressure_msl,cloud_cover";

const day = (d: Date) => d.toISOString().slice(0, 10);

/** Yesterday, in UTC. The archive lags real time; asking it for today returns nothing. */
const isHistoric = (when: Date) => Date.now() - when.getTime() > 36 * 3600_000;

/** S1: the week before a visit, one row per day. */
const DAILY = "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,weather_code";

/**
 * How far behind real time the archive's DAILY rows are complete. Open-Meteo documents its reanalysis as days behind;
 * a week inside that window is asked of the forecast endpoint with `past_days`, which carries the recent past.
 */
const ARCHIVE_LAG_MS = 5 * 86_400_000;

/** How far back the forecast endpoint's `past_days` reaches (Open-Meteo's documented maximum). */
const PAST_DAYS_MAX = 92;

/** Midnight at the start of a local "YYYY-MM-DD". */
const localMidnight = (d: string): number => {
  const [y, m, day] = d.split("-").map(Number) as [number, number, number];
  return new Date(y, m - 1, day).getTime();
};

export function openMeteo(fetchImpl: typeof fetch = fetch): WeatherSource {
  return {
    /**
     * The week before a visit (S1), DAILY: max/min temperature, precipitation, strongest wind and the day's WMO code.
     *
     * `timezone=auto` makes a day the PLACE's day, which for this province is the reader's. The archive for a week
     * past its lag, the forecast endpoint with `past_days` for a recent one. Only the days asked for are returned, and
     * a day the service did not send is left out — the caller holds it as a day of nulls.
     */
    async days(lat, lon, days): Promise<DailyWeather[] | null> {
      const first = days[0], last = days[days.length - 1];
      if (!first || !last) return null;
      const endOfLast = localMidnight(last) + 86_400_000;
      const where = `latitude=${lat}&longitude=${lon}`;
      const today = new Date(); today.setHours(0, 0, 0, 0);
      const back = Math.round((today.getTime() - localMidnight(first)) / 86_400_000);
      const recent = `${CURRENT}?${where}&daily=${DAILY}&timezone=auto` +
                     `&past_days=${Math.min(PAST_DAYS_MAX, Math.max(1, back))}&forecast_days=1`;
      const wanted = new Set(days);
      /** One endpoint's days, only those asked for; null when it could not be read. */
      const ask = async (url: string): Promise<DailyWeather[] | null> => {
        try {
          const res = await fetchImpl(url);
          if (!res.ok) return null;
          const body = await res.json() as { daily?: Record<string, (number | null)[] | string[]> };
          const d = body.daily;
          const time = (d?.time ?? []) as string[];
          if (!d || !time.length) return null;
          const col = (k: string, i: number): number | null => {
            const v = (d[k] as (number | null)[] | undefined)?.[i];
            return typeof v === "number" && Number.isFinite(v) ? v : null;
          };
          return time.flatMap((date, i) => (wanted.has(date) ? [{
            date, maxC: col("temperature_2m_max", i), minC: col("temperature_2m_min", i),
            precipMm: col("precipitation_sum", i), windMaxKph: col("wind_speed_10m_max", i),
            code: col("weather_code", i),
          }] : []));
        } catch {
          return null;
        }
      };
      if (Date.now() - endOfLast <= ARCHIVE_LAG_MS) return ask(recent);
      const archived = await ask(`${ARCHIVE}?${where}&start_date=${first}&end_date=${last}&daily=${DAILY}&timezone=auto`);
      // THE ARCHIVE LAGS REAL TIME, and answers a day it has not reached with the day present and every value null
      // (owner bug 2026-10-22). Such a day, or one it left out, is asked of the forecast's `past_days` while that
      // reaches back far enough; a day neither has stays out, and the lead-up asks for it again on a refresh.
      const has = (g: DailyWeather | undefined) =>
        !!g && [g.maxC, g.minC, g.precipMm, g.windMaxKph, g.code].some((v) => v !== null);
      const holes = days.filter((day) => !has(archived?.find((g) => g.date === day)));
      if (!holes.length || back > PAST_DAYS_MAX) return archived;
      const more = await ask(recent);
      if (!more) return archived;
      return days.flatMap((day) => {
        const a = archived?.find((g) => g.date === day);
        const m = more.find((g) => g.date === day);
        const pick = has(a) ? a : has(m) ? m : a ?? m;
        return pick ? [pick] : [];
      });
    },

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
