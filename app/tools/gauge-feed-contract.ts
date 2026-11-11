/**
 * (T557 item 3 / T633 item 3) THE GAUGE FEED CONTRACT, as one check run on two inputs: the committed fixture feed
 * (tools/feed-contract.test.ts) and the feeds worker's OWN output on its recording (workers/feeds/test/
 * gauges.contract.test.ts). Before this the contract saw only a fixture cut on 2026-08-30, which still carried an
 * index `forecast` array the publisher has never written; a worker that renamed `percentile` passed every test.
 *
 * A file is held to: the app's own read check (`gaugeFeedRefusal`: format, shape, the index's time); a `fetchedAt`;
 * no section id anywhere (AGENTS rule 5); and KEYS THE APP KNOWS. A key outside these lists is a field the app does not
 * read (or a renamed one it then reads as undefined): add it here and to the app's type in packages/data feed/http.ts
 * in the same change, or do not write it.
 */
import { FORECAST_DAYS } from "../packages/core/src/gauge-policy.generated";
import { gaugeFeedRefusal, type GaugeFeedFile } from "../packages/core/src/gaugeFeed";

/** index.json's own keys (`IndexFile`; `forecastAttribution` is read by the panel's credits line). */
export const INDEX_KEYS = ["format", "fetchedAt", "forecastIssuedAt", "hydat", "maintenance", "forecastAttribution",
                           "stations", "listed"] as const;
/** One station's entry in the index (`IndexFile.stations`): what colours a dot and when it was read. `temperature` is the
 *  temperature's place in its own envelope, written by the publisher and not read by the app yet. */
export const INDEX_STATION_KEYS = ["percentile", "parameter", "discharge", "level", "observedAt", "stale", "staleSince",
                                   "forecast", "ahead", "aheadDays", "change", "temperatureC", "temperatureAt",
                                   "temperatureBand", "temperature"] as const;
/** What the bundle owns (the `gauge` table): never re-shipped by a feed. */
export const BUNDLE_OWNED = ["name", "at", "areaKm2", "area_km2", "river", "section", "lon", "lat", "mag", "active"] as const;
/** A station file's keys (`StationFile`) and its `now`. */
export const STATION_KEYS = ["format", "fetchedAt", "station", "now", "recent", "daily", "priorYears", "forecasts",
                             "change"] as const;
export const NOW_KEYS = ["discharge", "level", "at", "percentile", "parameter"] as const;
/** obs.json (`ObsFile`). */
export const OBS_KEYS = ["format", "fetchedAt", "windowHours", "stations"] as const;
const AHEAD_KEYS = ["discharge", "level", "model", "levelModel"];

/** "{blk}:{measure}", the shape a section id takes: it never leaves the bundle. */
const SECTION_ID = /^\d{6,}:\d+$/;

function strings(v: unknown, out: string[] = []): string[] {
  if (typeof v === "string") out.push(v);
  else if (Array.isArray(v)) for (const x of v) strings(x, out);
  else if (v && typeof v === "object") for (const [k, x] of Object.entries(v)) { out.push(k); strings(x, out); }
  return out;
}

const isRank = (v: unknown) => v === null || (typeof v === "number" && v >= 0 && v <= 1);
const isObj = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);

/** Every way the files break the contract, in words; [] when they keep it. `files` maps a file name to its parsed JSON. */
export function gaugeFeedContract(files: Record<string, unknown>): string[] {
  const bad: string[] = [];
  const unknownKeys = (where: string, o: Record<string, unknown>, known: readonly string[]) => {
    for (const k of Object.keys(o)) if (!known.includes(k)) bad.push(`${where}: '${k}' is not a key the app reads`);
  };
  const days = FORECAST_DAYS.map(String);
  if (!isObj(files["index.json"])) bad.push("index.json: missing");
  for (const [name, doc] of Object.entries(files)) {
    const kind: GaugeFeedFile = name === "index.json" ? "index" : name === "obs.json" ? "obs" : "station";
    const why = gaugeFeedRefusal(doc, kind);
    if (why) { bad.push(`${name}: ${why}`); continue; }
    const d = doc as Record<string, unknown>;
    if (!d.fetchedAt) bad.push(`${name}: no fetchedAt`);
    for (const s of strings(d)) if (SECTION_ID.test(s)) bad.push(`${name}: carries a section id ${s}`);
    if (kind === "index") {
      unknownKeys(name, d, INDEX_KEYS);
      for (const [st, e] of Object.entries(d.stations as Record<string, unknown>)) {
        if (!isObj(e)) { bad.push(`${name} ${st}: not an object`); continue; }
        for (const k of Object.keys(e))
          if ((BUNDLE_OWNED as readonly string[]).includes(k)) bad.push(`${name} ${st}: re-ships '${k}', which the bundle owns`);
        unknownKeys(`${name} ${st}`, e, INDEX_STATION_KEYS);
        for (const k of ["percentile", "discharge", "level", "temperature"])
          if (k in e && !isRank(e[k])) bad.push(`${name} ${st}: ${k} ${JSON.stringify(e[k])} is not a percentile`);
        if ("forecast" in e && typeof e.forecast !== "boolean") bad.push(`${name} ${st}: forecast is not a boolean`);
        if (e.ahead !== undefined) {
          if (!isObj(e.ahead)) { bad.push(`${name} ${st}: ahead is not an object`); continue; }
          for (const [day, at] of Object.entries(e.ahead)) {
            if (!days.includes(day)) bad.push(`${name} ${st}: ahead day '${day}' is not a forecast day (${days.join(",")})`);
            if (!isObj(at)) { bad.push(`${name} ${st}: ahead ${day} is not an object`); continue; }
            unknownKeys(`${name} ${st} ahead ${day}`, at, AHEAD_KEYS);
            for (const q of ["discharge", "level"])
              if (q in at && !isRank(at[q])) bad.push(`${name} ${st}: ahead ${day} ${q} is not a percentile`);
          }
        }
      }
    } else if (kind === "obs") {
      unknownKeys(name, d, OBS_KEYS);
      for (const [st, rows] of Object.entries(d.stations as Record<string, unknown>))
        if (!Array.isArray(rows) || rows.some((r) => !Array.isArray(r) || typeof r[0] !== "string"))
          bad.push(`${name} ${st}: rows are not [time, level, discharge]`);
    } else {
      unknownKeys(name, d, STATION_KEYS);
      if (`${d.station}.json` !== name) bad.push(`${name}: names station ${JSON.stringify(d.station)}`);
      if (!isObj(d.now)) bad.push(`${name}: no now`);
      else {
        unknownKeys(`${name} now`, d.now, NOW_KEYS);
        if (!isRank(d.now.percentile ?? null)) bad.push(`${name}: now.percentile is not a percentile`);
      }
    }
  }
  return bad;
}
