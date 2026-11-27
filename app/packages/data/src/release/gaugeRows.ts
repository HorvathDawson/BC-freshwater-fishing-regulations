/**
 * (T530, on T487) THE RELEASE'S GAUGE-SERVES ROWS: each station and the waters it serves (its own water, a lake it gauges,
 * a section it is the gauge of), from the conditions + core files. The one reader for the SEO build (every station) and the
 * app's Share on a gauge (one station): the rule that turns them into "has a page" is core's `gaugePageStations`.
 */
import { bestServes, TRUST_RANK, type GaugeRow, type GaugeRows, type ServeRow, type ServesTrust } from "@app/core";
import type { StationClim } from "../feed/stationSource";
import type { ReleaseLoader } from "./loader";
import { fileFor } from "./queries";

/** null when the release carries no conditions file (the regs fixture). `station`: only that station's rows. */
export async function readGaugeRows(loader: ReleaseLoader, station?: string): Promise<GaugeRows | null> {
  if (!loader.manifest.files.conditions) return null;
  const cond = await loader.db("conditions"), core = await loader.db("core");
  const num = (v: unknown) => (v === null || v === undefined ? null : Number(v));
  const one = station === undefined ? "" : " WHERE station = ?", arg = station === undefined ? [] : [station];
  // `SELECT *`: the flags are additive columns (a release cut before them has none: read as not flagged), never named in the query
  const gauges: GaugeRow[] = (await cond.all(`SELECT * FROM gauge${one} ORDER BY station`, ...arg)).map((r) => ({
    station: String(r.station), name: String(r.name), item: r.item_id == null || r.item_id === "" ? null : String(r.item_id), lat: num(r.lat), lon: num(r.lon),
    flags: { tidal: Number(r.tidal ?? 0) === 1, backwater: Number(r.backwater ?? 0) === 1, diversion: Number(r.diversion ?? 0) === 1 },
  }));
  const serves: ServeRow[] = [];
  for (const g of gauges) if (g.item) serves.push({ station: g.station, item: g.item, trust: "own" });
  for (const r of await cond.all(`SELECT item_id, station FROM lake_gauge${one}`, ...arg)) serves.push({ station: String(r.station), item: String(r.item_id), trust: "lake" });
  const sections = await cond.all(`SELECT sid, station, trust FROM section_gauge${one}`, ...arg);
  const itemOfSid = new Map<number, string[]>();
  if (sections.length) {
    const wanted = station === undefined ? null : new Set(sections.map((r) => Number(r.sid)));
    for (const r of await core.all("SELECT s.sid AS sid, i.item_id AS item FROM item_section s JOIN item i ON i.ord = s.ord")) {
      const sid = Number(r.sid);
      if (wanted && !wanted.has(sid)) continue;
      const l = itemOfSid.get(sid);
      if (l) l.push(String(r.item)); else itemOfSid.set(sid, [String(r.item)]);
    }
  }
  for (const r of sections) {
    const t = String(r.trust) as ServesTrust;
    if (t !== "good" && t !== "fair" && t !== "weak") throw new Error(`gauge rows: section_gauge trust "${String(r.trust)}" is not good/fair/weak`);
    for (const item of itemOfSid.get(Number(r.sid)) ?? []) serves.push({ station: String(r.station), item, trust: t });
  }
  return { gauges, serves };
}

/**
 * (T1039) EACH STATION'S CLIMATOLOGY, as the release holds it (`gauge_clim`: a quantity and its clim/3 bands, in the climate
 * file, or in conditions for a release cut before T182 — `fileFor`): what the site build writes into a gauge page, so the
 * page charts the station from the very bands the app reads (@app/data `stationChartSource`). The same query as the app's
 * `CLIMATOLOGY`, for every station at once. null: the release carries no conditions file (the regs fixture).
 */
export async function readGaugeClim(loader: ReleaseLoader): Promise<ReadonlyMap<string, StationClim[]> | null> {
  if (!loader.manifest.files.conditions) return null;
  const db = await loader.db(fileFor(loader.manifest.files, "climate"));
  const out = new Map<string, StationClim[]>();
  for (const r of await db.all("SELECT station, parameter, bands FROM gauge_clim ORDER BY station, parameter")) {
    const p = String(r.parameter);
    if (p !== "discharge" && p !== "level") continue;           // the quantities a chart is drawn in (`parametersOf`)
    if (!(r.bands instanceof Uint8Array)) throw new Error(`gauge_clim ${String(r.station)} ${p}: bands is not a blob`);
    const l = out.get(String(r.station)) ?? [];
    l.push({ parameter: p, bands: r.bands });
    out.set(String(r.station), l);
  }
  return out;
}

/** (T1194) What the in-app gauge screen reads of one station from the release (offline: all of it is on the phone). */
export interface GaugePageFacts {
  readonly station: string;
  readonly name: string;
  /** the release's flags (`gauge.tidal`, `.backwater`, `.diversion`); absent columns (an older release) read as not flagged */
  readonly tidal: boolean;
  readonly backwater: boolean;
  readonly diversion: boolean;
  /** the waters it serves, best trust first (own, lake, good, fair, weak), then by name; empty: it serves no water */
  readonly serves: readonly { readonly item: string; readonly name: string; readonly trust: ServesTrust }[];
}

/**
 * (T1194) ONE STATION'S PAGE FACTS: null when the release holds no such station (a stale link: the screen says so). The serves
 * come from `readGaugeRows` (the rule the site's page and Share use), the water names from the core file.
 */
export async function readGaugePage(loader: ReleaseLoader, station: string): Promise<GaugePageFacts | null> {
  const rows = await readGaugeRows(loader, station);
  const g = rows?.gauges[0];
  if (!rows || !g) return null;
  const cond = await loader.db("conditions");
  const flags = (await cond.all("SELECT * FROM gauge WHERE station = ?", station))[0] ?? {};
  const best = bestServes(rows).get(station) ?? new Map<string, ServesTrust>();
  const items = [...best.keys()];
  const core = await loader.db("core");
  const names = new Map<string, string>();
  if (items.length)
    for (const r of await core.all(`SELECT item_id, name FROM item WHERE item_id IN (${items.map(() => "?").join(",")})`, ...items))
      names.set(String(r.item_id), String(r.name));
  const serves = items.flatMap((item) => (names.get(item) ? [{ item, name: names.get(item)!, trust: best.get(item)! }] : []))
    .sort((a, b) => TRUST_RANK[a.trust] - TRUST_RANK[b.trust] || a.name.localeCompare(b.name));
  return { station, name: g.name, tidal: Number(flags.tidal ?? 0) === 1, backwater: Number(flags.backwater ?? 0) === 1,
           diversion: Number(flags.diversion ?? 0) === 1, serves };
}
