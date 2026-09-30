/**
 * An in-memory RegsSource holding a slice of the REAL Chilliwack valley build.
 *
 * Not a toy. Every value below came out of build 54ea0bb4 or a live feed on 30 Aug 2026,
 * so the conformance suite exercises the cases that actually broke things: a river cut into
 * several sections, a gauge running at its 4th percentile, and a creek the Fraser gauge must
 * refuse to speak for. It carries NO regulations — they are not integrated (see
 * `regulations.ts` in @app/core).
 */
import type { Band } from "@app/core";
import type {
  Aged, BundleInfo, GaugeLink, ItemId, LakeInfo, NameHit, NearHit, Parameter,
  PlaceHit, PlaceId, Reading, RegsSource, Release, SectionId, Series, StationId, Water,
} from "./index";

const id = <T extends string>(s: string): T => s as T;

const CHILLIWACK = {
  item: "gnis:8634", name: "Chilliwack River",
  sections: ["380887781:0", "380887781:8200", "380887781:23325"],
};
const JEPERSON = { item: "gnis:11481", name: "Jeperson Side Channel",
                   section: "355994562:0", alias: "Greyell Slough" };

/**
 * The fixture's own handle table, built the way the atlas builds the real one.
 *
 * A section is an INTEGER HANDLE everywhere else — in the tile, in the bundle, in
 * `SectionId` — and a fixture that kept strings would be the one place the app's own model
 * did not hold, which is exactly where a bug hides from its tests. So the sections this
 * fixture names are sorted into the water's own order (blue line, then measure as a NUMBER —
 * see pipeline/common/section_handles) and the handle is the index.
 */
const ORDERED = [...CHILLIWACK.sections, JEPERSON.section]
  .sort((a, b) => {
    const [al, am] = a.split(":");
    const [bl, bm] = b.split(":");
    return al! === bl! ? Number(am) - Number(bm) : al!.localeCompare(bl!);
  });
const HANDLES = new Map<string, SectionId>(
  ORDERED.map((s, i) => [s, i as unknown as SectionId]));
/** A fixture section name to its handle. Unknown names fail loudly — a silent 0 is a reach. */
const handleOf = (s: string): SectionId => {
  const h = HANDLES.get(s);
  if (h === undefined) throw new Error(`fixture: no handle for section ${s}`);
  return h;
};

/**
 * The sections a test may need to NAME, exported rather than written down in the test.
 *
 * A handle only means something against the table that minted it, so a literal in a test
 * would be a guess about this file's internals. These are the two the conformance suite
 * needs: the lowest reach of the Chilliwack, and the side channel no gauge may speak for.
 */
export const FIXTURE_SECTIONS = {
  get lowerChilliwack(): SectionId { return handleOf(CHILLIWACK.sections[0]!); },
  get jeperson(): SectionId { return handleOf(JEPERSON.section); },
};

const GAUGE = {
  station: "08MH001", name: "Chilliwack River at Vedder Crossing",
  magnitude: 2182, discharge: 15.7, level: 1.487, percentile: 0.038,
  at: "2026-08-30T07:00:00Z",
};

/**
 * One BC River Forecast Centre run, as the real feed publishes them — a SERIES with bounds,
 * thirty daily steps. A fixture carrying a single point could not have caught the triangle
 * the chart used to draw.
 */
const ELF = {
  model: "ELF", issuedAt: "2026-08-30T00:00:00Z", horizonDays: 30,
  value: 9.4, extreme: "min" as const, min: 9.4, ave: 12.1, max: 16.8, unit: "m3/s",
  disclaimer: "USERS OF THIS DATA MUST ACCEPT ALL RESPONSIBILITY FOR THE USE AND " +
              "INTERPRETATION.",
  series: {
    step: "1d" as const,
    at: Array.from({ length: 30 }, (_, i) =>
      new Date(Date.UTC(2026, 7, 30 + i)).toISOString().slice(0, 10)),
    mid: Array.from({ length: 30 }, (_, i) => 15.7 - i * 0.2),
    lo: Array.from({ length: 30 }, (_, i) => 15.7 - i * 0.28),
    hi: Array.from({ length: 30 }, (_, i) => 15.7 - i * 0.1),
  },
};

export function makeFixtureSource(now = Date.parse("2026-08-30T12:00:00Z")): RegsSource {
  /** Every section this fixture knows, and the water each belongs to. */
  const itemOf = new Map<SectionId, string>([
    ...CHILLIWACK.sections.map((sec) => [handleOf(sec), CHILLIWACK.item] as const),
    [handleOf(JEPERSON.section), JEPERSON.item],
  ]);

  return {
    async info(): Promise<BundleInfo> {
      // A DIGEST OF ITS OWN, and deliberately not the province's.
      //
      // This fixture mints its own handle table from the handful of sections it names, so
      // its section 1 and the province tile's section 1 are different rivers. Declaring a
      // distinct digest is what lets the vintage check say so: the dev server pairs
      // `/bundle.sqlite` (this) with `/atlas.pmtiles` (the province), and that pair is
      // genuinely mismatched. It worked before only because section ids used to be strings
      // that happened to be globally meaningful.
      return { version: "54ea0bb4", validUntil: "2027-03-31",
               sectionHandles: "fixture-local" };
    },
    // The fixture is one valley, and these are ITS counts — not the province's. That
    // distinction is the whole reason the app must read them rather than state them:
    // the numbers a screen shows have to belong to the bundle it actually opened.
    async counts() {
      return { waters: 2, reaches: itemOf.size, surveyed: null, stations: 1 };
    },

    async itemExists(i) { return i === CHILLIWACK.item || i === JEPERSON.item; },
    async itemForSection(s) {
      const item = itemOf.get(s);
      return item ? id<ItemId>(item) : null;
    },

    async water(i): Promise<Water | null> {
      const w = i === CHILLIWACK.item ? { ...CHILLIWACK, kind: "stream" }
        : i === JEPERSON.item ? { ...JEPERSON, sections: [JEPERSON.section], kind: "stream" }
        : null;
      if (!w) return null;
      return { item: id<ItemId>(w.item), name: w.name, kind: w.kind,
               sections: w.sections.map(handleOf) };
    },

    async searchNames(q, limit): Promise<readonly NameHit[]> {
      const needle = q.toLowerCase();
      const rows: NameHit[] = [];
      // THE SAME TOWN the bundle's `place_water` would name, with the same fields, so a
      // screen rendering "near Chilliwack" is exercised by the fixture too.
      const near = { name: "Chilliwack", kind: "city", lat: 49.171, lon: -121.953, km: 1.8 };
      if (CHILLIWACK.name.toLowerCase().includes(needle)) {
        rows.push({ item: id<ItemId>(CHILLIWACK.item), name: CHILLIWACK.name, kind: "stream",
                    matchedAs: null, pieces: CHILLIWACK.sections.length, size: 2236, areaHa: null,
                    near });
      }
      const byAlias = JEPERSON.alias.toLowerCase().includes(needle);
      if (byAlias || JEPERSON.name.toLowerCase().includes(needle)) {
        rows.push({ item: id<ItemId>(JEPERSON.item), name: JEPERSON.name, kind: "stream",
                    matchedAs: byAlias ? JEPERSON.alias : null, pieces: 1, size: 12, areaHa: null,
                    near: { ...near, km: 2.4 } });
      }
      return rows.slice(0, Math.max(0, limit));
    },
    async searchPlaces(q, limit): Promise<readonly PlaceHit[]> {
      return "chilliwack".includes(q.toLowerCase())
        ? [{ place: id<PlaceId>("12"), name: "Chilliwack", kind: "city",
             lat: 49.171, lon: -121.953, pop: 93203 }].slice(0, limit)
        : [];
    },
    async watersNear(p): Promise<readonly NearHit[]> {
      return p === "12"
        ? [{ item: id<ItemId>(CHILLIWACK.item), name: CHILLIWACK.name, kind: "stream", km: 1.8,
             signals: { mag: 2166, areaHa: null, pieces: 19, towns: 72, gauged: true, stocked: false,
                        listed: true } },
           { item: id<ItemId>(JEPERSON.item), name: JEPERSON.name, kind: "stream", km: 2.4,
             signals: { mag: 12, areaHa: null, pieces: 1, towns: 40, gauged: false, stocked: false,
                        listed: false } }]
        : [];
    },
    async locate(i) {
      // The station sits ON the Chilliwack; the side channel has only its town ring.
      if (i === CHILLIWACK.item)
        return { on: [{ lat: 49.096, lon: -121.958 }],
                 near: [{ lat: 49.171, lon: -121.953, km: 1.8 }] };
      if (i === JEPERSON.item)
        return { on: [], near: [{ lat: 49.171, lon: -121.953, km: 2.4 }] };
      return { on: [], near: [] };
    },

    async gaugeForSection(s): Promise<GaugeLink | null> {
      if (!itemOf.has(s)) return null;
      // THE BAND IS READ, NOT COMPUTED — because that is what the real source does. It
      // selects `section_gauge.trust`, a value `pipeline/gauges/consume/shed.py` decided against the
      // full graph. This used to call a `gaugeTrust()` in @app/core, so the suite asserted
      // against a SECOND implementation of the rule (asymmetric, and with no drainage gate)
      // rather than against anything the province bundle could produce. That function is
      // gone; the fixture states outcomes the same way the bundle stores them.
      //
      // A tiny creek in the same window is measured by the Fraser at Hope, which must
      // refuse to speak for it — that is the whole point of the magnitude floor. 12 against
      // 273,576 is a ten-thousandth, below even the `weak` floor, so the bundler writes NO
      // ROW: the honest answer is a null link, not a fourth band meaning "none".
      if (s === handleOf(JEPERSON.section)) return null;
      return {
        station: id<StationId>(GAUGE.station), name: GAUGE.name, trust: "good",
        reachMagnitude: GAUGE.magnitude, gaugeMagnitude: GAUGE.magnitude,
        live: true, areaKm2: 1230, lon: -121.958, lat: 49.096, section: s,
      };
    },

    async gaugeForItem(i): Promise<GaugeLink | null> {
      // The best reach on the water, which is not the same as any particular one: the
      // Jeperson creek section would answer "weak", the river as a whole answers "good".
      if (i !== CHILLIWACK.item) return null;
      return this.gaugeForSection(handleOf(CHILLIWACK.sections[0]!));
    },
    async stationsFor(sections): Promise<ReadonlyMap<SectionId, StationId>> {
      const out = new Map<SectionId, StationId>();
      for (const s of sections)
        if (itemOf.has(s)) out.set(s, id<StationId>(GAUGE.station));
      return out;
    },

    /**
     * The dev fixture has no panels, and says so by returning nothing at all rather than
     * an empty panel per section. The two are different answers — "nothing qualified" is a
     * fact about the water, "not asked" is a fact about the query — and a fixture that
     * fabricated the first would let a screen ship that cannot tell them apart.
     */
    async panelsFor() { return new Map(); },

    async gaugePoints() {
      // Vedder Crossing on the Chilliwack: magnitude 2,236, so the dot appears at z5 —
      // the same zoom the river it measures does.
      return [{ station: id<StationId>(GAUGE.station), name: GAUGE.name,
                lon: -121.958, lat: 49.096, mag: 2236 }];
    },

    async gaugeNow(st): Promise<Aged<Reading> | null> {
      if (st !== GAUGE.station) return null;
      return {
        fetchedAt: Date.parse(GAUGE.at),
        value: { discharge: GAUGE.discharge, level: GAUGE.level, at: GAUGE.at,
                 percentile: GAUGE.percentile, standing: "much-below" },
      };
    },
    async gaugeSeries(st, span, parameter, model): Promise<Aged<Series> | null> {
      if (st !== GAUGE.station) return null;
      const param: Parameter = parameter ?? "discharge";
      const level = param === "level";
      // Two envelopes in two units, because that is the shape of the real thing: a station
      // measuring both has one band in m3/s and one in metres, and a fixture that carried
      // only one would let a unit bug through the tests that exist to catch it.
      const band: Band = level ? [1.20, 1.35, 1.52, 1.74, 2.05] : [18.8, 23.0, 30.6, 39.0, 56.6];
      const value = level ? GAUGE.level : GAUGE.discharge;
      // Year to date plus a month, as the bundle source builds it — the whole calendar
      // year would be four empty months on the right.
      // A day per point on the year axis, mirroring the bundle source: the envelope is
      // five-day but the line across it is daily.
      const n = span === "72h" ? 72 : 273;
      return {
        fetchedAt: now,
        value: {
          step: span === "72h" ? "1h" : "1d",
          from: span === "72h" ? "2026-08-27T07:00:00Z" : "2026-01-01",
          parameter: param,
          at: Array.from({ length: n }, (_, i) => span === "72h"
            ? new Date(Date.UTC(2026, 7, 27, 7 + i)).toISOString()
            : new Date(Date.UTC(2026, 0, 1 + i)).toISOString().slice(0, 10)),
          values: Array.from({ length: n }, () => (span === "72h" ? value : null)),
          band: Array.from({ length: n }, () => band),
          now: { index: span === "72h" ? n - 1 : 241, value },
          // A REAL SERIES, thirty daily steps with bounds — not one number. A fixture
          // carrying a single point could not have caught the triangle the chart drew.
          forecast: level ? null : ELF,
          forecasts: level ? [] : [ELF],
          // Two complete years on the same day index, so 3 August sits above 3 August.
          priorYears: span === "year"
            ? [{ year: 2025, values: Array.from({ length: n }, (_, i) => value * (1 + i / 400)) },
               { year: 2024, values: Array.from({ length: n }, (_, i) => value * (1.3 - i / 500)) }]
            : [],
        },
      };
    },
    async gaugeParameters(st): Promise<readonly Parameter[]> {
      return st === GAUGE.station ? ["discharge", "level"] : [];
    },
    async traceToGauge(from) {
      return itemOf.has(from) ? [from] : [];
    },
    // No panel in the fixture (`panelsFor` is empty), so no routes. Empty and not a throw:
    // "this water has no donors" is a real answer the screens must render.
    async panelRoutes() { return []; },
    // The design fixture has no lake stations, and an empty map is the real answer for a
    // province where 196 lake_gauge rows cover a few dozen lakes.
    async lakeStationsFor() { return new Map(); },
    // The design fixture has no watershed polygons, so the field has nothing to colour.
    // Empty is the real answer, and the map draws no field rather than a wrong one.
    async basinMembers() { return new Map(); },
    // No `this`: a source is routinely destructured, and a fixture that only works while
    // its methods are still attached to the object is a trap set for the next test.
    async waterFor(sec) {
      if (sec === handleOf(JEPERSON.section))
        return { item: id<ItemId>(JEPERSON.item), name: JEPERSON.name, kind: "stream" };
      return itemOf.has(sec)
        ? { item: id<ItemId>(CHILLIWACK.item), name: CHILLIWACK.name, kind: "stream" }
        : null;
    },

    async lakeInfo(i): Promise<LakeInfo | null> {
      if (i !== "wbk:329083342") return null;
      return {
        item: id<ItemId>("wbk:329083342"),
        charts: [{ id: "00239801", title: "CULTUS LAKE", kind: "scan",
                   drafted: "1974-06-04", scale: 30000, bytes: 2_100_000 }],
        lastStocked: { date: "1987-07-01", species: "Rainbow Trout", count: 19305 },
      };
    },
    async stockingHistory(i): Promise<readonly Release[]> {
      return i === "wbk:329083342"
        ? [{ date: "1987-07-01", species: "Rainbow Trout", count: 19305, stage: "Yearling" }]
        : [];
    },
  };
}
