/**
 * An in-memory RegsSource holding a slice of the REAL Chilliwack valley build.
 *
 * Not a toy. Every value below came out of build 54ea0bb4 or a live feed on 30 Aug 2026,
 * so the conformance suite exercises the cases that actually broke things: a river with
 * several regulated stretches, a June closure, a rule nobody could place, a gauge running
 * at its 4th percentile, and a creek the Fraser gauge must refuse to speak for.
 */
import type { Band, PlainDate, Rule, SpeciesGroup, Status } from "@app/core";
import { evaluate } from "@app/core";
import type {
  Aged, BundleInfo, GaugeLink, ItemId, ItemRegs, LakeInfo, NameHit, NearHit, Parameter,
  PlaceHit, PlaceId, Reading, RegsSource, Release, SectionId, Series, StationId,
} from "./index";

const id = <T extends string>(s: string): T => s as T;

/* A closure is a retention limit of zero you may NOT fish for. `mayTarget` is what
   separates it from catch-and-release, which is the same type with the same take. */
const JUNE_CLOSURE: Rule = {
  id: "chilliwack_vedder_rivers.r5", type: "retention_limit", family: "retention",
  dimension: "daily", label: "No fishing", take: 0, mayTarget: false,
  scope: "section", via: "reach",
  group: "provincial", windows: [{ from: { month: 6, day: 1 }, to: { month: 6, day: 30 } }],
};
const FLY_ONLY: Rule = {
  id: "chilliwack_vedder_rivers.r4a", type: "tackle_restriction", family: "gear_and_method",
  dimension: "lure", label: "Fly fishing only", scope: "section", via: "reach",
  group: "provincial",
  windows: [{ from: { month: 5, day: 1 }, to: { month: 5, day: 31 } }],
};
const UPSTREAM_CLOSURE: Rule = {
  id: "chilliwack_vedder_rivers.r1", type: "retention_limit", family: "retention",
  dimension: "daily", label: "No fishing", take: 0, mayTarget: false,
  scope: "section", via: "reach", group: "provincial", windows: [],
};
/** Real: the Fraser side-channel closure for which no extent was ever authored. */
const UNPLACEABLE: Rule = {
  id: "fraser_river_region2.r4", type: "retention_limit", family: "retention",
  dimension: "daily", label: "No fishing", take: 0, mayTarget: false,
  scope: "section", via: "reach", group: "provincial", uncertain: true,
  windows: [{ from: { month: 5, day: 15 }, to: { month: 7, day: 31 } }],
};

interface Reach { section: string; seq: number; lo: string | null; hi: string | null; rules: Rule[] }

const CHILLIWACK: { item: string; name: string; reaches: Reach[] } = {
  item: "gnis:8634", name: "Chilliwack River",
  reaches: [
    { section: "380887781:0", seq: 0, lo: null, hi: "Vedder Crossing Bridge",
      rules: [JUNE_CLOSURE, FLY_ONLY] },
    { section: "380887781:8200", seq: 1, lo: "Vedder Crossing Bridge", hi: "Tamihi Rapids Bridge",
      rules: [] },
    { section: "380887781:23325", seq: 2, lo: "Tamihi Rapids Bridge", hi: "Chilliwack Lake",
      rules: [UPSTREAM_CLOSURE] },
  ],
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
const ORDERED = [...CHILLIWACK.reaches.map((r) => r.section), JEPERSON.section]
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
 * needs: the reach carrying the June closure, and the side channel whose only rule could
 * never be placed.
 */
export const FIXTURE_SECTIONS = {
  get lowerChilliwack(): SectionId { return handleOf(CHILLIWACK.reaches[0]!.section); },
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
  const sectionRules = new Map<SectionId, Rule[]>(
    CHILLIWACK.reaches.map((r) => [handleOf(r.section), r.rules]),
  );
  sectionRules.set(handleOf(JEPERSON.section), [UNPLACEABLE]);

  const statusOf = (section: SectionId, on: PlainDate, group: SpeciesGroup): Status =>
    evaluate({ rules: sectionRules.get(section) ?? [], on, group });

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
      return { waters: 2, reaches: sectionRules.size, surveyed: null, stations: 1 };
    },

    async itemExists(i) { return i === CHILLIWACK.item || i === JEPERSON.item; },
    async itemForSection(s) {
      if (sectionRules.has(s) && s !== handleOf(JEPERSON.section)) return id<ItemId>(CHILLIWACK.item);
      if (s === handleOf(JEPERSON.section)) return id<ItemId>(JEPERSON.item);
      return null;
    },

    async regsForItem(i, on, group): Promise<ItemRegs | null> {
      if (i !== CHILLIWACK.item) return null;
      return {
        item: id<ItemId>(CHILLIWACK.item), name: CHILLIWACK.name,
        reaches: CHILLIWACK.reaches.map((r) => ({
          section: handleOf(r.section), seq: r.seq,
          // One section per stretch in the hand-written fixture: it names distinct reaches
          // already, so there is nothing to collapse.
          sections: [handleOf(r.section)], pieces: 1,
          lowerLabel: r.lo, upperLabel: r.hi,
          status: statusOf(handleOf(r.section), on, group),
        })),
        rules: [UPSTREAM_CLOSURE, JUNE_CLOSURE, FLY_ONLY],
        area: [{
          rule: { id: "mu.2-2.bait", type: "bait_restriction", family: "gear_and_method",
                  dimension: "bait:any", label: "Bait ban", scope: "mu", via: "reach",
                  group: "provincial", windows: [] },
          scopeLabel: "Everywhere in MU 2-2",
        }],
        verbatim: {
          text: "**No Fishing **upstream from a line between two fishing boundary signs",
          clauseStart: 0, clauseLength: 14,
        },
        unplaceable: [],
      };
    },

    async statusFor(ids, on, group) {
      return new Map(ids.map((s) => [s, statusOf(s, on, group)]));
    },

    async searchNames(q, limit): Promise<readonly NameHit[]> {
      const needle = q.toLowerCase();
      const rows: NameHit[] = [];
      if (CHILLIWACK.name.toLowerCase().includes(needle)) {
        rows.push({ item: id<ItemId>(CHILLIWACK.item), name: CHILLIWACK.name,
                    matchedAs: null, pieces: CHILLIWACK.reaches.length });
      }
      const byAlias = JEPERSON.alias.toLowerCase().includes(needle);
      if (byAlias || JEPERSON.name.toLowerCase().includes(needle)) {
        rows.push({ item: id<ItemId>(JEPERSON.item), name: JEPERSON.name,
                    matchedAs: byAlias ? JEPERSON.alias : null, pieces: 1 });
      }
      return rows.slice(0, limit);
    },
    async searchPlaces(q, limit): Promise<readonly PlaceHit[]> {
      return "chilliwack".includes(q.toLowerCase())
        ? [{ place: id<PlaceId>("chilliwack"), name: "Chilliwack", kind: "city" }].slice(0, limit)
        : [];
    },
    async watersNear(p): Promise<readonly NearHit[]> {
      return p === "chilliwack"
        ? [{ item: id<ItemId>(CHILLIWACK.item), name: CHILLIWACK.name, km: 1.8 }]
        : [];
    },

    async gaugeForSection(s): Promise<GaugeLink | null> {
      if (!sectionRules.has(s)) return null;
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
      return this.gaugeForSection(handleOf(CHILLIWACK.reaches[0]!.section));
    },
    async stationsFor(sections): Promise<ReadonlyMap<SectionId, StationId>> {
      const out = new Map<SectionId, StationId>();
      for (const s of sections)
        if (sectionRules.has(s)) out.set(s, id<StationId>(GAUGE.station));
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
      return sectionRules.has(from) ? [from] : [];
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
      return sectionRules.has(sec)
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
