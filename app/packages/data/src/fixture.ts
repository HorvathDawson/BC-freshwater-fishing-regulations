/**
 * An in-memory RegsSource holding a slice of the REAL Chilliwack valley build.
 *
 * Not a toy. Every value below came out of build 54ea0bb4 or a live feed on 30 Aug 2026,
 * so the conformance suite exercises the cases that actually broke things: a river with
 * several regulated stretches, a June closure, a rule nobody could place, a gauge running
 * at its 4th percentile, and a creek the Fraser gauge must refuse to speak for.
 */
import type { Band, PlainDate, Rule, SpeciesGroup, Status } from "@app/core";
import { evaluate, gaugeTrust } from "@app/core";
import type {
  Aged, BundleInfo, GaugeLink, ItemId, ItemRegs, LakeInfo, NameHit, NearHit, Parameter,
  PlaceHit, PlaceId, Reading, RegsSource, Release, SectionId, Series, StationId,
} from "./index";

const id = <T extends string>(s: string): T => s as T;

const JUNE_CLOSURE: Rule = {
  id: "chilliwack_vedder_rivers.r5", kind: "closure", scope: "section",
  group: "provincial", windows: [{ from: { month: 6, day: 1 }, to: { month: 6, day: 30 } }],
};
const FLY_ONLY: Rule = {
  id: "chilliwack_vedder_rivers.r4a", kind: "gear_restriction", scope: "section",
  group: "provincial", subject: "gear",
  windows: [{ from: { month: 5, day: 1 }, to: { month: 5, day: 31 } }],
};
const UPSTREAM_CLOSURE: Rule = {
  id: "chilliwack_vedder_rivers.r1", kind: "closure", scope: "section",
  group: "provincial", windows: [],
};
/** Real: the Fraser side-channel closure for which no extent was ever authored. */
const UNPLACEABLE: Rule = {
  id: "fraser_river_region2.r4", kind: "closure", scope: "section",
  group: "provincial", uncertain: true,
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

const GAUGE = {
  station: "08MH001", name: "Chilliwack River at Vedder Crossing",
  magnitude: 2182, discharge: 15.7, level: 1.487, percentile: 0.038,
  at: "2026-08-30T07:00:00Z",
};
const FRASER_AT_HOPE_MAGNITUDE = 273576;

export function makeFixtureSource(now = Date.parse("2026-08-30T12:00:00Z")): RegsSource {
  const sectionRules = new Map<string, Rule[]>(
    CHILLIWACK.reaches.map((r) => [r.section, r.rules]),
  );
  sectionRules.set(JEPERSON.section, [UNPLACEABLE]);

  const statusOf = (section: string, on: PlainDate, group: SpeciesGroup): Status =>
    evaluate({ rules: sectionRules.get(section) ?? [], on, group });

  return {
    async info(): Promise<BundleInfo> {
      return { version: "54ea0bb4", validUntil: "2027-03-31" };
    },
    async itemExists(i) { return i === CHILLIWACK.item || i === JEPERSON.item; },
    async itemForSection(s) {
      if (sectionRules.has(s) && s !== JEPERSON.section) return id<ItemId>(CHILLIWACK.item);
      if (s === JEPERSON.section) return id<ItemId>(JEPERSON.item);
      return null;
    },

    async regsForItem(i, on, group): Promise<ItemRegs | null> {
      if (i !== CHILLIWACK.item) return null;
      return {
        item: id<ItemId>(CHILLIWACK.item), name: CHILLIWACK.name,
        reaches: CHILLIWACK.reaches.map((r) => ({
          section: id<SectionId>(r.section), seq: r.seq,
          lowerLabel: r.lo, upperLabel: r.hi,
          status: statusOf(r.section, on, group),
        })),
        rules: [UPSTREAM_CLOSURE, JUNE_CLOSURE, FLY_ONLY],
        area: [{
          rule: { id: "mu.2-2.bait", kind: "gear_restriction", scope: "mu",
                  group: "provincial", subject: "bait", windows: [] },
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
      // A tiny creek in the same window is measured by the Fraser at Hope, which must
      // refuse to speak for it — that is the whole point of the magnitude floor.
      const reachMagnitude = s === JEPERSON.section ? 12 : GAUGE.magnitude;
      const gaugeMagnitude = s === JEPERSON.section ? FRASER_AT_HOPE_MAGNITUDE : GAUGE.magnitude;
      return {
        station: id<StationId>(GAUGE.station), name: GAUGE.name,
        trust: gaugeTrust(reachMagnitude, gaugeMagnitude),
        reachMagnitude, gaugeMagnitude,
        live: true, areaKm2: 1230, lon: -121.958, lat: 49.096, section: s,
      };
    },

    async gaugeForItem(i): Promise<GaugeLink | null> {
      // The best reach on the water, which is not the same as any particular one: the
      // Jeperson creek section would answer "weak", the river as a whole answers "good".
      if (i !== CHILLIWACK.item) return null;
      return this.gaugeForSection(id<SectionId>(CHILLIWACK.reaches[0]!.section));
    },
    async stationsFor(sections): Promise<ReadonlyMap<SectionId, StationId>> {
      const out = new Map<SectionId, StationId>();
      for (const s of sections)
        if (sectionRules.has(s)) out.set(s, id<StationId>(GAUGE.station));
      return out;
    },

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
    async gaugeSeries(st, span, parameter): Promise<Aged<Series> | null> {
      if (st !== GAUGE.station) return null;
      const param: Parameter = parameter ?? "discharge";
      const level = param === "level";
      // Two envelopes in two units, because that is the shape of the real thing: a station
      // measuring both has one band in m3/s and one in metres, and a fixture that carried
      // only one would let a unit bug through the tests that exist to catch it.
      const band: Band = level ? [1.20, 1.35, 1.52, 1.74, 2.05] : [18.8, 23.0, 30.6, 39.0, 56.6];
      const value = level ? GAUGE.level : GAUGE.discharge;
      // The year axis is 73 pentads of envelope with today marked on it — no observations,
      // exactly as the bundle source builds it.
      const n = span === "72h" ? 72 : 73;
      return {
        fetchedAt: now,
        value: {
          step: span === "72h" ? "1h" : "5d",
          from: span === "72h" ? "2026-08-27T07:00:00Z" : "2026-01-01",
          parameter: param,
          values: Array.from({ length: n }, () => (span === "72h" ? value : null)),
          band: Array.from({ length: n }, () => band),
          now: { index: span === "72h" ? n - 1 : 48, value },
          forecast: level ? null : {
            model: "ELF", issuedAt: "2026-08-30T00:00:00Z", horizonDays: 30,
            value: 9.4, extreme: "min", min: 9.4, ave: 12.1, max: 16.8, unit: "m3/s",
          },
        },
      };
    },
    async gaugeParameters(st): Promise<readonly Parameter[]> {
      return st === GAUGE.station ? ["discharge", "level"] : [];
    },
    async traceToGauge(from) {
      return sectionRules.has(from) ? [id<SectionId>(from)] : [];
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
