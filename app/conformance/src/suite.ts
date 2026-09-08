/**
 * ONE suite, run against EVERY RegsSource implementation.
 *
 * This is the file that keeps mobile and web answering the same question the same way.
 * When a query is added to the interface it is added here in the SAME commit — otherwise
 * an implementation can diverge with no test noticing, which is exactly how v1's two
 * clients ended up 1,203 lines apart.
 *
 * The cases are the ones that actually go wrong, not a coverage sweep: a river with
 * several regulated stretches, a seasonal closure, a rule nobody could place, an alias
 * search, and a gauge that must refuse to speak for water it does not measure.
 */
import { expect, it } from "vitest";
import type { PlainDate } from "@app/core";
import type { ItemId, RegsSource, SectionId, StationId } from "@app/data";

const AUG = { year: 2026, month: 8, day: 30 } satisfies PlainDate;
const JUN = { year: 2026, month: 6, day: 15 } satisfies PlainDate;
const CHILLIWACK = "gnis:8634" as ItemId;
/**
 * A handle no table issues, for "look up something that is not there". Not `0` — that is
 * reserved for "no section" and would test a different thing — and not a big round number
 * pulled from nowhere: it is past the end of any table either source builds.
 */
const NO_SUCH_SECTION = 2_000_000_000 as SectionId;

/**
 * THE SECTIONS COME FROM THE CALLER, and that is forced rather than tidy.
 *
 * A section is an integer HANDLE — an index into the atlas's section_handles.txt — so it is
 * meaningful only against the artifact that minted it. This suite runs against every
 * RegsSource, and the fixture's table and the province bundle's table are different tables:
 * a literal here would pass against one source and fail against the next, which is the
 * opposite of what a conformance suite is for. So each source names its own two sections
 * and the suite tests the BEHAVIOUR. `item_id` stays written down, because that one is
 * durable across rebuilds (99.88%) and identical in every source.
 */
export interface ConformanceSections {
  /** The lowest reach of the named water — the one carrying the June closure. */
  lowerChilliwack: SectionId;
  /** The side channel whose only rule could never be placed. */
  jeperson: SectionId;
}

export function runConformance(name: string, make: () => Promise<RegsSource>,
                               sections: ConformanceSections) {
  const T = (what: string, fn: (s: RegsSource) => Promise<void>) =>
    it(`${name}: ${what}`, async () => { await fn(await make()); });
  const LOWER_REACH = sections.lowerChilliwack;
  const JEPERSON = sections.jeperson;

  // ---- identity ----
  T("reports a bundle version and an edition expiry", async (s) => {
    const i = await s.info();
    expect(i.version).toBeTruthy();
    // Past the expiry the client must degrade loudly; it cannot do that without a date.
    expect(i.validUntil).toBeTruthy();
  });

  T("reports counts that belong to THIS bundle", async (s) => {
    // The app used to state these as literals copied from the design mock — 255 waters and
    // 5 stations, against a province bundle holding 19,699 and 2,324. A count a screen
    // shows about the data has to come from the data, so every source must answer.
    const c = await s.counts();
    expect(c.waters).toBeGreaterThan(0);
    expect(c.reaches).toBeGreaterThan(0);
    expect(c.stations).toBeGreaterThan(0);
    // `null` is the honest answer for something not built yet; 0 would claim we looked.
    expect(c.surveyed === null || c.surveyed > 0).toBe(true);
    // Every reach the search can reach belongs to a water, so there is never less than one
    // reach per named water. A bundle that says otherwise has lost rows.
    expect(c.reaches).toBeGreaterThanOrEqual(c.waters);
  });

  T("a missing item is absent, never a silent empty answer", async (s) => {
    expect(await s.itemExists("gnis:does-not-exist" as ItemId)).toBe(false);
  });

  T("a section resolves to the item that owns it", async (s) => {
    expect(await s.itemForSection(LOWER_REACH)).toBe(CHILLIWACK);
    expect(await s.itemForSection(NO_SUCH_SECTION)).toBeNull();
  });

  // ---- regulations ----
  T("a sheet arrives in ONE call, with its stretches in order", async (s) => {
    const r = await s.regsForItem(CHILLIWACK, AUG, "provincial");
    expect(r).not.toBeNull();
    const seqs = r!.reaches.map((x) => x.seq);
    expect(seqs).toEqual([...seqs].sort((a, b) => a - b));
    // A river with several regulated stretches must not collapse to one answer.
    expect(r!.reaches.length).toBeGreaterThan(1);
  });

  T("every stretch names the landmarks that bound it", async (s) => {
    const r = await s.regsForItem(CHILLIWACK, AUG, "provincial");
    // Without these the sheet cannot tell a person WHICH stretch they tapped.
    expect(r!.reaches.some((x) => x.lowerLabel || x.upperLabel)).toBe(true);
  });

  T("zone and province-wide rules are carried, and labelled for a reader", async (s) => {
    const r = await s.regsForItem(CHILLIWACK, AUG, "provincial");
    expect(r!.area.length).toBeGreaterThan(0);
    for (const a of r!.area) expect(a.scopeLabel).toBeTruthy();
  });

  T("the date decides: a June closure is closed in June and open in August", async (s) => {
    const jun = await s.statusFor([LOWER_REACH], JUN, "provincial");
    const aug = await s.statusFor([LOWER_REACH], AUG, "provincial");
    expect(jun.get(LOWER_REACH)!.outcome).toBe("closed");
    expect(aug.get(LOWER_REACH)!.outcome).not.toBe("closed");
  });

  T("statusFor answers a whole viewport in one call", async (s) => {
    const m = await s.statusFor([LOWER_REACH, JEPERSON], AUG, "provincial");
    expect(m.size).toBe(2);
  });

  T("a rule nobody could place reads UNKNOWN, never open", async (s) => {
    // The Fraser side-channel closure: no extent was ever authored for it.
    const m = await s.statusFor([JEPERSON], AUG, "provincial");
    expect(m.get(JEPERSON)!.outcome).toBe("unknown");
  });

  // ---- search ----
  T("finds a water by its display name", async (s) => {
    const hits = await s.searchNames("chilliwack", 10);
    expect(hits.some((h) => h.item === CHILLIWACK)).toBe(true);
  });

  T("finds a water by an alias, and says which alias matched", async (s) => {
    const hits = await s.searchNames("greyell", 10);
    expect(hits.length).toBeGreaterThan(0);
    expect(hits[0]!.matchedAs).toBeTruthy();
  });

  T("honours the limit", async (s) => {
    expect((await s.searchNames("chilliwack", 0)).length).toBe(0);
  });

  T("a town yields the water near it, nearest first", async (s) => {
    const places = await s.searchPlaces("chilliwack", 5);
    expect(places.length).toBeGreaterThan(0);
    const near = await s.watersNear(places[0]!.place);
    expect(near.length).toBeGreaterThan(0);
    const km = near.map((n) => n.km);
    expect(km).toEqual([...km].sort((a, b) => a - b));
  });

  // ---- conditions ----
  T("a gauge reading always carries its age", async (s) => {
    const g = await s.gaugeForSection(LOWER_REACH);
    const now = await s.gaugeNow(g!.station);
    // A stale reading rendered as a live one is the failure this field exists to prevent.
    expect(now!.fetchedAt).toBeTypeOf("number");
    expect(now!.value.standing).toBeTruthy();
  });

  T("a reading's percentile is what the colour means, not its magnitude", async (s) => {
    const g = await s.gaugeForSection(LOWER_REACH);
    const now = await s.gaugeNow(g!.station);
    expect(now!.value.percentile).not.toBeNull();
  });

  T("a gauge refuses to speak for water it does not measure", async (s) => {
    // The Fraser at Hope drains 216,600 km2; it knows nothing about a 12-magnitude creek.
    //
    // THE REFUSAL IS A NULL LINK, NOT A BAND CALLED "none". `pipeline/gauges/consume/shed.py` bands
    // only what it will vouch for and writes NO `section_gauge` ROW below the `weak` floor,
    // so there is nothing for the source to return. This asserted `"none"` for as long as
    // three separate implementations of the trust rule existed — the pipeline's, one in
    // @app/core, and one in the fixture builder — and only the last two could produce it.
    expect(await s.gaugeForSection(JEPERSON)).toBeNull();
    const near = await s.gaugeForSection(LOWER_REACH);
    expect(near!.trust).toBe("good");
  });

  T("the series carries the percentile envelope beside the observations", async (s) => {
    const g = await s.gaugeForSection(LOWER_REACH);
    const series = await s.gaugeSeries(g!.station, "72h");
    expect(series!.value.values.length).toBeGreaterThan(0);
    expect(series!.value.band.length).toBeGreaterThan(0);
  });

  T("a series says which quantity it is, and the envelope matches it", async (s) => {
    // The whole reason `parameter` is on the series. A level compared against a discharge
    // envelope produces a percentile that looks entirely reasonable and means nothing, and
    // nothing downstream could tell the difference — the numbers are both just numbers.
    const g = await s.gaugeForSection(LOWER_REACH);
    for (const p of await s.gaugeParameters(g!.station)) {
      const series = await s.gaugeSeries(g!.station, "72h", p);
      expect(series!.value.parameter, `series asked for ${p}`).toBe(p);
    }
  });

  T("the year span is an envelope with today on it, not a relabelled 72 hours", async (s) => {
    const g = await s.gaugeForSection(LOWER_REACH);
    const year = await s.gaugeSeries(g!.station, "year");
    // A POINT PER DAY. The envelope is sampled every five days because percentiles are
    // noisy at daily resolution, but the line across it is this year's own record and
    // belongs at its own resolution — on 73 buckets a whole autumn is fourteen points.
    expect(year!.value.step).toBe("1d");
    expect(year!.value.at.length).toBe(year!.value.values.length);
    expect(year!.value.band.length).toBe(year!.value.values.length);
    // YEAR TO DATE PLUS A MONTH, never the whole calendar year: past that the record has
    // nothing in it and the longest forecast has already ended, so the rest of the frame
    // would be an empty band.
    expect(year!.value.at.length).toBeGreaterThan(180);
    expect(year!.value.at[0]).toMatch(/-01-01$/);
    const last = new Date(year!.value.at[year!.value.at.length - 1]!);
    const today = new Date(year!.value.at[(year!.value.now?.index ?? 0)]!);
    const ahead = (last.getTime() - today.getTime()) / 86_400_000;
    expect(ahead).toBeGreaterThan(20);
    expect(ahead).toBeLessThanOrEqual(31);
    // The marker is explicit rather than "the last value": most of the year's cells are
    // null, and taking the last would put today on New Year's Eve.
    expect(year!.value.now).not.toBeNull();
  });

  T("never puts a discharge forecast on a chart of metres", async (s) => {
    // The BC River Forecast Centre models discharge and nothing else. The Fraser at Mission
    // reports STAGE — around 1.8 m — and its outlook is 3,157 m3/s; drawn on one axis that
    // is two units in one frame, and the forecast leaves the picture entirely.
    const g = await s.gaugeForSection(LOWER_REACH);
    const level = await s.gaugeSeries(g!.station, "72h", "level");
    if (level) expect(level.value.forecast).toBeNull();
  });

  T("an unknown station is null, not an empty reading", async (s) => {
    expect(await s.gaugeNow("00XX000" as StationId)).toBeNull();
  });

  T("a tap traces downstream to the gauge that measures it", async (s) => {
    expect((await s.traceToGauge(LOWER_REACH)).length).toBeGreaterThan(0);
  });

  // ---- lakes ----
  T("a lake reports its charts and when it was last stocked", async (s) => {
    const info = await s.lakeInfo("wbk:329083342" as ItemId);
    expect(info!.charts.length).toBeGreaterThan(0);
    for (const c of info!.charts) expect(["digitised", "scan"]).toContain(c.kind);
    expect(info!.lastStocked).not.toBeNull();
  });

  T("stocking history is available separately from the map's one number", async (s) => {
    expect((await s.stockingHistory("wbk:329083342" as ItemId)).length).toBeGreaterThan(0);
  });
}
