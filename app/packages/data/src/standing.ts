/**
 * THE CONDITIONS MAP'S STANDINGS, SPLIT INTO WHAT NEVER CHANGES AND WHAT CHANGES WITH THE FEED (owner 2026-10-21: "we
 * need to do the same painting speed up for the conditions map too").
 *
 * A section's standing is `answerFrom(its panel, the feed, quantity, horizon)` — and the panel is the release's: which
 * donors speak for it and the section's own target area (paint + flow-area). So everything that depends on the
 * SECTION is fixed for the release, and only the feed, the quantity and the horizon move. Measured on r-7b6e802bf22b:
 * 249,415 sections with a panel, 3,281 panels, 4,586 distinct target areas — and only **18,548 distinct (panel, area)
 * pairs**. Every section of one pair gets the same answer from the same feed, because the weights depend on nothing
 * else (`estimate` reads the members and the target area, nothing more).
 *
 * So a section's CLASS (`StandingClasses.classOf`: a (panel, target area) pair, or a lake's own station) is fixed for the
 * release, and a feed refresh or a horizon change asks one standing PER CLASS (`classStanding`, computed when a loaded
 * section first needs it — hundreds per change, not 18.5k). The map writes the class's value into each section's
 * feature-state as its tile arrives (`@app/map` `colouring.ts`, the `standing` channel). Not per panel: two sections of
 * one panel with different target areas weigh the same donors differently, so a per-panel value would be a different
 * answer from the sheet's for most of them.
 *
 * ONE ARITHMETIC (AGENTS 23, rule 25): `sectionStanding` is the only place a standing is read off a panel or a lake
 * station; the route map's path (`usePanelStandings`) and the class path both call it, so the map and the
 * sheet cannot disagree. `conformance/src/release.test.ts` holds every class value to the sheet's own answer
 * (`answerFrom`) for every section of the fixture release, on both readers, at every horizon and quantity.
 *
 * ONE SPEAKER (T105): "which gauge speaks for this section?" is `speakerFor` — its panel, else the station in its lake,
 * else nothing. The map's classes, the viewport standings and the conditions sheet (`useSpeaker` / `sheetLead` in
 * @app/ui) all ask it. `section_gauge` (`gaugeForSection`) is NOT a speaker: it stays the reach-to-station match record
 * (the trace), and the sheet once quoted its station for 54,386 sections the map left uncoloured.
 */
import { basinStanding, donorCounted, recordYears, type BasinVote, type Horizon } from "@app/core";
import { answerFrom, panelStanding, reading, type Quantity } from "./panel";
import type { BasinMember, Panel, PanelMember, SectionId, StationId } from "./index";

/** The live feed's index, as `answerFrom` reads it. */
export type FeedIndex = Parameters<typeof answerFrom>[1];

/**
 * WHO SPEAKS FOR A SECTION: its donor panel (at the section's own target area), or the station standing IN its lake.
 * Null (from `speakerFor`) is nothing: the map leaves the water uncoloured and the sheet refuses.
 */
export type Speaker =
  | { readonly kind: "panel"; readonly panel: Panel }
  | { readonly kind: "lake"; readonly station: StationId };

/** What one class of sections asks the feed: one speaker, shared by every section of the class. */
export type StandingClass = Speaker;

/**
 * THE ONE DECISION (T105): which gauge speaks for this section, from what the release holds about it — its panel
 * (`panelsFor`) and the station in its lake (`lakeStationsFor`).
 *
 * A PANEL WINS over a lake station, as the viewport path always had it (lakes were set first and panels written over).
 * `section_gauge` is not an input: a match is not a speaker (the panel's own gate, `panel.py` `eligible`, decides who
 * may speak; `shed.py`'s match has no regulation or record gate).
 */
export function speakerFor(panel: Panel | undefined, lake: StationId | undefined): Speaker | null {
  if (panel) return { kind: "panel", panel };
  if (lake !== undefined) return { kind: "lake", station: lake };
  return null;
}

/** Whether `station` is one of the speaker's own: a member of its panel, or its lake's station. */
export function speaks(speaker: Speaker | null, station: string | null | undefined): boolean {
  if (!speaker || station == null) return false;
  return speaker.kind === "lake" ? speaker.station === station
    : speaker.panel.members.some((m) => m.station === station);
}

/**
 * WHICH OF THE SPEAKER'S STATIONS THE SHEET LEADS WITH (its chart and reading): the reader's pick when it is one of
 * the speaker's; else the heaviest reporting donor (`rows` are `answerFrom`'s, in weight order), any donor, the panel's
 * first member (offline: no rows); a lake's own station; nothing where nothing speaks. Never a station the speaker
 * does not hold, so the sheet cannot quote a gauge the map does not colour by.
 */
export function sheetLead(speaker: Speaker | null,
                          rows: readonly { readonly station: StationId; readonly percentile: number | null }[],
                          picked: StationId | null): StationId | null {
  if (!speaker) return null;
  if (speaks(speaker, picked)) return picked;
  if (speaker.kind === "lake") return speaker.station;
  return rows.find((r) => donorCounted(r))?.station ?? rows[0]?.station
    ?? speaker.panel.members[0]?.station ?? null;
}

/**
 * THE STATIC HALF, for one release: each section's class, the classes, and the basins' stations. Built once per release
 * (`WaterSource.standingClasses`); the map labels a section with `classOf` as its tile arrives.
 */
export interface StandingClasses {
  /** The release the classes were built from. A different object is a different labelling: the map relabels. */
  readonly release: string;
  readonly classes: readonly StandingClass[];
  /** A section's class, or null when nothing speaks for it (drawn as unmeasured water, as before). */
  classOf(section: number): number | null;
  /** Each catchment's stations (the zoomed-out field), as `basinMembers` reads them. */
  readonly basins: ReadonlyMap<string, readonly BasinMember[]>;
}

/**
 * A SECTION'S STANDING — the one reading, for the map in either path and for the class values.
 *
 *   null    nothing speaks for it (no panel, no lake station, the lake's station not transmitting, or offline)
 *   -0.01   a panel or a lake station exists but answered this question with nothing (the "no baseline" dash)
 *   0..1    the percentile
 *
 * A PANEL WINS over a lake station, as the viewport path always had it (lakes were set first and panels written over).
 */
export function sectionStanding(panel: Panel | undefined, lake: StationId | undefined, idx: FeedIndex,
                                quantity: Quantity, horizon: Horizon): number | null {
  return speakerStanding(speakerFor(panel, lake), idx, quantity, horizon);
}

/** The standing of one speaker (`sectionStanding`'s arithmetic, after `speakerFor` has decided who speaks). */
export function speakerStanding(speaker: Speaker | null, idx: FeedIndex, quantity: Quantity,
                                horizon: Horizon): number | null {
  if (!idx) return null;                         // offline: colour nothing rather than colour wrong
  if (!speaker) return null;
  if (speaker.kind === "panel") return panelStanding(speaker.panel, idx, quantity, horizon);
  const row = idx.stations[speaker.station as string];
  if (!row) return null;                         // not transmitting: say nothing
  const p = reading(row, quantity, horizon);
  return typeof p === "number" ? p : -0.01;
}

/** Every class's standing for this feed, quantity and horizon, in class order; NaN where `sectionStanding` is null. */
export function classStandings(c: StandingClasses, idx: FeedIndex, quantity: Quantity,
                               horizon: Horizon): Float64Array {
  const out = new Float64Array(c.classes.length);
  for (let i = 0; i < c.classes.length; i++) out[i] = classStanding(c, i, idx, quantity, horizon);
  return out;
}

/** One class's standing (`speakerStanding` of its speaker); NaN where that is null. */
export function classStanding(c: StandingClasses, i: number, idx: FeedIndex, quantity: Quantity,
                              horizon: Horizon): number {
  const k = c.classes[i];
  if (!k) return NaN;
  const v = speakerStanding(k, idx, quantity, horizon);
  return v === null ? NaN : v;
}

/**
 * THE FIELD — one value per catchment, from every gauge in it (`basinStanding`), in the reader's quantity and horizon.
 * A group with nothing to say is LEFT OUT: at a province on screen "we cannot say" is one answer, not two.
 */
export function basinStandings(basins: ReadonlyMap<string, readonly BasinMember[]>, idx: FeedIndex,
                               quantity: Quantity, horizon: Horizon): Map<string, number> {
  const out = new Map<string, number>();
  if (!idx) return out;
  for (const [basin, members] of basins) {
    const votes: BasinVote[] = [];
    for (const m of members) {
      const r = reading(idx.stations[m.station as string], quantity, horizon);
      if (typeof r === "number") votes.push({ percentile: r, areaKm2: m.areaKm2, years: recordYears(m.years, quantity) });
    }
    const p = basinStanding(votes);
    if (typeof p === "number") out.set(basin, p);
  }
  return out;
}

/** One section of a release, as a source reads it: its panel's id, the members, and the section's target area. */
export interface PanelledSection { sid: number; panel: number; areaKm2: number | null }

/**
 * THE CLASSES, from what a source holds: every panelled section (any order), the panels' members, the lake sections'
 * stations, and the basins. A class is one (panel, target area) pair — the only inputs `answerFrom` reads off a section
 * — or one lake station for a section no panel speaks for. Looked up by binary search over the sorted section ids in
 * two typed arrays (~2 MB for the province's ~250k panelled sections). No string key, tuple or Set per section: the
 * release's sections arrive in id order (paint's runs), so the build at the first Conditions visit is one pass.
 */
export function buildStandingClasses(release: string, sections: Iterable<PanelledSection>,
                                     members: ReadonlyMap<number, readonly PanelMember[]>,
                                     lakes: ReadonlyMap<SectionId | number, StationId>,
                                     basins: ReadonlyMap<string, readonly BasinMember[]>): StandingClasses {
  const classes: StandingClass[] = [];
  /** panel -> target area (none as NaN: a Map key compares NaN equal to itself) -> class */
  const byPanel = new Map<number, Map<number, number>>();
  let sids: number[] = [], cls: number[] = [];
  let sorted = true;
  for (const s of sections) {
    const area = s.areaKm2 ?? NaN;
    let byArea = byPanel.get(s.panel);
    if (!byArea) byPanel.set(s.panel, (byArea = new Map()));
    let k = byArea.get(area);
    if (k === undefined) {
      k = classes.length;
      byArea.set(area, k);
      classes.push(speakerFor({ areaKm2: s.areaKm2, members: members.get(s.panel) ?? [] }, undefined)!);
    }
    if (sids.length && s.sid <= sids[sids.length - 1]!) sorted = false;
    sids.push(s.sid);
    cls.push(k);
  }
  if (!sorted) {
    const order = sids.map((_, i) => i).sort((a, b) => sids[a]! - sids[b]!);
    sids = order.map((i) => sids[i]!);
    cls = order.map((i) => cls[i]!);
  }
  // WHO SPEAKS is `speakerFor`'s (a panel wins over a lake station): a lake class only where it answers "the lake"
  const byStation = new Map<string, number>();
  const extra: [number, number][] = [];
  for (const [sid, station] of lakes) {
    const n = Number(sid);
    const speaker = speakerFor(at(sids, n) >= 0 ? PANELLED : undefined, station);
    if (speaker?.kind !== "lake") continue;
    let k = byStation.get(station as string);
    if (k === undefined) {
      k = classes.length;
      byStation.set(station as string, k);
      classes.push(speaker);
    }
    extra.push([n, k]);
  }
  extra.sort((a, b) => a[0] - b[0]);
  const ids = new Float64Array(sids.length + extra.length), of = new Int32Array(ids.length);
  for (let i = 0, j = 0, o = 0; o < ids.length; o++) {
    if (j >= extra.length || (i < sids.length && sids[i]! < extra[j]![0])) { ids[o] = sids[i]!; of[o] = cls[i++]!; }
    else { ids[o] = extra[j]![0]; of[o] = extra[j++]![1]; }
  }
  const classOf = (section: number): number | null => {
    const i = at(ids, section);
    return i < 0 ? null : of[i]!;
  };
  return { release, classes, classOf, basins };
}

/** A section a panel speaks for, as `speakerFor` sees it when only that a panel exists matters (the class build). */
const PANELLED: Panel = { areaKm2: null, members: [] };

/** Where `section` sits in sorted `ids`, or -1. */
function at(ids: ArrayLike<number>, section: number): number {
  let lo = 0, hi = ids.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1, s = ids[mid]!;
    if (section < s) hi = mid - 1;
    else if (section > s) lo = mid + 1;
    else return mid;
  }
  return -1;
}
