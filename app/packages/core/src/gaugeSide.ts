/**
 * ON THIS WATER, OR ESTIMATING FROM ANOTHER (T168, owner 2026-10-23: "when it says (13 gauges in view) we need to make
 * a division between real gauges and donor gauges"; 2026-10-24: "nicomen slough has so many not reporting gauges. they
 * need to be hidden under drop down").
 *
 * A panel names every station that may speak for a section — Nicomen Slough's holds its own two gauges and eleven on
 * the Fraser — and the screens listed them as one run. Here is the ONE decision that splits them, and the one grouping
 * every surface draws: the count line, the donor rows, the chart's gauge switch and the route map's key (own first).
 *
 *   own     the station stands on THIS water: its `gauge.item_id` is one of the section's waters — the section's own
 *           item and every water joined to it across `water_continue` (the Vedder and the Chilliwack are one river,
 *           as `build_panels.own_distances` reads a water's own gauges). A station on no named water (no item) is own
 *           only where it stands on the very section asked about.
 *   donor   anything else: another water's gauge the estimate borrows from (a side channel's river, a tributary, the
 *           river a creek flows into, a neighbouring valley).
 *
 * NOT `panel_member.lead`, which is "may lead the answer" — near enough and big enough — and is set on the Fraser's
 * gauges in Nicomen Slough's panel (lead 3–8). NOT `same_river`, which is "on one blue line", true of every Fraser
 * gauge there too. Neither says whose gauge it is.
 *
 * WHETHER A MEMBER IS REPORTING is not decided here: it is `donorState` (T123), and a member is reporting exactly when
 * that state is `reads` (`donorCounted`). The rest — not real-time, quiet, reporting but unranked, no forecast that far
 * ahead — fold under one closed disclosure per group.
 */
import { donorState, type DonorReading, type DonorState, type Horizon } from "./conditions";

export type GaugeSide = "own" | "donor";

/** Where a station stands: its water (`gauge.item_id`) and its reach (`gauge.sid`), null where the release has none. */
export interface GaugePlace { item: string | null; section: number | null }

/** The water being read: its waters (its own item and its continuations) and the section asked about. */
export interface ThisWater { waters: readonly string[]; section: number | null }

/** On this water, or a donor. A station whose place is not known is a donor: "on this water" is a claim, never a default. */
export function gaugeSide(g: GaugePlace | undefined, w: ThisWater): GaugeSide {
  if (!g) return "donor";
  if (g.item !== null) return w.waters.includes(g.item) ? "own" : "donor";
  return g.section !== null && g.section === w.section ? "own" : "donor";
}

/** Each station's side, from `WaterSource.gaugeWaters`' answer — the map every surface reads. */
export function gaugeSides(stations: readonly string[], w: ThisWater,
                           places: ReadonlyMap<string, GaugePlace>): ReadonlyMap<string, GaugeSide> {
  return new Map(stations.map((s) => [s, gaugeSide(places.get(s), w)]));
}

/** The groups' headings. */
export const GAUGE_GROUP_WORDS: Readonly<Record<GaugeSide, string>> = {
  own: "ON THIS WATER",
  donor: "ESTIMATING FROM OTHER WATERS",
};

/** What the folded members are, said for the horizon: today they are not reporting; ahead, not counted. */
export function quietWord(states: readonly DonorState["kind"][], horizon: Horizon = 0): string {
  if (horizon === "change") return "with no forecast running";
  if (horizon) return "not counted this far ahead";
  // "reporting, not ranked" is not "not reporting": the word says both when one is among them
  return states.includes("unranked") ? "not reporting or not ranked" : "not reporting";
}

/** The positive of `quietWord`, for "No gauge on this water reporting". */
function countedWord(states: readonly DonorState["kind"][], horizon: Horizon): string {
  if (horizon === "change") return "with a forecast running";
  if (horizon) return "counted this far ahead";
  return states.includes("unranked") ? "ranked" : "reporting";
}

/** One group of a surface's items: those reporting shown, the rest under `fold`. */
export interface GaugeGroup<T> {
  side: GaugeSide;
  heading: string;
  /** Reporting (`reads`), and any the caller keeps in sight (the switch's chosen gauge), in the given order. */
  shown: readonly T[];
  /** The rest, under the disclosure: closed by default. */
  folded: readonly T[];
  /** The disclosure's words; null when nothing is folded. */
  fold: { label: string; open: string; closed: string } | null;
}

/**
 * THE GROUPS, own first: each item's side and its donor state decide where it goes; `keep` holds an item in sight
 * whatever its state (the gauge the chart shows). The order within a group is the caller's (the panel's weight order).
 * A group with no items is left out.
 */
export function groupGauges<T>(items: readonly T[], sideOf: (t: T) => GaugeSide, stateOf: (t: T) => DonorState["kind"],
                               o: { horizon?: Horizon; keep?: (t: T) => boolean } = {}): GaugeGroup<T>[] {
  const out: GaugeGroup<T>[] = [];
  for (const side of ["own", "donor"] as const) {
    const mine = items.filter((t) => sideOf(t) === side);
    if (!mine.length) continue;
    const folded = mine.filter((t) => stateOf(t) !== "reads" && !o.keep?.(t));
    const shown = mine.filter((t) => !folded.includes(t));
    out.push({ side, heading: GAUGE_GROUP_WORDS[side], shown, folded,
               fold: folded.length ? foldWords(side, folded.map(stateOf), o.horizon ?? 0) : null });
  }
  return out;
}

const gauges = (n: number) => (n === 1 ? "gauge" : "gauges");

/** "6 not reporting", and its screen-reader names closed and open, which say the count, the group and the action. */
function foldWords(side: GaugeSide, states: readonly DonorState["kind"][], horizon: Horizon) {
  const n = states.length, word = quietWord(states, horizon);
  const where = side === "own" ? "on this water" : "from other waters";
  return { label: `${n} ${word}`,
           closed: `${n} ${gauges(n)} ${where} ${word}. Show them`,
           open: `${n} ${gauges(n)} ${where} ${word}. Hide them` };
}

/** A station in the count: its side and its donor state at the horizon asked about. */
export interface CountedGauge { side: GaugeSide; state: DonorState["kind"] }

/** A member's state at the horizon, through `donorState` — the one test of a donor's reading fields. */
export const stateAt = (r: DonorReading, ask: { ref?: number; horizon?: Horizon } = {}): DonorState["kind"] =>
  donorState(r, ask).kind;

/**
 * THE COUNT LINE: "2 gauges on this water · 5 estimating from other waters · 6 not reporting". Reporting members are
 * counted by side; the rest once, together, in the fold's word. Every station is counted exactly once, so the parts
 * add up to the panel. Null with no stations.
 *
 *   no station on this water        "No gauge on this water · …"
 *   own stations, none reporting    "No gauge on this water reporting · …" (they are in the last part; "ranked" where
 *                                   one reports unranked, "counted this far ahead" / "with a forecast running" ahead)
 */
export function gaugeCountLine(gs: readonly CountedGauge[], horizon: Horizon = 0): string | null {
  if (!gs.length) return null;
  const reads = (side: GaugeSide) => gs.filter((g) => g.side === side && g.state === "reads").length;
  const ownAll = gs.filter((g) => g.side === "own").length;
  const own = reads("own"), donor = reads("donor");
  const quiet = gs.filter((g) => g.state !== "reads");
  const parts = [
    ownAll === 0 ? "No gauge on this water"
      : own === 0 ? `No gauge on this water ${countedWord(gs.filter((g) => g.side === "own").map((g) => g.state), horizon)}`
      : `${own} ${gauges(own)} on this water`,
  ];
  if (donor) parts.push(`${donor} estimating from ${donor === 1 ? "another water" : "other waters"}`);
  if (quiet.length) parts.push(`${quiet.length} ${quietWord(quiet.map((g) => g.state), horizon)}`);
  return parts.join(" · ");
}
