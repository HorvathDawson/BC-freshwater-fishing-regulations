/**
 * The donor panel for a spot, turned into an answer.
 *
 * DECIDES NOTHING ITSELF (rule 25): the arithmetic is `estimate` in core, shared with the
 * pipeline and pinned to it by a test. This is the fetch, the join against today's
 * readings, and the ordering the screen displays in — which is the SAME ordering the
 * arithmetic used, so the table and the number above it can never disagree.
 */
import { useMemo } from "react";
import { estimate, trustFor, weightFor, type Answer, type TrustClass } from "@app/core";
import type { Panel, RegsSource, SectionId, StationId } from "@app/data";
import { useAsync } from "./async";

/** One row of the table under the answer — a donor, and what it contributed. */
export interface DonorRow {
  station: StationId;
  role: "up" | "down";
  /** 0–1, this donor's own percentile today. Null when it is not reporting. */
  percentile: number | null;
  /** Its share of the answer, 0–1, normalised across the panel. */
  weight: number;
  /** How many times bigger the larger of the two catchments is. Always ≥ 1. */
  areaRatio: number;
  trust: TrustClass;
  years: number;
}

export interface PanelAnswer {
  answer: Answer;
  rows: readonly DonorRow[];
}

type Index = { stations: Record<string, { percentile: number | null;
                                          discharge?: number | null;
                                          level?: number | null;
                                          parameter?: "discharge" | "level" }> } | null;

/**
 * Build the answer and its working from a panel and today's index.
 *
 * Exported and pure so it can be tested without a database or a network — the hook below
 * is only the fetching around it.
 */
export function answerFrom(panel: Panel | undefined, index: Index,
                           quantity: "discharge" | "level" = "discharge"): PanelAnswer {
  if (!panel) return { answer: { ok: false, why: "no-station" }, rows: [] };
  if (!index) return { answer: { ok: false, why: "offline" }, rows: [] };

  const contributions = [];
  const raw: (DonorRow & { _w: number })[] = [];
  for (const m of panel.members) {
    const row = index.stations[m.station];
    // `percentile` is the station's own default and `parameter` says which quantity it is
    // about, so it stands in for that one only — a level percentile read as a flow is
    // arithmetic across two units.
    const own = row
      ? row[quantity] ?? ((row.parameter ?? "discharge") === quantity
                          ? row.percentile ?? null : null)
      : null;
    const ratio = panel.areaKm2 && m.areaKm2
      ? Math.max(panel.areaKm2, m.areaKm2) / Math.min(panel.areaKm2, m.areaKm2) : Infinity;
    const share = Number.isFinite(ratio) ? 1 / ratio : 0;
    const w = own === null ? 0 : weightFor(share, m.role, m.years);
    raw.push({ station: m.station, role: m.role, percentile: own, weight: 0,
               areaRatio: ratio, trust: trustFor(ratio).klass, years: m.years, _w: w });
    if (own !== null && panel.areaKm2)
      contributions.push({ percentile: own, role: m.role, areaKm2: m.areaKm2,
                           years: m.years });
  }
  const total = raw.reduce((a, r) => a + r._w, 0) || 1;
  const rows = raw
    .map(({ _w, ...r }) => ({ ...r, weight: _w / total }))
    .sort((a, b) => b.weight - a.weight);
  return { answer: estimate(panel.areaKm2, contributions), rows };
}

/** The panel for one section, joined against today's readings. */
export function usePanel(
  source: RegsSource,
  feed: { index(): Promise<Index> } | undefined,
  section: SectionId | null,
  quantity: "discharge" | "level" = "discharge",
): PanelAnswer {
  const got = useAsync(
    async () => {
      if (!section) return null;
      const [panels, idx] = await Promise.all([
        source.panelsFor([section]),
        feed ? feed.index() : Promise.resolve(null),
      ]);
      return answerFrom(panels.get(section), idx, quantity);
    },
    `panel:${section ?? ""}:${quantity}`,
    section !== null,
  );
  const none = useMemo<PanelAnswer>(
    () => ({ answer: { ok: false, why: "no-station" }, rows: [] }), []);
  return got.state === "ready" && got.value ? got.value : none;
}
