/**
 * (T211) THE STANDARD / SALMON TOGGLE AND THE SALMON COLOURING (owner 2026-10-27: in the main map's standard regulation
 * mode, a toggle between Standard and Salmon; Salmon paints the map from DFO's salmon rules, styled differently).
 *
 * FAST BY CONSTRUCTION (owner 2026-10-27: "toggling Standard/Salmon should be instant — no re-fetch, no re-tile"). The
 * release's `salmon` file is read ONCE per source, on the Map tab on a proven pair, after the day's status is in (so it
 * never competes with first paint) and WHATEVER THE TOGGLE SAYS; the feed handed to the map does not depend on the toggle,
 * so the engine labels every tile with its chain as it arrives. A toggle then moves only `mapModes`' modes — one paint
 * change per layer, no fetch, no relabel (`salmonColour.render.test.tsx` and `mapModes.test.ts` pin both).
 *
 * The answer per chain and day was decided at deliver time (`pipeline/deliver/salmon.py`); nothing here reads a rule.
 */
import { useMemo, useState } from "react";
import type { SalmonFacts } from "@app/core";
import type { WaterSource } from "@app/data";
import { useAsync } from "./async";
import { sourceKey } from "./conditionsColour";

/** Which regulations the Map tab paints: the provincial day's status, or DFO's salmon. */
export type RegsLayer = "standard" | "salmon";

/** What the map is handed (`MapProps.salmon`): the release's facts and the day. */
export interface SalmonFeedVM {
  readonly facts: SalmonFacts;
  readonly day: Date;
}

/**
 * The release's DFO salmon on the Map tab, on a proven pair, once the day's status is in. `day` the map's day (null
 * today). The SAME object while the facts and the day are — the toggle is not an input.
 */
export function useSalmonColour(i: { source: WaterSource; paired: boolean; statusOk: boolean; mapTab: boolean;
                                     day: Date | null }): { feed: SalmonFeedVM | null; facts: SalmonFacts | null } {
  const on = i.paired && i.statusOk && i.mapTab;
  // asked once per source and kept: leaving the Map tab does not forget it (a return is instant too)
  const [asked, setAsked] = useState<string | null>(null);
  const key = `salmonfacts:${sourceKey(i.source)}`;
  if (on && asked !== key) setAsked(key);
  const facts = useAsync(() => i.source.salmonFacts(), key, asked === key);
  const f = facts.state === "ready" ? facts.value : null;
  const dayKey = i.day ? `${i.day.getFullYear()}-${i.day.getMonth()}-${i.day.getDate()}` : "today";
  // eslint-disable-next-line react-hooks/exhaustive-deps -- the day's identity is its date, not the Date object
  const day = useMemo(() => i.day ?? new Date(), [dayKey]);
  const feed = useMemo(() => (f && i.paired ? { facts: f, day } : null), [f, day, i.paired]);
  return { feed, facts: f };
}

/** The toggle's choice, kept for the session (Standard by default). */
export function useRegsLayer(): readonly [RegsLayer, (p: RegsLayer) => void] {
  const [p, setP] = useState<RegsLayer>("standard");
  return [p, setP] as const;
}
