/**
 * "Flow that day" on a saved spot (T170): what the spot sheet says about the visit day's own flow, and the "Get the
 * flow for that day" action — decided here (rule 25), drawn by `SpotScreen`.
 *
 * The record and every sentence are `@app/data/spots`' (`dayFlowLine`, `DAY_FLOW_WORDS`); the asking is the app's
 * (`ask`: `fillDayFlow` through the one read, `flowOnDay`, then the spot saved). This holds only what is transient:
 * asking now, waiting for the network, the day not over yet. OFFLINE WAITS: the action says so and asks again by
 * itself the moment the device says it is back online.
 */
import { useEffect, useState } from "react";
import { DAY_FLOW_WORDS, dayFlowAskable, dayFlowLine, dayFlowStation, readingIsTheDays, type DayFlowAnswer,
         type Spot } from "@app/data/spots";
import type { Connectivity } from "./imagery";

export interface DayFlowView {
  heading: string;
  /** The day's flow said whole, or why there is none; null while it has never been asked for. */
  line: string | null;
  /** The action, or null where there is nothing to ask (settled, or filled). */
  action: { label: string; busy: boolean } | null;
  /** What is happening now: asking, offline (waiting), the day not over. Null when nothing is. */
  status: string | null;
}

type Phase = "idle" | "asking" | "offline" | "pending";

const STATUS: Readonly<Record<Phase, string | null>> =
  { idle: null, asking: DAY_FLOW_WORDS.asking, offline: DAY_FLOW_WORDS.offline, pending: DAY_FLOW_WORDS.pending };

/** The view, pure: null when the block has nothing to say (no station, or the reading is the day's own). */
export function dayFlowView(spot: Spot, phase: Phase): DayFlowView | null {
  if (!dayFlowStation(spot) || readingIsTheDays(spot)) return null;
  const askable = dayFlowAskable(spot);
  return {
    heading: DAY_FLOW_WORDS.heading,
    line: dayFlowLine(spot.dayFlow),
    action: askable ? { label: DAY_FLOW_WORDS.action, busy: phase === "asking" } : null,
    status: STATUS[phase],
  };
}

/**
 * The block for one open spot, with its action. `ask` fills and saves the spot, resolving to what it learned (null:
 * nothing was asked). Absent `ask`: no action, the record only.
 */
export function useDayFlow(spot: Spot | null, ask?: (spot: Spot) => Promise<DayFlowAnswer | null>,
                           connectivity?: Connectivity): { view: DayFlowView | null; onAsk: (() => void) | null } {
  const [phase, setPhase] = useState<Phase>("idle");
  const id = spot?.id ?? null;
  useEffect(() => { setPhase("idle"); }, [id]);
  const go = async (s: Spot) => {
    if (!ask) return;
    if (connectivity?.watches && !connectivity.online()) { setPhase("offline"); return; }
    setPhase("asking");
    try {
      const got = await ask(s);
      setPhase(got?.kind === "offline" ? "offline" : got?.kind === "pending" ? "pending" : "idle");
    } catch {
      setPhase("offline");
    }
  };
  // waiting offline: ask again the moment the device is back
  useEffect(() => {
    if (!spot || phase !== "offline" || !connectivity?.watches) return;
    return connectivity.subscribe((up) => { if (up) void go(spot); });
  }, [id, phase, connectivity]);
  const view = spot ? dayFlowView(spot, phase) : null;
  return { view, onAsk: spot && ask && view?.action ? () => void go(spot) : null };
}
