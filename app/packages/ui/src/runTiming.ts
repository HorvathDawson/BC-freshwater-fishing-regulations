/**
 * (T199) THE RUN TIMING MODE'S DATA: the run-timing feed read the first time the reader chooses Run timing (never up
 * front: it is its own stream, not the release), held for the session, and handed to the map only on a proven pair
 * whose tiles are the atlas the paint index was bound to. The species row's choice and the day are the map's inputs;
 * the day is the map's own day (the corner's day strip), its month the paint.
 */
import { useMemo, useState } from "react";
import { runSpecies, slotFor, type RunSpecies, type RunTimingFacts } from "@app/core";
import type { RunTimingSource } from "@app/data";
import { useAsync } from "./async";

/** What the map is handed (`MapProps.runs`): the facts, the slot painted and the month (0 = January). */
export interface RunsFeedVM {
  readonly facts: RunTimingFacts;
  readonly slot: number;
  readonly month: number;
}

export interface RunTimingVM {
  /** the map's input, null until loaded and while the atlas is not the one it was bound to */
  readonly feed: RunsFeedVM | null;
  readonly facts: RunTimingFacts | null;
  readonly loading: boolean;
  /** "stale": loaded, but for another atlas (nothing is painted, and it is said); "failed": nothing could be read */
  readonly problem: "stale" | "failed" | null;
  /** from the last capture (offline), not fetched now */
  readonly captured: boolean;
  readonly species: readonly RunSpecies[];
  readonly chosen: RunSpecies | null;
  readonly choose: (name: string) => void;
}

/**
 * `wanted`: the reader has chosen Run timing (the first time asks the feed; later it is kept). `tiles` the drawn tiles'
 * version ("<handles>.<build>"), `paired` the release / tiles pair proven. `day` the map's day (null: today).
 */
export function useRunTiming(i: { source: RunTimingSource | null | undefined; wanted: boolean; paired: boolean;
                                  tiles: string | null; day: Date | null }): RunTimingVM {
  const [asked, setAsked] = useState(false);
  if (i.wanted && i.source && !asked) setAsked(true);
  const got = useAsync(() => i.source!.latest(), "runtiming", asked && !!i.source);
  const data = got.state === "ready" ? got.value : null;
  const facts = data?.facts ?? null;
  const species = useMemo(() => (facts ? runSpecies(facts) : []), [facts]);
  const [name, setName] = useState<string | null>(null);
  const chosen = species.find((s) => s.name === name) ?? species[0] ?? null;
  const handles = i.tiles ? i.tiles.split(".")[0]! : null;
  const ours = facts !== null && i.paired && handles === facts.handles;
  const dayKey = i.day ? `${i.day.getFullYear()}-${i.day.getMonth()}-${i.day.getDate()}` : "today";
  // eslint-disable-next-line react-hooks/exhaustive-deps -- the day's identity is its date, not the Date object
  const day = useMemo(() => i.day ?? new Date(), [dayKey]);
  const slot = facts && chosen ? slotFor(facts, chosen, day.getFullYear()) : -1;
  const month = day.getMonth();
  const feed = useMemo(() => (ours && facts && slot >= 0 ? { facts, slot, month } : null), [ours, facts, slot, month]);
  const loading = asked && got.state === "loading";
  const problem = got.state === "failed" || (got.state === "ready" && !facts) ? "failed"
    : facts && i.paired && handles !== null && handles !== facts.handles ? "stale" : null;
  return { feed, facts, loading, problem, captured: data?.from === "capture", species, chosen,
           choose: setName };
}
