/**
 * (T199, T261) THE RUNS TAB'S DATA: the run-timing feed read the first time the reader opens Runs (never up front: it
 * is its own stream, not the release), held for the session, and handed to the map only on a proven pair whose tiles
 * are the atlas the paint index was bound to.
 *
 * (T261, owner 2026-10-29) Its inputs are its own, not the Map tab's: the SPECIES chosen (several at once, steelhead by
 * default, kept between visits in `store`) and the PENTAD the date slider holds (today's to start; a play button walks
 * the year). The map is repainted from a THROTTLED pentad, so a fast scrub relabels the tiles a few times a second, not
 * once per pixel; the slider and the sheet read the pentad as it moves.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { pentadOf, restoreRunSpecies, runSpecies, saveRunSpecies, slotsFor, type RunSpecies,
         type RunTimingFacts, bcNow, now as clockNow } from "@app/core";
import type { RunTimingSource } from "@app/data";
import { useAsync } from "./async";

/** What the map is handed (`MapProps.runs`): the facts, the slots painted and the pentad (0..72). */
export interface RunsFeedVM {
  readonly facts: RunTimingFacts;
  readonly slots: readonly number[];
  readonly pentad: number;
}

/** Where the species chosen are kept: whole text, by one name (`@app/data/spots` `openRunSpeciesStore`). */
export interface RunSpeciesStore { read(): string | null; write(text: string): void }

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
  /** the species chosen, in the row's order; never empty once the feed is in */
  readonly chosen: readonly RunSpecies[];
  /** add or remove a species (the last one stays: a map of no species answers nothing) */
  readonly toggle: (name: string) => void;
  /** (T367) that species alone; every species again */
  readonly only: (name: string) => void;
  readonly all: () => void;
  /** the pentad the slider holds (0..72), today's, and the year it is read in (pink's brood line) */
  readonly pentad: number;
  readonly today: number;
  readonly year: number;
  readonly setPentad: (p: number) => void;
  readonly playing: boolean;
  readonly setPlaying: (on: boolean) => void;
}

/** How often a moving slider repaints the map (ms), and how long one step of Play holds. */
export const RUNS_PAINT_MS = 140;
export const RUNS_PLAY_MS = 650;

/**
 * `wanted`: the reader is on the Runs tab (the first time asks the feed; later it is kept). `tiles` the drawn tiles'
 * version ("<handles>.<build>"), `paired` the release / tiles pair proven. `store` keeps the species chosen.
 */
export function useRunTiming(i: { source: RunTimingSource | null | undefined; wanted: boolean; paired: boolean;
                                  tiles: string | null; store?: RunSpeciesStore | null; now?: Date;
                                  /** (T620 G10) the day the map is on (null: today). Runs OPENS on its pentad and follows it when it moves. */
                                  mapDay?: Date | null }): RunTimingVM {
  const [asked, setAsked] = useState(false);
  if (i.wanted && i.source && !asked) setAsked(true);
  const got = useAsync(() => i.source!.latest(), "runtiming", asked && !!i.source);
  const data = got.state === "ready" ? got.value : null;
  const facts = data?.facts ?? null;
  const species = useMemo(() => (facts ? runSpecies(facts) : []), [facts]);
  // the reader's choice this session, else what the store kept (read once), else the default
  const [names, setNames] = useState<readonly string[] | null>(null);
  const kept = useMemo(() => {
    try { return i.store?.read() ?? null; } catch { return null; }
  }, [i.store]);
  const chosenNames = useMemo(() => restoreRunSpecies(names ? saveRunSpecies(names) : kept, species),
                              [names, kept, species]);
  const chosen = useMemo(() => species.filter((s) => chosenNames.includes(s.name)), [species, chosenNames]);
  const toggle = useCallback((name: string) => {
    const now = chosenNames.includes(name) ? chosenNames.filter((n) => n !== name) : [...chosenNames, name];
    if (now.length === 0) return;
    setNames(now);
    try { i.store?.write(saveRunSpecies(now)); } catch { /* a preference: losing it costs a tap */ }
  }, [chosenNames, i.store]);
  // (T367, owner 2026-11-04: "select multiple and be able to reduce quickly") one species alone, or every one again
  const choose = useCallback((now: readonly string[]) => {
    if (now.length === 0) return;
    setNames([...now]);
    try { i.store?.write(saveRunSpecies(now)); } catch { /* a preference: losing it costs a tap */ }
  }, [i.store]);
  const only = useCallback((name: string) => choose([name]), [choose]);
  const all = useCallback(() => choose(species.map((s) => s.name)), [choose, species]);

  const day = i.now ?? bcNow();
  const today = pentadOf(day);
  const year = day.getFullYear();
  const mapPentad = i.mapDay ? pentadOf(i.mapDay) : today;
  const [pentad, setPentadRaw] = useState(mapPentad);
  const [playing, setPlaying] = useState(false);
  // the map's day moving (the date pill, a start link) moves the slider with it; scrubbing the slider is Runs' own
  const lastMap = useRef(mapPentad);
  useEffect(() => {
    if (lastMap.current === mapPentad) return;
    lastMap.current = mapPentad;
    setPlaying(false);
    setPentadRaw(mapPentad);
  }, [mapPentad]);
  const setPentad = useCallback((p: number) => {
    setPlaying(false);
    setPentadRaw(Math.max(0, Math.min(72, Math.round(p))));
  }, []);
  // PLAY: a pentad a step, round the year, until the reader touches the slider or presses Pause
  useEffect(() => {
    if (!playing) return;
    const t = setInterval(() => setPentadRaw((p) => (p + 1) % 73), RUNS_PLAY_MS);
    return () => clearInterval(t);
  }, [playing]);
  const painted = useThrottled(pentad, RUNS_PAINT_MS);

  const handles = i.tiles ? i.tiles.split(".")[0]! : null;
  const ours = facts !== null && i.paired && handles === facts.handles;
  const slots = useMemo(() => (facts ? slotsFor(facts, chosen, year) : []), [facts, chosen, year]);
  const slotsKey = slots.join(",");
  const feed = useMemo(() => (ours && facts && slots.length ? { facts, slots, pentad: painted } : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the slots' identity is their content
    [ours, facts, slotsKey, painted]);
  const loading = asked && got.state === "loading";
  const problem = got.state === "failed" || (got.state === "ready" && !facts) ? "failed"
    : facts && i.paired && handles !== null && handles !== facts.handles ? "stale" : null;
  return { feed, facts, loading, problem, captured: data?.from === "capture", species, chosen, toggle, only, all,
           pentad, today, year, setPentad, playing, setPlaying };
}

/** The value, but changed at most every `ms` while it keeps moving — the last change always lands. */
function useThrottled<T>(v: T, ms: number): T {
  const [out, setOut] = useState(v);
  const last = useRef(0);
  useEffect(() => {
    const wait = last.current + ms - clockNow();
    if (wait <= 0) { last.current = clockNow(); setOut(v); return; }
    const t = setTimeout(() => { last.current = clockNow(); setOut(v); }, wait);
    return () => clearTimeout(t);
  }, [v, ms]);
  return out;
}
