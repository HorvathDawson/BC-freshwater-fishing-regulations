/**
 * THE PHONE'S COLOURING ENGINE (T74) — the same `Colouring` the web map runs (`colouring.ts`), fed the same way: each
 * atlas tile's sections are labelled the moment the tile is reported, with every channel's label (status, standing,
 * the lake modes, salmon, method). What differs is only the platform's plumbing, all of it here:
 *
 *   the tile     the patched binding's `MLRNTileIds` device event (deps.md): MapLibre Native's tile REQUEST, with the
 *                tile's ids read from the same local archive — so the labels usually land before the tile is parsed.
 *                The tiles reported are kept (`TileHold`) for what a new labelling or a style swap must relabel.
 *   the states   batched per source-layer into ONE `setFeatureStates` bridge call per burst, not one per section;
 *                nothing is sent before the map has drawn once (MapLibre Native dereferences a renderer that does not
 *                exist yet: N2's SIGSEGV), and what was asked for before is sent then.
 *   the values   a channel's values that live in the STYLE (the day's profiles, the salmon day, the basins' standings)
 *                come back as `values`, which the map writes into the style it hands the binding (`nativeStyle`) — a
 *                style swap, as every look change is on a phone, and a swap drops every feature-state: `swapped()`
 *                makes the engine forget what it labelled and labels the tiles it holds again.
 */
import { useCallback, useEffect, useMemo, useRef } from "react";
import { DeviceEventEmitter } from "react-native";
import { Colouring, LABEL_SOURCE_LAYERS, methodInput, runsInput, runsKeyOf, salmonInput, standingInput, statusInput, stockedInput, surveyInput,
         type Channel, type ChannelInput, type LabelTarget, type TileRenderer } from "../colouring";
import type { MapAdapter } from "../adapters/contract";
import type { MapProps } from "../map-props";
import type { ChannelValues } from "../style";
import { TILE_IDS_EVENT, TileHold, type TileIdsEvent } from "../tile-hold";
import type { CopyTarget, WaterCopy } from "../water-copy";
import { readingDayOf, salmonDayIndex } from "@app/core";
import { panProbe } from "./panProbe";

/** What the engine asks of the patched atlas `VectorSource` (`VectorSourceRef`, deps.md). */
export interface AtlasSource {
  setFeatureStates(sourceLayer: string, ids: string[], states: Record<string, unknown>[]): Promise<void>;
  removeFeatureState(f: { id?: string | number; sourceLayer: string }, key?: string): Promise<void>;
  watchTiles(sourceLayers: string[]): Promise<void>;
  nativeTag(): number;
}

type Channels = Pick<MapProps, "status" | "standing" | "lakes" | "salmon" | "method" | "runs">;

/**
 * (T369 D14) THE PHONE'S FEATURE-STATE QUEUE, per source-layer, in the order asked. A layer CLEARED is not sent as
 * `removeFeatureState({sourceLayer})`: MapLibre Native applies a frame's removals AFTER its sets, so a clear followed by
 * the relabelling in the same frame wiped the new labels (the highlight's own bug, fixed by 8a5b5bd the same way). It
 * is queued as a NULL write of every key the queue has set on that layer (a null state is unset to every paint), in
 * order with the sets around it — one `setFeatureStates` call per layer per burst.
 */
export class StateQueue {
  private q = new Map<string, { ids: string[]; states: Record<string, unknown>[] }>();
  /** what each layer wears, by this queue: the ids and every key ever set there */
  private worn = new Map<string, { ids: Set<string>; keys: Set<string> }>();
  get size(): number { return this.q.size; }
  set(layer: string, id: string, s: Record<string, unknown>): void {
    let b = this.q.get(layer);
    if (!b) this.q.set(layer, b = { ids: [], states: [] });
    b.ids.push(id);
    b.states.push(s);
    let w = this.worn.get(layer);
    if (!w) this.worn.set(layer, w = { ids: new Set(), keys: new Set() });
    w.ids.add(id);
    for (const k of Object.keys(s)) w.keys.add(k);
  }
  /** Every state on `layer` unset: what is queued for it is dropped, and every id it wears gets its keys written null. */
  clear(layer: string): void {
    this.q.delete(layer);
    const w = this.worn.get(layer);
    if (!w || !w.ids.size) return;
    const nulls = Object.fromEntries([...w.keys].map((k) => [k, null]));
    this.q.set(layer, { ids: [...w.ids], states: [...w.ids].map(() => nulls) });
    this.worn.set(layer, { ids: new Set(), keys: new Set() });
  }
  /** A style swap dropped every state: nothing is worn any more. */
  forget(): void { this.worn.clear(); }
  /** The batches to send now, one per layer, and the queue emptied. */
  drain(): [string, string[], Record<string, unknown>[]][] {
    const out = [...this.q].map(([l, b]) => [l, b.ids, b.states] as [string, string[], Record<string, unknown>[]]);
    this.q.clear();
    return out;
  }
  dispose(): void { this.q.clear(); this.worn.clear(); }
}

export interface NativeColouring {
  /** The channels' style-borne values, for `nativeStyle`. */
  values: ChannelValues;
  /** The atlas source attached (or re-attached): ask it for its tiles. */
  attached(): void;
  /** A style finished loading: every feature-state is gone. */
  swapped(): void;
  /** The probe's sample (development measurement, `EXPO_PUBLIC_COLOUR_PROBE`): is this section's colour on the map? */
  coloured(id: number): boolean;
}

export function useNativeColouring(o: {
  atlas: () => AtlasSource | null;
  copy: () => WaterCopy;
  toCopy: CopyTarget;
  /** Put back what a clear took that was not the engine's (the highlight, pushed data): resend them. */
  restore: () => void;
  /** The map has drawn once: feature-state may be sent. */
  ready: boolean;
  theme: string;
  atlasUrl: string;
  report: (e: unknown) => void;
} & Channels): NativeColouring {
  panProbe.render();                   // (T273) this hook runs on every render of the native map
  // the engine's adapter on a phone: its one call (`setChannel`) writes nothing — the style's values are read from the
  // same inputs in the render that hands them over (`values`, T451)
  const adapter = useMemo(() => ({ setChannel: () => {} }) as unknown as MapAdapter, []);
  const engine = useRef<Colouring | null>(null);
  engine.current ??= new Colouring(adapter);
  const hold = useRef(new TileHold());
  const tag = useRef<number | null>(null);
  const ready = useRef(o.ready);
  ready.current = o.ready;
  /** Unmounted: a batch still queued goes nowhere (its source's view is gone). */
  const alive = useRef(true);
  useEffect(() => { alive.current = true; return () => { alive.current = false; queue.current.dispose(); }; }, []);
  const ref = useRef(o);
  ref.current = o;

  /** Each section whose state the map has acknowledged, one bit per id (the probe's ground truth). */
  const acked = useRef(new Uint8Array(0));
  const ack = (id: number) => {
    if ((id >> 3) >= acked.current.length) {
      const g = new Uint8Array(Math.max((id >> 3) + 1, acked.current.length * 2, 1024));
      g.set(acked.current);
      acked.current = g;
    }
    acked.current[id >> 3]! |= 1 << (id & 7);
  };

  // THE BATCH: per source-layer, sent in one call per burst once the map has drawn
  const queue = useRef(new StateQueue());
  /** (T273) labels queued so far (the pan probe's count per tile) */
  const labelled = useRef(0);
  const queued = useRef(false);
  /** (T451, the pan probe's build) a style load's relabel not yet acknowledged; a section acknowledged before it */
  const swapPending = useRef(false);
  const lastAck = useRef<{ layer: string; id: string } | null>(null);
  const flush = useCallback(() => {
    queued.current = false;
    const src = ref.current.atlas();
    if (!src || !ready.current || !alive.current) return;
    for (const [layer, ids, states] of queue.current.drain()) {
      const sent = panProbe.send();
      void src.setFeatureStates(layer, ids, states)
        .then(() => {
          sent();
          for (const id of ids) ack(Number(id));
          if (panProbe.on() && ids.length) {
            lastAck.current = { layer, id: ids[0]! };
            if (swapPending.current) { swapPending.current = false; panProbe.acked(); }
          }
        })
        .catch(ref.current.report);
    }
  }, []);
  const schedule = useCallback(() => {
    if (queued.current) return;
    queued.current = true;
    setTimeout(flush, 0);
  }, [flush]);
  useEffect(() => { if (o.ready && queue.current.size) schedule(); }, [o.ready, schedule]);

  const target = useMemo<LabelTarget>(() => ({
    setState: (layer, id, s) => {
      queue.current.set(layer, String(id), s);
      labelled.current += 1;
      schedule();
      // (T175) a label for one of the water copy's sections is the copy's too — the one pathway
      const fresh = ref.current.copy().fresh(id, s);
      if (fresh) ref.current.toCopy.set(id, fresh);
    },
    clearAll: (layer) => {
      // (T369 D14) null writes in the queue's order, never a whole-layer removal applied after the frame's sets
      queue.current.clear(layer);
      acked.current = new Uint8Array(0);
      schedule();
      const copy = ref.current.copy();
      for (const id of copy.ids)
        if ((copy.layerOf(id) ?? layer) === layer && copy.drop(id)) ref.current.toCopy.remove(id);
    },
  }), [schedule]);
  const renderer = useMemo<TileRenderer>(() => ({
    loaded: () => hold.current.all(),
    restore: () => ref.current.restore(),
  }), []);

  /** Ask the binding for every tile's ids again (it forgets what it sent): the attach, a new archive, a lost tile. */
  const resyncing = useRef(false);
  const rewatch = useCallback(() => {
    resyncing.current = true;
    const src = ref.current.atlas();
    if (!src) return;
    try { tag.current = src.nativeTag(); } catch { return; }
    if ((globalThis as { __tbNoTileEvents?: boolean }).__tbNoTileEvents) return;   // (T420) the experiment build only
    void src.watchTiles([...LABEL_SOURCE_LAYERS]).catch(ref.current.report);
  }, []);

  // THE TILE EVENT: label the tile at once (the engine skips every id that already wears its labels)
  useEffect(() => {
    const sub = DeviceEventEmitter.addListener(TILE_IDS_EVENT, (e: TileIdsEvent) => {
      if (tag.current === null) { try { tag.current = ref.current.atlas()?.nativeTag() ?? null; } catch { /* not mounted */ } }
      if (tag.current === null || e.tag !== tag.current) return;
      if (!e.layers) {
        // (T420) a repeat: its ids came once already and were labelled then
        panProbe.repeat();
        if (hold.current.touch(e)) return;
        // just after the hold was emptied (a new archive, a re-watch) a repeat already on its way is expected to miss;
        // the binding forgets what it sent with the same step, so the tile comes whole on its next request
        if (resyncing.current) return;
        ref.current.report(new Error(`MLRNTileIds: repeat of tile ${e.z}/${e.x}/${e.y} not held (${hold.current.size} held); re-watching`));
        rewatch();
        return;
      }
      resyncing.current = false;
      const t = hold.current.put(e as TileIdsEvent & { layers: Record<string, readonly number[]> });
      const c = engine.current!;
      const before = labelled.current;
      if (c.active) c.label(target, t);
      panProbe.tile(labelled.current - before);
    });
    return () => sub.remove();
  }, [target, rewatch]);
  // another archive is other tiles: what was held means nothing for it, and the binding must send them all again
  const firstUrl = useRef(true);
  useEffect(() => {
    hold.current.clear();
    resyncing.current = true;
    if (firstUrl.current) { firstUrl.current = false; return; }
    rewatch();
  }, [o.atlasUrl, rewatch]);

  const attached = rewatch;
  const swapped = useCallback(() => {
    const c = engine.current!;
    if (panProbe.on()) {
      // (T451) does a section labelled before the load still wear its state? (asked before the relabel is sent)
      swapPending.current = true;
      const was = lastAck.current, src = ref.current.atlas() as unknown as
        { getFeatureState?: (f: { id: string; sourceLayer: string }) => Promise<Record<string, unknown>> } | null;
      if (was && src?.getFeatureState)
        void src.getFeatureState({ id: was.id, sourceLayer: was.layer })
          .then((st) => panProbe.kept(Object.keys(st ?? {}).length > 0), () => panProbe.kept(null));
    }
    c.reset();
    queue.current.forget();
    acked.current = new Uint8Array(0);
    if (c.active) for (const t of hold.current.all()) c.label(target, t);
  }, [target]);

  /*
   * (T451) EACH CHANNEL'S INPUT, made once per change and handed to the engine (the web's `useChannels`, the same calls)
   * — and its STYLE-BORNE VALUES read from the same input IN THIS RENDER (`values` below). They were the engine's
   * `setChannel` through state, one render after the tab's modes: a tab switch handed the binding the new modes with the
   * old (or no) day profiles first and the values a render later — two style swaps, the first drawing the streams
   * without their colours (the flicker going to the Map tab).
   */
  const { status, standing, lakes, salmon, method, runs } = o;
  const statusDay = status ? readingDayOf(status.day) : 0;
  const statusIn = useMemo(() => (status ? statusInput(status) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [status?.index, statusDay, status?.only, o.atlasUrl]);
  const standingIn = useMemo(() => (standing ? standingInput(standing) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [standing?.labels, standing?.values, o.atlasUrl]);
  const surveyIn = useMemo(() => (lakes ? surveyInput(lakes) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [lakes?.facts, o.atlasUrl]);
  const stockedIn = useMemo(() => (lakes ? stockedInput(lakes) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [lakes?.facts, lakes?.day, o.atlasUrl]);
  const salmonDay = salmon ? salmonDayIndex(salmon.day) : -1;
  const salmonIn = useMemo(() => (salmon ? salmonInput(salmon) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [salmon?.facts, salmonDay, salmon?.only, o.atlasUrl]);
  const methodDay = method ? readingDayOf(method.day) : -1;
  const methodIn = useMemo(() => (method ? methodInput(method) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [method?.facts, methodDay, o.atlasUrl]);
  // (T199) the run timing: the state IS the label, a species or month change relabels
  const runsKey = runsKeyOf(runs);
  const runsIn = useMemo(() => (runs ? runsInput(runs) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [runs?.facts, runsKey, o.atlasUrl]);
  const toChannel = (channel: Channel, input: ChannelInput | null) =>
    engine.current!.set({} as never, target, renderer, channel, input, ref.current.theme);
  useEffect(() => toChannel("status", statusIn), [statusIn]);        // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => toChannel("standing", standingIn), [standingIn]);  // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => toChannel("survey", surveyIn), [surveyIn]);        // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => toChannel("stocked", stockedIn), [stockedIn]);     // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => toChannel("salmon", salmonIn), [salmonIn]);        // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => toChannel("method", methodIn), [methodIn]);        // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => toChannel("runs", runsIn), [runsIn]);              // eslint-disable-line react-hooks/exhaustive-deps
  /** What the engine's `setChannel` would write for each channel (`Colouring.set`: `input.values()`, or null). */
  const values = useMemo<ChannelValues>(() => ({
    status: (statusIn?.values() ?? null) as ChannelValues["status"],
    standing: (standingIn?.values() ?? null) as ChannelValues["standing"],
    survey: (surveyIn?.values() ?? null) as ChannelValues["survey"],
    stocked: (stockedIn?.values() ?? null) as ChannelValues["stocked"],
    salmon: (salmonIn?.values() ?? null) as ChannelValues["salmon"],
    method: (methodIn?.values() ?? null) as ChannelValues["method"],
    runs: (runsIn?.values() ?? null) as ChannelValues["runs"],
  }), [statusIn, standingIn, surveyIn, stockedIn, salmonIn, methodIn, runsIn]);

  const coloured = useCallback((id: number) =>
    (id >> 3) < acked.current.length && (acked.current[id >> 3]! & (1 << (id & 7))) !== 0, []);
  return { values, attached, swapped, coloured };
}
