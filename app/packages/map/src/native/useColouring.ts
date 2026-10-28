/**
 * THE PHONE'S COLOURING ENGINE (T74) — the same `Colouring` the web map runs (`colouring.ts`), fed the same way: each
 * atlas tile's sections are labelled the moment the tile is reported, with every channel's label (status, standing,
 * the lake modes, salmon). What differs is only the platform's plumbing, all of it here:
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
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { DeviceEventEmitter } from "react-native";
import { Colouring, LABEL_SOURCE_LAYERS, salmonInput, standingInput, statusInput, stockedInput, surveyInput,
         type Channel, type ChannelInput, type LabelSourceLayer, type LabelTarget, type TileRenderer } from "../colouring";
import type { MapAdapter } from "../adapters/contract";
import type { MapProps } from "../map-props";
import { NO_VALUES, type ChannelValues } from "../style";
import { TILE_IDS_EVENT, TileHold, type TileIdsEvent } from "../tile-hold";
import type { CopyTarget, WaterCopy } from "../water-copy";
import { readingDayOf, salmonDayIndex } from "@app/core";

/** What the engine asks of the patched atlas `VectorSource` (`VectorSourceRef`, deps.md). */
export interface AtlasSource {
  setFeatureStates(sourceLayer: string, ids: string[], states: Record<string, unknown>[]): Promise<void>;
  removeFeatureState(f: { id?: string | number; sourceLayer: string }, key?: string): Promise<void>;
  watchTiles(sourceLayers: string[]): Promise<void>;
  nativeTag(): number;
}

type Channels = Pick<MapProps, "status" | "standing" | "lakes" | "salmon">;

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
  const [values, setValues] = useState<ChannelValues>(NO_VALUES);
  // the engine's adapter on a phone: a channel's values go to the style through state (the only call it makes)
  const adapter = useMemo(() => ({
    setChannel: (_h: unknown, ch: Channel, v: ChannelValues[Channel]) =>
      setValues((was) => (was[ch] === v ? was : { ...was, [ch]: v })),
  }) as unknown as MapAdapter, []);
  const engine = useRef<Colouring | null>(null);
  engine.current ??= new Colouring(adapter);
  const hold = useRef(new TileHold());
  const tag = useRef<number | null>(null);
  const ready = useRef(o.ready);
  ready.current = o.ready;
  /** Unmounted: a batch still queued goes nowhere (its source's view is gone). */
  const alive = useRef(true);
  useEffect(() => { alive.current = true; return () => { alive.current = false; queue.current.clear(); }; }, []);
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
  const queue = useRef(new Map<LabelSourceLayer, { ids: string[]; states: Record<string, unknown>[] }>());
  const queued = useRef(false);
  const flush = useCallback(() => {
    queued.current = false;
    const src = ref.current.atlas();
    if (!src || !ready.current || !alive.current) return;
    for (const [layer, b] of queue.current) {
      const ids = b.ids;
      void src.setFeatureStates(layer, ids, b.states)
        .then(() => { for (const id of ids) ack(Number(id)); })
        .catch(ref.current.report);
    }
    queue.current.clear();
  }, []);
  const schedule = useCallback(() => {
    if (queued.current) return;
    queued.current = true;
    setTimeout(flush, 0);
  }, [flush]);
  useEffect(() => { if (o.ready && queue.current.size) schedule(); }, [o.ready, schedule]);

  const target = useMemo<LabelTarget>(() => ({
    setState: (layer, id, s) => {
      let b = queue.current.get(layer);
      if (!b) queue.current.set(layer, b = { ids: [], states: [] });
      b.ids.push(String(id));
      b.states.push(s);
      schedule();
      // (T175) a label for one of the water copy's sections is the copy's too — the one pathway
      const fresh = ref.current.copy().fresh(id, s);
      if (fresh) ref.current.toCopy.set(id, fresh);
    },
    clearAll: (layer) => {
      queue.current.delete(layer);
      acked.current = new Uint8Array(0);
      const src = ref.current.atlas();
      if (src && ready.current) void src.removeFeatureState({ sourceLayer: layer }).catch(ref.current.report);
      const copy = ref.current.copy();
      for (const id of copy.ids)
        if ((copy.layerOf(id) ?? layer) === layer && copy.drop(id)) ref.current.toCopy.remove(id);
    },
  }), [schedule]);
  const renderer = useMemo<TileRenderer>(() => ({
    loaded: () => hold.current.all(),
    restore: () => ref.current.restore(),
  }), []);

  // THE TILE EVENT: label the tile at once (the engine skips every id that already wears its labels)
  useEffect(() => {
    const sub = DeviceEventEmitter.addListener(TILE_IDS_EVENT, (e: TileIdsEvent) => {
      if (tag.current === null) { try { tag.current = ref.current.atlas()?.nativeTag() ?? null; } catch { /* not mounted */ } }
      if (tag.current === null || e.tag !== tag.current) return;
      const t = hold.current.put(e);
      const c = engine.current!;
      if (c.active) c.label(target, t);
    });
    return () => sub.remove();
  }, [target]);
  // another archive is other tiles: what was held means nothing for it
  useEffect(() => { hold.current.clear(); }, [o.atlasUrl]);

  const attached = useCallback(() => {
    const src = ref.current.atlas();
    if (!src) return;
    try { tag.current = src.nativeTag(); } catch { return; }
    void src.watchTiles([...LABEL_SOURCE_LAYERS]).catch(ref.current.report);
  }, []);
  const swapped = useCallback(() => {
    const c = engine.current!;
    c.reset();
    acked.current = new Uint8Array(0);
    if (c.active) for (const t of hold.current.all()) c.label(target, t);
  }, [target]);

  /** One channel's input, to the engine — the web's `useChannels`, the same calls. */
  const toChannel = (channel: Channel, input: ChannelInput | null) =>
    engine.current!.set({} as never, target, renderer, channel, input, ref.current.theme);
  const { status, standing, lakes, salmon } = o;
  const statusDay = status ? readingDayOf(status.day) : 0;
  useEffect(() => toChannel("status", status ? statusInput(status) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [status?.index, statusDay, o.atlasUrl]);
  useEffect(() => toChannel("standing", standing ? standingInput(standing) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [standing?.labels, standing?.values, o.atlasUrl]);
  useEffect(() => toChannel("survey", lakes ? surveyInput(lakes) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [lakes?.facts, o.atlasUrl]);
  useEffect(() => toChannel("stocked", lakes ? stockedInput(lakes) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [lakes?.facts, lakes?.day, o.atlasUrl]);
  const salmonDay = salmon ? salmonDayIndex(salmon.day) : -1;
  useEffect(() => toChannel("salmon", salmon ? salmonInput(salmon) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [salmon?.facts, salmonDay, o.atlasUrl]);

  const coloured = useCallback((id: number) =>
    (id >> 3) < acked.current.length && (acked.current[id >> 3]! & (1 << (id & 7))) !== 0, []);
  return { values, attached, swapped, coloured };
}
