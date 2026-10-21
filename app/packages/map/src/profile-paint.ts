/**
 * THE MAP'S STATUS, COLOURED AS EACH TILE ARRIVES (colour-latency study, recommendation AP; owner 2026-10-20: "when we
 * pan it takes a second to color from default").
 *
 * The grey after a pan was never the GPU. The status went into feature-state only after MapLibre's `idle` — every tile
 * loaded and the 300 ms symbol fade over — and a React round trip, so a new tile drew grey for about half a second.
 * Here the renderer labels each section with its PROFILE (@app/core `profileLabel`) the moment its tile arrives,
 * synchronously, before the tile's first frame; the DAY is in the style (`setStatusDay` with @app/core `dayProfiles`).
 * A section's label never changes within one index, so it is set once per session, and a day change is one paint
 * property with no per-section call.
 *
 * Platform-free on purpose: the renderer hands in each tile's ids per source-layer and a target that sets state, so
 * the release-swap sequence below is tested as plain code (`profile-paint.test.ts`), not through a browser.
 */
import { dayOfYear, dayProfiles, profileLabel, type StatusIndex } from "@app/core";
import type { MapAdapter, MapHandle } from "./adapters/contract";
import type { Tokens } from "./style";

/** The day's status as the map is told it — given ONLY on a proven pair, with an index that passed its digest checks. */
export interface ProfileStatus {
  readonly index: StatusIndex;
  readonly day: Date;
}

/**
 * The source-layers whose sections wear a profile. Feature-state is per source-layer, so a stream's polygon
 * (`lake_flowing`, `wetland_flowing`) reads the label set on `lake` / `wetland`.
 */
export const PROFILE_SOURCE_LAYERS = ["stream", "lake", "wetland"] as const;
export type ProfileSourceLayer = typeof PROFILE_SOURCE_LAYERS[number];

/** A tile's section ids, by source-layer. */
export type TileSections = Partial<Record<ProfileSourceLayer, ArrayLike<number>>>;

/** What the colouring asks of a renderer: per-SOURCE-LAYER state, which is how MapLibre keys it. */
export interface ProfileTarget {
  setState(sourceLayer: ProfileSourceLayer, id: number, state: Record<string, unknown>): void;
  /** Drop EVERY state on a source-layer — the label, `selected`, `standing`, all of it. */
  clearAll(sourceLayer: ProfileSourceLayer): void;
}

/** What the renderer gives back after a clear: the tiles it holds, and the non-colour state to put back. */
export interface ProfileRenderer {
  /** Every tile the map holds now — in view and cached — so a new index colours what is already loaded. */
  loaded(): Iterable<TileSections>;
  /** Re-apply what a clear took that was not ours: the selected highlight, and the active mode's pushed data. */
  restore(): void;
}

/**
 * Which ids already wear the current index's label, as one bit per handle and source-layer — about 245 KB per layer
 * at 1.96M handles, where a Set of numbers would be tens of MB (verifier).
 */
class Seen {
  private bits = new Uint8Array(0);
  has(id: number): boolean {
    return (id >> 3) < this.bits.length && (this.bits[id >> 3]! & (1 << (id & 7))) !== 0;
  }
  add(id: number): void {
    if ((id >> 3) >= this.bits.length) {
      const grown = new Uint8Array(Math.max((id >> 3) + 1, this.bits.length * 2, 1024));
      grown.set(this.bits);
      this.bits = grown;
    }
    this.bits[id >> 3]! |= 1 << (id & 7);
  }
}

export class ProfileColouring {
  private index: StatusIndex | null = null;
  private readingDay = 0;
  /** Any label set since the last clear — so a clear happens only when there is something of ours to clear. */
  private dirty = false;
  private seen = new Map<ProfileSourceLayer, Seen>();
  /** Feature-state calls made, for the record (the study's "work per pan"). */
  calls = 0;

  constructor(private readonly adapter: MapAdapter) {}

  /** True while an index is installed: only then does a tile need labelling. */
  get active(): boolean { return this.index !== null; }

  /**
   * Label every section of a tile that does not wear the current index's label yet. Synchronous, so a tile labelled
   * in its own load event draws its first frame in colour. Returns how many states were set.
   */
  label(t: ProfileTarget, tile: TileSections): number {
    const ix = this.index;
    if (!ix) return 0;
    let n = 0;
    for (const layer of PROFILE_SOURCE_LAYERS) {
      const ids = tile[layer];
      if (!ids) continue;
      let seen = this.seen.get(layer);
      if (!seen) this.seen.set(layer, seen = new Seen());
      for (let i = 0; i < ids.length; i++) {
        const id = ids[i]!;
        if (!Number.isInteger(id) || id < 0 || seen.has(id)) continue;
        seen.add(id);
        t.setState(layer, id, { p: profileLabel(ix, id) });
        n++;
      }
    }
    if (n) this.dirty = true;
    this.calls += n;
    return n;
  }

  /**
   * The status the map is told, every time it may have changed.
   *
   * SAME INDEX, OTHER DAY: one repaint of the day (`setStatusDay`) and no feature-state call at all.
   *
   * ANOTHER INDEX, OR NONE (a release swap, P11; the pair no longer proven; the index withdrawn): a profile number
   * means something only within one index, so every label on the map is now a WRONG colour, not a grey one — the worse
   * failure. In one synchronous step, so no frame sees a half-done state:
   *   1. drop the status colouring to plain;
   *   2. clear every feature-state on stream, lake and wetland — which takes the `selected` highlight and any pushed
   *      data with it, so
   *   3. the renderer puts those back (`restore`);
   *   4. forget which ids wear a label;
   *   5. only then, with a new index: label every tile already loaded and install the new day.
   */
  set(h: MapHandle, t: ProfileTarget, r: ProfileRenderer, status: ProfileStatus | null,
      themeName: string, overrides: Tokens = {}): void {
    const next = status?.index ?? null;
    if (next !== null && next === this.index) {
      const d = dayOfYear(status!.day);
      if (d === this.readingDay) return;
      this.readingDay = d;
      this.adapter.setStatusDay(h, dayProfiles(next, status!.day), themeName, overrides);
      return;
    }
    this.adapter.setStatusDay(h, null, themeName, overrides);
    if (this.dirty) {
      for (const layer of PROFILE_SOURCE_LAYERS) t.clearAll(layer);
      r.restore();
      this.dirty = false;
    }
    this.seen = new Map();
    this.index = next;
    this.readingDay = 0;
    if (next === null) return;
    for (const tile of r.loaded()) this.label(t, tile);
    this.readingDay = dayOfYear(status!.day);
    this.adapter.setStatusDay(h, dayProfiles(next, status!.day), themeName, overrides);
  }
}
