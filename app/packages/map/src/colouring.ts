/**
 * THE MAP'S COLOURING ENGINE — ONE, for every colouring that reads a release's data (colour-latency AP, generalised;
 * owner 2026-10-21: "the painting for regulations is way faster now ... we need to do the same painting speed up for the
 * conditions map too", and "it should be a single pathway ideally").
 *
 * Every data-driven colouring splits the same way: what is FIXED for the release is a LABEL on each section, set the
 * moment its tile arrives, synchronously, before the tile's first frame — no `idle`, no React round trip. A CHANNEL is
 * one such colouring:
 *
 *   status     label `p`: the section's day-PROFILE (@app/core `profileLabel`). What changes — the DAY — lives in the
 *              style (`dayProfiles`, ~115 labels): a day change is one paint property and no per-section call.
 *   standing   label `standing`: the section's value, read off its standing CLASS (@app/data `StandingClasses.classOf`:
 *              a donor panel at one target area, or a lake's own station — fixed for the release) in the class table
 *              (`classStandings`: one value per class for the live feed at the chosen horizon). What changes — the
 *              table, 18.5k classes — does NOT go in the style: MapLibre 5.24 re-lays out every loaded tile on any
 *              data-driven paint change, and with a table that size it cost 300 ms of long tasks per horizon change on
 *              the phone profile (measured, colour-latency README "Conditions"). A new table instead RELABELS the tiles
 *              already loaded (feature-state only: no reload) and is read by every tile that loads after. The basins'
 *              values go on the basins by id (`setChannel`), 190 states.
 *   survey     label `survey`: a lake's survey state (`sheet` / `digitised`), and
 *   stocked    label `stocked`: its `stock_recency` band for today — both from the release's lake facts (@app/core
 *              `LakeFacts`, the file `lakes`), the Depth and Stocked modes' values (owner 2026-10-21: the lake modes go
 *              through the same pathway, and nothing about them is in the tiles). The values ARE the labels: a new
 *              release relabels, a new day relabels the stocked bands; neither touches the style.
 *
 * The engine owns, for every channel alike: labelling a tile (one feature-state call per section, carrying every
 * channel's label it lacks), the de-dupe (a bitset per channel and source-layer), the release-swap / withdraw sequence,
 * putting back what a clear took that was not ours, and the proven-pair gate (no input, no label). Nothing here knows
 * what a status or a standing is — the labels and values are core's and data's; the colours are the style's.
 *
 * Platform-free on purpose: the renderer hands in each tile's ids per source-layer and a target that sets state, so
 * every sequence is tested as plain code (`colouring.test.ts`), not through a browser.
 */
import { dayOfYear, dayProfiles, profileLabel, stockRecency, type LakeFacts, type StatusIndex } from "@app/core";
import type { ChannelValues, MapAdapter, MapHandle } from "./adapters/contract";
import type { StandingValues, Tokens } from "./style";

/** The day's status as the map is told it — given ONLY on a proven pair, with an index that passed its digest checks. */
export interface ProfileStatus {
  readonly index: StatusIndex;
  readonly day: Date;
}

/**
 * The Conditions map's standings as the map is told them — ONLY on a proven pair. `labels` is the release's static
 * half (each section's class, fixed for the release); `values` the changing half (each class's standing for the live
 * feed at the chosen horizon, computed from THESE labels), or null while the feed has not answered.
 */
export interface StandingFeed {
  readonly labels: { classOf(section: number): number | null };
  readonly values: StandingValues | null;
}

/**
 * The lake modes' values as the map is told them — ONLY on a proven pair: the release's lake facts (per lake section,
 * surveyed and last stocked; @app/core `LakeFacts`) and TODAY as a day number (days since 1900-01-01, `dayNumber`), which
 * the stocked bands are counted from.
 */
export interface LakeFeed {
  readonly facts: LakeFacts;
  readonly day: number;
}

/** The colourings that label sections: one per question a release's data answers on the map. */
export type Channel = "status" | "standing" | "stocked" | "survey";

/**
 * What a channel labels a section with. `of` is the labelling's identity (the index, the classes): another object is
 * another labelling, and every label of the old one is wrong, not merely stale. `label` answers `undefined` for a
 * section the channel says nothing about (it gets no key), and may answer `null` to clear a value it had.
 */
export interface Labelling {
  readonly key: string;
  readonly of: object;
  label(section: number): string | number | null | undefined;
}

/**
 * One channel's input: its labelling, and its values (`valuesKey` says when they changed). `relabels`: the values are
 * IN the labels (standing), so new values relabel the loaded tiles; otherwise they are in the style (status).
 */
export interface ChannelInput {
  readonly labels: Labelling;
  readonly valuesKey: unknown;
  readonly relabels?: boolean;
  values(): ChannelValues[Channel];
}

/** The status channel from what the Map tab is told: profile labels, the day in the style. */
export function statusInput(s: ProfileStatus): ChannelInput {
  const index = s.index;
  return {
    labels: { key: "p", of: index, label: (id) => profileLabel(index, id) },
    valuesKey: dayOfYear(s.day),
    values: () => dayProfiles(index, s.day),
  };
}

/**
 * The standing channel from what the Conditions tab is told: each section's value through its class, in feature-state
 * `standing` — the key the `standing` mode has always read, so both renderers paint it with one style. A section with
 * no class is never labelled (unmeasured, as before); a class with nothing to say today is `null`, so a value it had
 * is cleared; no values yet labels nothing.
 */
export function standingInput(s: StandingFeed): ChannelInput {
  const classes = s.labels, v = s.values;
  return {
    labels: {
      key: "standing", of: classes,
      label: (id) => {
        if (!v) return undefined;
        const k = classes.classOf(id);
        if (k === null) return undefined;
        const x = v.section(k);
        return Number.isFinite(x) ? x : null;
      },
    },
    valuesKey: v,
    relabels: true,
    values: () => v,
  };
}

/**
 * The Depth mode's channel: each lake's survey state in feature-state `survey` (the key the `surveyed` mode has always
 * read) — `sheet` or `digitised`; a lake never surveyed gets no key (the mode's `missing`, plain water). Fixed for the
 * release: the values ARE the labels, so nothing goes in the style.
 */
export function surveyInput(l: LakeFeed): ChannelInput {
  const f = l.facts;
  return {
    labels: { key: "survey", of: f, label: (id) => f.survey(id) ?? undefined },
    valuesKey: f,
    values: () => f.sections,
  };
}

/**
 * The Stocked mode's channel: each lake's `stock_recency` band in feature-state `stocked` (the key the `stocked` mode has
 * always read), counted from the day. A new DAY relabels the loaded lakes (a band may move on); a lake never stocked
 * gets no key (the mode's `missing`, which is the `never` colour).
 */
export function stockedInput(l: LakeFeed): ChannelInput {
  const f = l.facts, day = l.day;
  return {
    labels: { key: "stocked", of: f, label: (id) => stockRecency(f.stockedDay(id), day) ?? undefined },
    valuesKey: day,
    relabels: true,
    values: () => day,
  };
}

/**
 * Whether the map on this platform (`Platform.OS`) labels each tile as it arrives (`MapProps.status`, `standing`). Only
 * the web map: `Map.native.tsx` (N2) has no per-tile load event and colours only from pushed `data` (`status` mode,
 * `standing` values), so a phone handed the per-tile `profile` mode painted every water plain with no error (colour-ap
 * review). How the
 * native map labels per tile is open (the colour-latency README) — flip this then.
 */
export function coloursEachTile(platform: string): boolean {
  return platform === "web";
}

/**
 * The source-layers whose sections wear labels. Feature-state is per source-layer, so a stream's polygon
 * (`lake_flowing`, `wetland_flowing`) reads the label set on `lake` / `wetland`.
 */
export const LABEL_SOURCE_LAYERS = ["stream", "lake", "wetland"] as const;
export type LabelSourceLayer = typeof LABEL_SOURCE_LAYERS[number];

/** A tile's section ids, by source-layer. */
export type TileSections = Partial<Record<LabelSourceLayer, ArrayLike<number>>>;

/** What the colouring asks of a renderer: per-SOURCE-LAYER state, which is how MapLibre keys it. */
export interface LabelTarget {
  setState(sourceLayer: LabelSourceLayer, id: number, state: Record<string, unknown>): void;
  /** Drop EVERY state on a source-layer — every channel's label, `selected`, any pushed data, all of it. */
  clearAll(sourceLayer: LabelSourceLayer): void;
}

/** What the renderer gives back after a clear: the tiles it holds, and the non-colour state to put back. */
export interface TileRenderer {
  /** Every tile the map holds now — in view and cached — so a new labelling colours what is already loaded. */
  loaded(): Iterable<TileSections>;
  /** Re-apply what a clear took that was not ours: the selected highlight, and the active mode's pushed data. */
  restore(): void;
}

/**
 * Which ids already wear a channel's current label, as one bit per handle and source-layer — about 245 KB per layer
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

interface Installed {
  labels: Labelling;
  valuesKey: unknown;
  seen: Map<LabelSourceLayer, Seen>;
  /** Any of this channel's labels set since the last clear — only then does withdrawing it need a clear. */
  dirty: boolean;
}

export class Colouring {
  /** The installed channels, in a fixed order so a tile's state object is built the same way every time. */
  private channels = new Map<Channel, Installed>();
  /** Feature-state calls made, for the record (the study's "work per pan"). */
  calls = 0;

  constructor(private readonly adapter: MapAdapter) {}

  /** True while any channel is installed: only then does a tile need labelling. */
  get active(): boolean { return this.channels.size > 0; }

  /**
   * Label every section of a tile with every installed channel's label it does not wear yet — ONE state call per
   * section, whatever the number of channels. Synchronous, so a tile labelled in its own load event draws its first
   * frame in colour. A section a channel has no label for (no class: nothing speaks for it) is left without that key,
   * which the style draws as it always drew such water. Returns how many states were set.
   */
  label(t: LabelTarget, tile: TileSections): number {
    if (!this.channels.size) return 0;
    const chans = [...this.channels.values()];
    let n = 0;
    for (const layer of LABEL_SOURCE_LAYERS) {
      const ids = tile[layer];
      if (!ids) continue;
      const seen = chans.map((c) => {
        let s = c.seen.get(layer);
        if (!s) c.seen.set(layer, s = new Seen());
        return s;
      });
      for (let i = 0; i < ids.length; i++) {
        const id = ids[i]!;
        if (!Number.isInteger(id) || id < 0) continue;
        let state: Record<string, unknown> | null = null;
        for (let k = 0; k < chans.length; k++) {
          if (seen[k]!.has(id)) continue;
          seen[k]!.add(id);
          const v = chans[k]!.labels.label(id);
          if (v === undefined) continue;
          (state ??= {})[chans[k]!.labels.key] = v;
          chans[k]!.dirty = true;
        }
        if (state === null) continue;
        t.setState(layer, id, state);
        n++;
      }
    }
    this.calls += n;
    return n;
  }

  /**
   * What one channel is told, every time it may have changed.
   *
   * SAME LABELLING, OTHER VALUES. Status (a new day): ONE repaint of the day (`setChannel`), no feature-state call.
   * Standing (a feed refresh, a horizon or quantity change): the tiles already loaded are RELABELLED with the new
   * values — feature-state only, no clear and no tile reload — and every tile that loads after is labelled from them;
   * then the basins' values (`setChannel`). The same values again: nothing.
   *
   * ANOTHER LABELLING, OR NONE (a release swap, P11; the pair no longer proven; the data withdrawn): a label means
   * something only within one labelling, so every label of this channel on the map is now a WRONG colour, not a grey
   * one — the worse failure. In one synchronous step, so no frame sees a half-done state:
   *   1. drop this channel's colouring (its modes paint plain, or `missing` where that is the honest answer);
   *   2. if this channel has labels on the map: clear every feature-state on stream, lake and wetland — which takes
   *      every other channel's labels, the `selected` highlight and any pushed data with it, so
   *   3. the renderer puts the highlight and the data back (`restore`), and
   *   4. every channel forgets which ids wear its label (the clear took them all);
   *   5. only then: label every tile already loaded, for every channel now installed, and install the new values.
   * A channel installed for the FIRST time has nothing to clear: the loaded tiles take its label beside the others'.
   */
  set(h: MapHandle, t: LabelTarget, r: TileRenderer, channel: Channel, input: ChannelInput | null,
      themeName: string, overrides: Tokens = {}): void {
    const cur = this.channels.get(channel);
    if (input && cur && input.labels.of === cur.labels.of && input.labels.key === cur.labels.key) {
      if (input.valuesKey === cur.valuesKey) return;
      cur.valuesKey = input.valuesKey;
      if (input.relabels) {
        cur.labels = input.labels;
        cur.seen = new Map();
        for (const tile of r.loaded()) this.label(t, tile);
      }
      this.adapter.setChannel(h, channel, input.values(), themeName, overrides);
      return;
    }
    if (!input && !cur) return;
    this.adapter.setChannel(h, channel, null, themeName, overrides);
    if (cur?.dirty) {
      for (const layer of LABEL_SOURCE_LAYERS) t.clearAll(layer);
      r.restore();
      for (const c of this.channels.values()) { c.seen = new Map(); c.dirty = false; }
    }
    this.channels.delete(channel);
    if (input)
      this.channels.set(channel, { labels: input.labels, valuesKey: input.valuesKey, seen: new Map(), dirty: false });
    if (this.channels.size) for (const tile of r.loaded()) this.label(t, tile);
    if (input) this.adapter.setChannel(h, channel, input.values(), themeName, overrides);
  }
}

/**
 * THE KEYS ONLY THE ENGINE WRITES on a section on a map that labels its own tiles: the channels' labels (the
 * day-profile `p`, the `standing` value) and `status`, which only a pushed-data map (the phone) is handed in their
 * place. Pushed through `data` to such a map they would be a second pathway beside the engine (owner 2026-10-21: "it
 * should be a single pathway"): `Map.web.tsx` refuses them, and reports it (tools/one-colouring-engine.test.ts).
 */
export const ENGINE_KEYS: readonly string[] = ["p", "standing", "status"];

type Pushed = Record<string, Record<string, Record<string, unknown>>>;

/** `data` without any value an engine channel owns; a layer that carried one is reported once. */
export function withoutEngineKeys(data: Pushed, report: (e: Error) => void): Pushed {
  const out: Pushed = {};
  for (const [layerId, values] of Object.entries(data)) {
    let refused = false;
    const kept: Pushed[string] = {};
    for (const [id, state] of Object.entries(values)) {
      const rest = Object.fromEntries(Object.entries(state).filter(([k]) => !ENGINE_KEYS.includes(k)));
      if (Object.keys(rest).length !== Object.keys(state).length) refused = true;
      if (Object.keys(rest).length) kept[id] = rest;
    }
    if (refused) report(new Error(`pushed ${ENGINE_KEYS.join("/")} on "${layerId}" refused: the web map colours `
                                  + "sections through the colouring engine only (MapProps.status, MapProps.standing)"));
    if (Object.keys(kept).length) out[layerId] = kept;
  }
  return out;
}
