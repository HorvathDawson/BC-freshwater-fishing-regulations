/**
 * THE ONE WAY A WATER OPENS (owner rule 2026-10-21, "one water screen, whichever map you tapped").
 *
 * A tap on the Conditions map, a tap on the Map tab, a tap on a water's own map, and any list that opens a water
 * (search, a spot, a deep path) all end here — `openWater` builds the route action, `openTap` resolves a tapped section
 * to its water (`source.itemForSection`, called nowhere else in the app) and builds it. So the same tap from either map
 * produces the same frame — the same water, part, section and point — and only `view` (which view is in front) says
 * which map it came from. That they could differ is what this file makes impossible; `openWater.test.ts` pins it, and
 * pins that nothing else in the app builds an `openWater` action or resolves a tapped section.
 *
 * AN UNNAMED REACH (no registry item — most streams) opens as a frame with `item: null` offering Regulations and
 * Conditions (T53, owner 2026-10-22: an unnamed stream is fished under its region's base and any override reaching it,
 * which the pipeline answers per section). A Map-tab tap puts Regulations in front, a Conditions-map tap Conditions —
 * the same frame either way; the water Map stays a named water's (asked for it, the frame's first view, Regulations).
 */
import { useMemo } from "react";
import { reportFailure, reported } from "@app/core";
import { openProbe, type ItemId, type WaterSource, type SectionId } from "@app/data";
import type { StreamPlace } from "@app/data/regs/place";
import { frameViews, type LatLon, type Route, type RouteAction, type View } from "./route";

export interface WaterOpen {
  /** The water; null for an unnamed reach (then `tapSid` is required; it offers Regulations and Conditions). */
  item: ItemId | null;
  /** The section the reader tapped, null when not opened by a tap. */
  tapSid?: SectionId | null;
  /** Where on `tapSid` the tap landed. */
  at?: LatLon | null;
  /** The part to open at (a flow into a water names one). */
  part?: number | null;
  /** The view in front: "conditions" from the Conditions map, "regs" from the Map tab and everything else. */
  view?: View;
  /** Opened from a search result: the search is consumed (route `fromSearch`) — back lands on the map. */
  fromSearch?: boolean;
  /** (T203) With view "map": the Map opens framed on the whole water. */
  whole?: boolean;
  /** (T58) The water's split-lake areas, itself among them (`siblingItems`): another area of a lake held is that lake. */
  group?: readonly ItemId[];
}

/**
 * THE ONE BUILDER of an `openWater` route action. The view asked for is put in front when the frame offers it, else the
 * frame's first view (`frameViews`): an unnamed reach asked for the water Map opens on its Regulations.
 */
export function openWater(o: WaterOpen): RouteAction {
  const asked = o.view ?? "regs";
  const views = frameViews({ item: o.item });
  const a: RouteAction = { type: "openWater", item: o.item, tapSid: o.tapSid ?? null, at: o.at ?? null,
                          part: o.part ?? null, view: views.includes(asked) ? asked : views[0]!,
                          ...(o.group ? { group: o.group } : {}) };
  const w = o.whole && a.view === "map" ? { ...a, whole: true } : a;
  return o.fromSearch ? { ...w, fromSearch: true } : w;
}

/**
 * THE WATER A SECTION IS: the one place the app asks (a map tap's section here; T369, a link's PLACE resolved to its
 * section, `place.ts` `resolvePlace` — the same rule, so a link opens what a tap on that point would). Null: an unnamed
 * reach.
 */
export function waterOfSection(source: Pick<WaterSource, "itemForSection">, sid: SectionId): Promise<ItemId | null> {
  return source.itemForSection(sid);
}

/**
 * A map tap -> the route action, or null when nothing opens: the tapped section resolved to its water HERE, once.
 * `except`: the water already on screen (a tap on a water's own map changes nothing).
 */
export async function openTap(source: Pick<WaterSource, "itemForSection">, sid: SectionId, at: LatLon | null,
                              view: View, except: ItemId | null = null,
                              groupOf?: (item: ItemId) => Promise<readonly ItemId[] | undefined>,
                              partOf?: OpenerOptions["partOf"],
                              unnamed?: OpenerOptions["unnamed"]): Promise<RouteAction | null> {
  const item = await waterOfSection(source, sid);
  if (item !== null && item === except) return null;
  // (T737) an unnamed reach: what its page waits on, asked now and together
  if (item === null) unnamed?.(sid);
  const [group, at2] = await Promise.all([
    item !== null && groupOf ? groupOf(item) : undefined,
    // (T369 D3) the tapped water opens AT its part: one frame for it
    // (T368) a failed read opens part-less (each view asks the same `targetFor`), and is reported
    item !== null && partOf ? partOf(item, sid).catch(reported(null)) : null]);
  return openWater({ item, tapSid: sid, at, view, group, ...(at2 ? { part: at2.part } : {}) });
}

export interface WaterOpener {
  /**
   * A tap on a map: the section, where on it, and which view to put in front. (T716) Resolves true when the water was
   * opened, false when nothing opened (the water already on screen, or the open was overtaken or went stale).
   */
  tap(sid: SectionId, at: LatLon | null, view: View, except?: ItemId | null): Promise<boolean>;
  /**
   * (T58) "Flows into …" from a water's map: the receiving water `item`, its action built by `build` (waterMap.ts
   * `exitAction`) once the water's split-lake areas are known.
   */
  flow(item: ItemId, build: (group: readonly ItemId[] | undefined) => RouteAction): Promise<void>;
  /** A water by its id (a spot, a deep path). */
  /** (T1034) `part`: the part a link names; null: the water's own default. (T1038) `whole`: the Map framed on the whole water */
  item(item: ItemId, view?: View, part?: number | null, whole?: boolean): void;
  /**
   * A water picked from search's results: the search is consumed, so back lands on the map (route `fromSearch`). It
   * opens on its Map (T203), framed on its part (T747) — (T925) a water of one part too: the map shows what it covers.
   */
  searched(item: ItemId, at?: StreamPlace | null): Promise<void>;
  /** (T716) Drop any open still being looked up (a tap's lift whose click never came): it dispatches nothing. */
  cancel(): void;
}

export interface OpenerOptions {
  /**
   * (T58) THE ROUTE NOW. An open waits on a lookup (the tapped section's water, a lake's areas); when it finishes after
   * the route has moved — the reader pressed back or ×, picked a tab, opened something else — it is STALE and dropped:
   * a slow tap never opens a water over where the reader has gone since. Of two opens in flight the later wins.
   * Absent: nothing is dropped but an open overtaken by a later one.
   */
  now?: () => Route;
  /**
   * (T58) A water's split-lake areas, itself among them (`@app/data` `siblingItems` over the regs store). Asked only in
   * water mode — the one place a frame is matched against frames held; from browse mode a water opens alone and at once.
   * A read that fails opens the water without them (as a water alone).
   */
  group?: (item: ItemId) => Promise<readonly ItemId[]>;
  /**
   * (T369 D3) The part a tapped section of a water opens at — the Regulations view's own
   * answer (`frameView.ts` `regsPartOf`). Every tapped frame then carries its part from the start, so no view adopts one
   * later and the address names it. A read that fails opens part-less (each view asks the same `targetFor`).
   */
  partOf?: (item: ItemId, sid: SectionId) => Promise<{ part: number } | null>;
  /**
   * (T737, 2026-11-15) A tap on an UNNAMED reach is known: what its page will read is asked now, together
   * (`regs.ts` `warmUnnamedPage`) — a hint, it decides nothing. Absent: nothing asked.
   */
  unnamed?: (sid: SectionId) => void;
  /**
   * (T927) The part of a water holding a PLACE on it (a boundary place picked in search: the FWA stream key and metres,
   * AGENTS 5) — the link's own lookup (`place.ts` `resolvePartPlace`). null: no part of it holds the place (the water
   * opens on its own part). Absent: a place opens the water as a name would.
   */
  placePart?: (item: ItemId, at: StreamPlace) => Promise<number | null>;
  /**
   * (T925, from T737) A water opened by its id (a search result, a spot, a link) is known at once: what its page reads
   * beside its rules (`regs.ts` `warmWaterPage`: geo) is asked now — a hint, it decides nothing. T221's solo question did
   * this for these opens; with it gone, the opener asks it itself. Absent: nothing asked.
   */
  warm?: (item: ItemId) => void;
  /**
   * (T1034) How many parts a water has in the open release: a LINK's `part=` is checked against it before the water opens
   * (a part that is not there would throw when the map frames it). Absent: no release is open, nothing can be framed.
   */
  partCount?: (item: ItemId) => Promise<number | null>;
}

/** The opener (`useOpenWater`'s value), React-free: what a test drives. */
export function waterOpener(source: Pick<WaterSource, "itemForSection">, go: (a: RouteAction) => void,
                            o: OpenerOptions = {}): WaterOpener {
  let latest = 0;
  /** Ask, then dispatch what was asked for — unless a later open was asked, or the route moved, meanwhile. */
  const settle = async (ask: (groupOf: ((item: ItemId) => Promise<readonly ItemId[] | undefined>) | undefined)
                          => Promise<RouteAction | null>): Promise<boolean> => {
    const mine = ++latest;
    const start = o.now?.();
    const groupOf = o.group && (start === undefined || start.mode === "water")
      ? (item: ItemId) => o.group!(item).catch(reported(undefined)) : undefined;
    const a = await ask(groupOf);
    if (a === null || mine !== latest || (o.now && o.now() !== start)) return false;
    openProbe.mark("go");
    go(a);
    return true;
  };
  return {
    tap: (sid, at, view, except = null) => settle((groupOf) => openTap(source, sid, at, view, except, groupOf, o.partOf,
                                                                        o.unnamed)),
    flow: (item, build) => settle(async (groupOf) => build(groupOf ? await groupOf(item) : undefined)).then(() => undefined),
    // nothing to wait on: at once, and any open still in flight is overtaken
    item: (item, view = "regs", part = null, whole = false) => {
      latest += 1; o.warm?.(item);
      const count = o.partCount;
      if (part === null || !count) { go(openWater({ item, view, part, whole })); return; }
      // (T1034) a link's part is checked against the water's own parts: a part that is not there is said (reported), the
      // water opens without it — never a throw at render, never a silently different part
      void settle(async () => {
        const n = await count(item).catch(reported(null));
        if (n !== null && part < n) return openWater({ item, view, part });
        reportFailure(new Error(`app link: part=${part} but ${item} has ${n ?? "no"} part(s): opened without a part`));
        return openWater({ item, view });
      });
    },
    cancel: () => { latest += 1; },
    // (T203) a result opens the water's Map, from Conditions or Regs alike — (T925) a water of one part too (T221 had
    // opened that one on its Regulations, with no Map view). (T747, owner 2026-11-14: "it is the section view but it is
    // fully zoomed out instead of on the first section") framed on THE PART ITS BAR NAMES (the frame's part,
    // `useFrameView`), never the whole water: T203 opened it whole while the bar said "Part 1 of 18", and on the wide
    // desktop strip (243 px tall) the whole Skeena read as the province. ⤢ "Whole river" is one press away.
    searched: (item, at = null) => {
      if (at && o.placePart) {
        // (T927) a boundary place: the water opens on the part holding it (a failed lookup opens it on its own part,
        // reported); a later open overtakes it, as any open that waits
        o.warm?.(item);
        const part = o.placePart;
        return settle(async () => openWater({ item, view: "map", fromSearch: true,
                                              part: await part(item, at).catch(reported(null)) })).then(() => undefined);
      }
      latest += 1; o.warm?.(item); go(openWater({ item, view: "map", fromSearch: true })); return Promise.resolve();
    },
  };
}

/** The Shell's handle on `openWater` / `openTap`, dispatching to the route. */
export function useOpenWater(source: Pick<WaterSource, "itemForSection">, go: (a: RouteAction) => void,
                             o: OpenerOptions = {}): WaterOpener {
  const { now, group, partOf, unnamed, warm, placePart, partCount } = o;
  return useMemo(() => waterOpener(source, go, { now, group, partOf, unnamed, warm, placePart, partCount }),
                 [source, go, now, group, partOf, unnamed, warm, placePart, partCount]);
}
