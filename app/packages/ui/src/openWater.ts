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
import { reported } from "@app/core";
import { openProbe, type ItemId, type WaterSource, type SectionId } from "@app/data";
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
  /** (T221) The water is solo (one part, no continuation, `@app/core` `soloWater`): no Map view, Regulations in front. */
  solo?: boolean;
}

/**
 * THE ONE BUILDER of an `openWater` route action. The view asked for is put in front when the frame offers it, else the
 * frame's first view (`frameViews`): an unnamed reach asked for the water Map opens on its Regulations.
 */
export function openWater(o: WaterOpen): RouteAction {
  const asked = o.view ?? "regs";
  const solo = !!o.solo && o.item !== null;
  const views = frameViews({ item: o.item, solo });
  const a: RouteAction = { type: "openWater", item: o.item, tapSid: o.tapSid ?? null, at: o.at ?? null,
                          part: o.part ?? null, view: views.includes(asked) ? asked : views[0]!,
                          ...(o.group ? { group: o.group } : {}), ...(solo ? { solo: true } : {}) };
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
                              solo?: OpenerOptions["solo"]): Promise<RouteAction | null> {
  const item = await waterOfSection(source, sid);
  if (item !== null && item === except) return null;
  const [group, at2, isSolo] = await Promise.all([
    item !== null && groupOf ? groupOf(item) : undefined,
    // (T369 D3) the tapped water opens AT its part: one frame for it
    // (T368) a failed read opens part-less (each view asks the same `targetFor`), and is reported
    item !== null && partOf ? partOf(item, sid).catch(reported(null)) : null,
    // (T498) and KNOWING whether it is solo, as a search result does: told after it opened, its Map view dropped out of
    // the strip under the reader (the pane changed shape after it was shown). A failed read opens with the Map view.
    item !== null && solo ? solo(item).catch(reported(false)) : false]);
  return openWater({ item, tapSid: sid, at, view, group, ...(at2 ? { part: at2.part } : {}),
                     ...(isSolo ? { solo: true } : {}) });
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
  item(item: ItemId, view?: View): void;
  /**
   * A water picked from search's results: the search is consumed, so back lands on the map (route `fromSearch`). (T221)
   * A solo water (`OpenerOptions.solo`) opens on its Regulations; any other on its whole Map (T203).
   */
  searched(item: ItemId): Promise<void>;
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
   * (T221) Whether a water is solo (one part, no continuation: `@app/core` `soloWater` over its group). Asked for a search
   * result, a Map asked for by id, and (T498) a map tap, before the frame opens; a read that fails counts as not solo (its
   * Map view is offered, as before). Absent: never asked.
   */
  solo?: (item: ItemId) => Promise<boolean>;
  /**
   * (T369 D3) The part a tapped section of a water opens at — the Regulations view's own
   * answer (`frameView.ts` `regsPartOf`). Every tapped frame then carries its part from the start, so no view adopts one
   * later and the address names it. A read that fails opens part-less (each view asks the same `targetFor`).
   */
  partOf?: (item: ItemId, sid: SectionId) => Promise<{ part: number } | null>;
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
    tap: (sid, at, view, except = null) => settle((groupOf) => openTap(source, sid, at, view, except, groupOf, o.partOf, o.solo)),
    flow: (item, build) => settle(async (groupOf) => build(groupOf ? await groupOf(item) : undefined)).then(() => undefined),
    // nothing to wait on: at once, and any open still in flight is overtaken — (T369 D13) but a Map asked for asks first
    // whether the water is solo (no Map view: its Regulations), as a search result does, so no Map mounts and is dropped
    item: (item, view = "regs") => {
      if (view !== "map" || !o.solo) { latest += 1; go(openWater({ item, view })); return; }
      const ask = o.solo;
      void settle(async () => ((await ask(item).catch(reported(false)))
        ? openWater({ item, view: "regs", solo: true }) : openWater({ item, view })));
    },
    cancel: () => { latest += 1; },
    // (T203) a result opens the water's Map, the whole water shown, from Conditions or Regs alike — (T221) unless the
    // water is solo: nothing to choose on its map, so its Regulations
    searched: (item) => {
      if (!o.solo) { latest += 1; go(openWater({ item, view: "map", whole: true, fromSearch: true })); return Promise.resolve(); }
      const ask = o.solo;
      return settle(async () => ((await ask(item).catch(reported(false)))
        ? openWater({ item, view: "regs", solo: true, fromSearch: true })
        : openWater({ item, view: "map", whole: true, fromSearch: true }))).then(() => undefined);
    },
  };
}

/** The Shell's handle on `openWater` / `openTap`, dispatching to the route. */
export function useOpenWater(source: Pick<WaterSource, "itemForSection">, go: (a: RouteAction) => void,
                             o: OpenerOptions = {}): WaterOpener {
  const { now, group, solo, partOf } = o;
  return useMemo(() => waterOpener(source, go, { now, group, solo, partOf }), [source, go, now, group, solo, partOf]);
}
