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
 * AN UNNAMED REACH (no registry item — most gauged reaches) has no regulations page, so its frame (`item: null`) offers
 * the Conditions view alone. It opens the SAME frame from either map: the builder puts Conditions in front whatever view
 * was asked, so a Map-tab tap on it opens exactly what a Conditions-map tap opens (only where back goes differs). Neither
 * map is a dead end for a stream the other one opens.
 */
import { useMemo } from "react";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import { frameViews, type LatLon, type RouteAction, type View } from "./route";

export interface WaterOpen {
  /** The water; null for an unnamed reach (then `tapSid` is required, and Conditions — its only view — is in front). */
  item: ItemId | null;
  /** The section the reader tapped, null when not opened by a tap. */
  tapSid?: SectionId | null;
  /** Where on `tapSid` the tap landed. */
  at?: LatLon | null;
  /** The part to open at (a flow into a water names one). */
  part?: number | null;
  /** The view in front: "conditions" from the Conditions map, "regs" from the Map tab and everything else. */
  view?: View;
}

/**
 * THE ONE BUILDER of an `openWater` route action. The view asked for is put in front when the frame offers it, else the
 * frame's first view (`frameViews`): an unnamed reach opens on Conditions whichever map it was tapped on.
 */
export function openWater(o: WaterOpen): RouteAction {
  const asked = o.view ?? "regs";
  const views = frameViews({ item: o.item });
  return { type: "openWater", item: o.item, tapSid: o.tapSid ?? null, at: o.at ?? null, part: o.part ?? null,
           view: views.includes(asked) ? asked : views[0]! };
}

/**
 * A map tap -> the route action, or null when nothing opens: the tapped section resolved to its water HERE, once.
 * `except`: the water already on screen (a tap on a water's own map changes nothing).
 */
export async function openTap(source: Pick<RegsSource, "itemForSection">, sid: SectionId, at: LatLon | null,
                              view: View, except: ItemId | null = null): Promise<RouteAction | null> {
  const item = await source.itemForSection(sid);
  if (item !== null && item === except) return null;
  return openWater({ item, tapSid: sid, at, view });
}

export interface WaterOpener {
  /** A tap on a map: the section, where on it, and which view to put in front. */
  tap(sid: SectionId, at: LatLon | null, view: View, except?: ItemId | null): Promise<void>;
  /** A water by its id (search, a spot, a deep path). */
  item(item: ItemId, view?: View): void;
}

/** The Shell's handle on `openWater` / `openTap`, dispatching to the route. */
export function useOpenWater(source: Pick<RegsSource, "itemForSection">, go: (a: RouteAction) => void): WaterOpener {
  return useMemo(() => ({
    tap: async (sid, at, view, except = null) => {
      const a = await openTap(source, sid, at, view, except);
      if (a !== null) go(a);
    },
    item: (item, view = "regs") => go(openWater({ item, view })),
  }), [source, go]);
}
