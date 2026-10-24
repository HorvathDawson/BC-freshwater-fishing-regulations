/**
 * WHERE THE READER IS: the route, its one back, the one water opener and the water header's views — the Shell's
 * navigation, read once here and handed down (T62 3.4).
 */
import { waterExit } from "@app/core";
import { useCallback, useMemo, useRef } from "react";
import { Platform } from "react-native";
import type { ItemId, WaterSource } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { frameViews, regsGroupOf, useOpenWater, useRoute, waterFrame, type BrowseTab, type Route } from "@app/ui";
import { edgeBackDepth, useBack, useEdgeBack, useViewSwipe, type SystemBack } from "@app/ui";
import { useRegulationKeys } from "@app/ui";
import { TABS, type TabKey } from "../TabBar";
import type { WaterViews } from "../ViewSwipe";

export function useShellNav({ source, loader, systemBack, initialRoute }: {
  source: WaterSource; loader: ReleaseLoader | null; systemBack: SystemBack | null; initialRoute?: Route;
}) {
  /*
   * THE ROUTE (P13): what is on screen is ONE value, moved by one reducer (`@app/ui` route.ts). The tab, the water
   * open, the spot, the add flow and the Conditions drill are read from it; every way of moving dispatches to it.
   * Water mode keeps the browse state underneath (`from`), so back restores it whole.
   */
  const [route, go] = useRoute(initialRoute);
  const browse = route.mode === "browse" ? route : route.from;
  const tab: TabKey = browse.tab;
  const frame = waterFrame(route);
  const item: ItemId | null = frame?.item ?? null;
  // the water's regulations read (P11): on the release, opening it opens core, water and its region's file — a water in
  // another region fetches that region's. The page that draws them is T01/T03's; until then the placeholder stands.
  useRegulationKeys(source, item);
  const adding = route.mode === "browse" ? route.adding : null;
  // a spot open over the browse map, or (T131) over a water, its pin tapped on the water's map
  const openSpot = route.mode === "browse" ? route.spot : route.spot ?? null;
  const toTab = (k: BrowseTab) => go({ type: "tab", tab: k });
  /*
   * G1: ONE BACK for every input — ‹, the platform's system back, the iOS edge swipe.
   */
  const { back, depth, layers: backLayers } = useBack(route, go, systemBack);
  // iOS: no tab-to-tab swipe (tabs are not history there); Android and the web keep that step
  const edge = useEdgeBack(back, Platform.OS, edgeBackDepth(route, depth));
  /*
   * ONE WATER SCREEN, WHICHEVER MAP YOU TAPPED (owner rule 2026-10-21): every water opens through `@app/ui`
   * `useOpenWater` — a tap on the Conditions map, on the Map tab and on a water's own map alike — which resolves the
   * tapped section's water in one place. The two maps' taps differ ONLY in the view put in front.
   */
  /*
   * (T58) The opener reads THE ROUTE NOW, so a slow lookup that finishes after the reader pressed back or × (or moved
   * on) is dropped, and a split lake's areas from the release, so "Flows into" another area of a lake held goes back to it.
   */
  const routeNow = useRef(route);
  routeNow.current = route;
  const now = useCallback(() => routeNow.current, []);
  const groupOf = useMemo(() => (loader ? regsGroupOf(loader) : undefined), [loader]);
  const opener = useOpenWater(source, go, { now, group: groupOf });
  /** G1 (owner 2026-10-21): a sideways swipe moves between a water's views — Regulations · Conditions · Map. */
  const viewSwipe = useViewSwipe(route, go);
  /**
   * THE MAP UNDER A WATER'S MAP VIEW asks the Map tab's questions (the day's status), whatever tab the water was
   * opened from — a water opened from the Conditions map must not paint its map with flow percentiles.
   */
  const mapTab: BrowseTab = route.mode === "water" ? "map" : tab;
  /*
   * THE WATER'S HEADER (FaceBar): the views its frame offers (all three; an unnamed reach, Regulations and Conditions — T53), the one on
   * screen, and where back goes, in the reader's words — the views are lateral, so back always leaves the water: to
   * the tab it was opened from (the Conditions map or the Map tab), or "Back" to the water flowed in from. The swipe
   * listens on the header and on the body: the header is the water frame's (T44 `WaterFrame`, above every view's body,
   * a sibling of it) and always listens; Regulations and Conditions listen on their bodies too (the panel's maps and
   * chart shielded); on Map the canvas is the body, header only.
   */
  // ‹ and × name where they go: the tab, or search when it is still open under the water (a map tap beside it)
  const exit = route.mode === "water"
    ? waterExit(route.stack.length, TABS.find((t) => t.k === route.from.tab)!.t, route.from.search) : null;
  const waterViews: WaterViews | null = route.mode === "water" && frame !== null && exit !== null
    ? { views: frameViews(frame), on: frame.view, onView: (v) => go({ type: "view", view: v }), onBack: back,
        backLabel: exit.back,
        // (owner 2026-10-22 late) the one-tap way out of every water, back to the tab the first was opened from
        close: () => go({ type: "close" }), closeLabel: exit.close,
        swipe: viewSwipe, bodySwipe: frame.view === "map" ? null : viewSwipe }
    : null;
  return { route, go, browse, tab, frame, item, adding, openSpot, toTab, back, backLayers, edge, opener, mapTab,
           waterViews };
}

export type ShellNav = ReturnType<typeof useShellNav>;
