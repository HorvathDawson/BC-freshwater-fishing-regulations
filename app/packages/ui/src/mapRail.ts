/**
 * THE MAP RAIL (owner 2026-10-21, Fish Washington's side stack): the browse maps' Layers and Spots buttons, the Spots
 * menu, and the long press that drops a spot. Decided here and drawn by `@app/ui-native` `MapRail` / `SpotsSheet` /
 * `SpotDropSheet`, which compute nothing.
 *
 *   spotsMenu   the menu's view model: your spots as rows (their words are `@app/data/spots`'), the ways to add one
 *               (the map's middle; your location — only where the reader has let the app know it, or will be asked
 *               on the tap itself: the app never asks silently), the empty state, the stale line, an unreadable file.
 *   useMapRail  which sheet is open, the PROVISIONAL pin a long press dropped (Cancel, back or a drag away removes
 *               it and nothing is saved; Continue starts the one spot capture at it), and where "Show" sent the map.
 *               Every way of adding ends in the route's own `addSpot` at a point, so there is one capture flow.
 *               All three belong to the map where they were made: the route moving anywhere (a tab, a spot opened, a
 *               water, back) forgets them on that same render, as a tab change forgets search's `shown` — else a map
 *               remounted on the way back would be framed on an old spot again and wear its pin.
 */
import { useCallback, useEffect, useState } from "react";
import { SPOTS_MENU, boxAround, type Bbox, type LatLon } from "@app/core";
import { needsRefresh, spotLabel, spotRowLines, staleSpotsLine, type Spot } from "@app/data/spots";
import type { Route, RouteAction } from "./route";

/** Whether the app may read where the device is: asked of the platform, never assumed. */
export type LocatePermission = "granted" | "prompt" | "denied" | "unavailable";

/** The platform's location (the app's `locate`, beside `pickPhoto`). Absent where the platform has none wired. */
export interface Locate {
  /** What the platform says now — no prompt is raised by asking this. */
  permission(): Promise<LocatePermission>;
  /** Where the device is. May raise the platform's own prompt: called only from the reader's own tap. */
  here(): Promise<LatLon>;
}

export interface SpotsMenuRow {
  id: string;
  /** The row's name, said aloud too. */
  title: string;
  /** What was recorded, then where: `spotRowLines` whole. */
  lines: string;
  /** The first photo's reference, for the thumbnail; null: NO PHOTO. */
  photo: string | null;
  /** "Show" — the map goes to the spot — said aloud. */
  showLabel: string;
}
export type SpotsMenuAddKind = "here" | "mine";
export interface SpotsMenuAdd { k: SpotsMenuAddKind; label: string; detail: string }
export interface SpotsMenuVM {
  title: string;
  /** The spots could not be read (P01): said, never an empty list. */
  error: string | null;
  /** No spots (and no error): what a spot is, since this is the only place that says so. */
  empty: { title: string; body: string } | null;
  rows: SpotsMenuRow[];
  /** How many spots are missing their weather, when they can be refreshed; null otherwise. */
  stale: string | null;
  /** The ways to add one, first the map's middle. Empty where spots cannot be saved. */
  adds: SpotsMenuAdd[];
}

export function spotsMenu(spots: readonly Spot[], o: {
  error?: string | null; canAdd: boolean; canRefresh: boolean; permission: LocatePermission | null;
}): SpotsMenuVM {
  const error = o.error ?? null;
  const stale = spots.filter(needsRefresh).length;
  const adds: SpotsMenuAdd[] = !o.canAdd ? [] : [
    { k: "here", label: SPOTS_MENU.addHere, detail: SPOTS_MENU.addHereDetail },
    ...(o.permission === "granted" ? [{ k: "mine" as const, label: SPOTS_MENU.addMine, detail: "" }]
      : o.permission === "prompt" ? [{ k: "mine" as const, label: SPOTS_MENU.addMine, detail: SPOTS_MENU.addMineAsk }]
      : []),
  ];
  return {
    title: SPOTS_MENU.title,
    error,
    empty: error === null && spots.length === 0 ? { title: SPOTS_MENU.emptyTitle, body: SPOTS_MENU.emptyBody } : null,
    rows: error !== null ? [] : spots.map((s) => {
      const title = spotLabel(s);
      return { id: s.id, title, lines: spotRowLines(s).join("\n"), photo: s.photos[0] ?? null,
               showLabel: SPOTS_MENU.showLabel(title) };
    }),
    stale: error === null && o.canRefresh && stale > 0 ? staleSpotsLine(stale) : null,
    adds,
  };
}

/** Where "Show" sends the map: a box about the spot (the map's `fit`, keyed so it moves once), and the pin there. */
export interface SpotFly { key: string; bbox: Bbox; at: LatLon }

/** How far either side of a shown spot the map frames: a reach's worth, which the map's fit floor (z14) then caps. */
export const SPOT_FLY_KM = 0.4;

export interface MapRail {
  /** Which rail sheet is up. The Layers sheet stays the Shell's own state; this is the Spots menu. */
  spotsOpen: boolean;
  openSpots(): void;
  closeSpots(): void;
  /** What the platform says of location, read each time the menu opens; null until it answers or with no `locate`. */
  permission: LocatePermission | null;
  /** The provisional pin a long press dropped, or null. Only the drop sheet's Continue makes anything of it. */
  drop: LatLon | null;
  /** A long press on the map. */
  pressLong(lat: number, lon: number): void;
  /** Cancel, back, Done or a drag away: the pin goes and nothing is saved. */
  cancelDrop(): void;
  /** Continue: the one spot capture, starting at the pin. */
  continueDrop(): void;
  /** "Add a spot here": the capture at the map's middle. */
  addAt(at: LatLon): void;
  /** "At my location": read it (the platform may ask, on this tap), then the capture there; a failure is said. */
  addMine(): Promise<void>;
  /** The location could not be read; cleared when the menu opens again. */
  error: string | null;
  /** "Show": the map goes to the spot and the menu closes. */
  show(spot: Pick<Spot, "id" | "lat" | "lon">): void;
  fly: SpotFly | null;
}

/** Where the route is, as far as the rail cares: any change of it forgets the rail's menu, pin and framing. */
export function railPlace(r: Route): string {
  return r.mode === "water" ? "water"
    : `${r.tab}|${r.spot ?? ""}|${r.adding === null ? "" : "adding"}|${r.shown ?? ""}`;
}

export function useMapRail(go: (a: RouteAction) => void, locate: Locate | null = null,
                           route: Route | null = null): MapRail {
  const [spotsOpen, setSpotsOpen] = useState(false);
  const [permission, setPermission] = useState<LocatePermission | null>(null);
  const [drop, setDrop] = useState<LatLon | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [fly, setFly] = useState<SpotFly | null>(null);
  const [flights, setFlights] = useState(0);
  // the route moved: forget, during this render (React's "state from the previous render"), and draw nothing stale
  const place = route === null ? "" : railPlace(route);
  const [madeAt, setMadeAt] = useState(place);
  const moved = madeAt !== place;
  if (moved) {
    setMadeAt(place);
    setSpotsOpen(false);
    setDrop(null);
    setFly(null);
  }

  // asked each time the menu opens: the reader may have changed it in the system's settings meanwhile
  useEffect(() => {
    if (!spotsOpen || !locate) return;
    let alive = true;
    locate.permission().then((p) => { if (alive) setPermission(p); }, () => { if (alive) setPermission("unavailable"); });
    return () => { alive = false; };
  }, [spotsOpen, locate]);

  const addAt = useCallback((at: LatLon) => {
    setSpotsOpen(false);
    setDrop(null);
    go({ type: "addSpot", at });
  }, [go]);

  return {
    spotsOpen: spotsOpen && !moved, permission, drop: moved ? null : drop, error, fly: moved ? null : fly,
    openSpots: () => { setError(null); setSpotsOpen(true); },
    closeSpots: () => setSpotsOpen(false),
    pressLong: (lat, lon) => { setSpotsOpen(false); setDrop({ lat, lon }); },
    cancelDrop: () => setDrop(null),
    continueDrop: () => { if (drop) addAt(drop); },
    addAt,
    addMine: async () => {
      if (!locate) return;
      setError(null);
      try {
        addAt(await locate.here());
      } catch {
        setError(SPOTS_MENU.noLocation);
      }
    },
    show: (spot) => {
      const n = flights + 1;
      setFlights(n);
      const at = { lat: spot.lat, lon: spot.lon };
      setFly({ key: `spot:${spot.id}:${n}`, bbox: boxAround(at, SPOT_FLY_KM), at });
      setSpotsOpen(false);
    },
  };
}
