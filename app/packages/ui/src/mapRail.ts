/**
 * THE SPOTS FLOW (owner 2026-10-21; T48 owner 2026-10-22): the Spots TAB's list and the long press that drops a spot
 * on either map. (The rail that once held a Spots button is Layers only now.) Decided here and drawn by
 * `@app/ui-native` `SpotsScreen` / `SpotDropSheet`, which compute nothing.
 *
 *   spotsMenu   the Spots tab's view model: your spots as rows (their words are `@app/data/spots`'), the TWO ways to
 *               add one (owner 2026-10-22: AT MY LOCATION — only where the reader has let the app know it, or will be
 *               asked on the tap itself: the app never asks silently; refused or absent it is not offered and the tab
 *               says why — and PICK ON THE MAP; the map's-middle "Add a spot here" is gone), the empty state, the stale
 *               line, an unreadable file.
 *   useMapRail  the PROVISIONAL pin a long press dropped (Cancel, back or a drag away removes it and nothing is saved;
 *               Continue starts the one spot capture at it), PICK MODE (the Map tab until its first tap, which drops
 *               that same provisional pin; Cancel, back or the route moving leave it with nothing), the ways to add
 *               from the tab, and where "Show" sent the map. Every way of adding ends in the route's own `addSpot` at a point, so there is ONE capture flow.
 *               The pin and the framing belong to the map where they were made: the route moving anywhere (a tab, a
 *               spot opened, a water, back) forgets them on that same render — else a map remounted on the way back
 *               would be framed on an old spot again and wear its pin. The one exception is Show's own move to the Map
 *               tab, which is what the framing was made for.
 */
import { useCallback, useEffect, useState } from "react";
import { SPOTS_MENU, boxAround, type Bbox, type LatLon } from "@app/core";
import { needsRefresh, spotLabel, spotRowLines, staleSpotsLine, type Spot } from "@app/data/spots";
import { HOME, type Route, type RouteAction } from "./route";

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
export type SpotsMenuAddKind = "mine" | "pick";
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
  /** The ways to add one: your location (when it can be offered), then pick on the map. Empty where spots cannot be saved. */
  adds: SpotsMenuAdd[];
  /** Why "At my location" is not offered (refused, or none on this platform); null when it is, or while still asking. */
  note: string | null;
}

export function spotsMenu(spots: readonly Spot[], o: {
  error?: string | null; canAdd: boolean; canRefresh: boolean; permission: LocatePermission | null;
}): SpotsMenuVM {
  const error = o.error ?? null;
  const stale = spots.filter(needsRefresh).length;
  const adds: SpotsMenuAdd[] = !o.canAdd ? [] : [
    ...(o.permission === "granted" ? [{ k: "mine" as const, label: SPOTS_MENU.addMine, detail: "" }]
      : o.permission === "prompt" ? [{ k: "mine" as const, label: SPOTS_MENU.addMine, detail: SPOTS_MENU.addMineAsk }]
      : []),
    { k: "pick", label: SPOTS_MENU.addPick, detail: SPOTS_MENU.addPickDetail },
  ];
  const note = !o.canAdd ? null
    : o.permission === "denied" ? SPOTS_MENU.mineOff : o.permission === "unavailable" ? SPOTS_MENU.mineNone : null;
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
    note,
  };
}

/** Where "Show" sends the map: a box about the spot (the map's `fit`, keyed so it moves once), and the pin there. */
export interface SpotFly { key: string; bbox: Bbox; at: LatLon }

/** How far either side of a shown spot the map frames: a reach's worth, which the map's fit floor (z14) then caps. */
export const SPOT_FLY_KM = 0.4;

export interface MapRail {
  /** What the platform says of location, read each time the Spots tab is entered; null until it answers or with no `locate`. */
  permission: LocatePermission | null;
  /** The provisional pin a long press dropped, or null. Only the drop sheet's Continue makes anything of it. */
  drop: LatLon | null;
  /** A long press on the map. */
  pressLong(lat: number, lon: number): void;
  /** Cancel, back, Done or a drag away: the pin goes and nothing is saved. */
  cancelDrop(): void;
  /** Continue: the one spot capture, starting at the pin. */
  continueDrop(): void;
  /** "At my location": read it (the platform may ask, on this tap), then the capture there; a failure is said. */
  addMine(): Promise<void>;
  /** PICK ON THE MAP: on the Map tab, until its first tap (`tapPick`), Cancel, back, or the route moving anywhere else. */
  picking: boolean;
  /** "Pick on the map": the Map tab, in pick mode. */
  pick(): void;
  /** The map tapped in pick mode: the provisional pin there (the drop sheet asks), and pick mode ends. */
  tapPick(lat: number, lon: number): void;
  /** The banner's Cancel, back, Escape: pick mode ends and nothing is made. */
  cancelPick(): void;
  /** The location could not be read; cleared when the route moves. */
  error: string | null;
  /**
   * "Show" — from the Spots tab's row or a spot's own screen alike (one function): the Map tab, framed on the spot with
   * its pin. The framing is made FOR the Map tab, so the move there keeps it; any later move forgets it.
   */
  show(spot: Pick<Spot, "id" | "lat" | "lon">): void;
  fly: SpotFly | null;
}

/** Where the route is, as far as the rail cares: any change of it forgets the pin and the framing. */
export function railPlace(r: Route): string {
  return r.mode === "water" ? "water"
    : `${r.tab}|${r.spot ?? ""}|${r.adding === null ? "" : "adding"}|${r.search ? "search" : ""}`;
}

/** Where Show lands: the Map tab's root. */
const MAP_ROOT = railPlace(HOME);
/** The Spots tab's root, where the permission is asked. */
const SPOTS_ROOT = railPlace({ ...HOME, tab: "spots" });

export function useMapRail(go: (a: RouteAction) => void, locate: Locate | null = null,
                           route: Route | null = null): MapRail {
  const [permission, setPermission] = useState<LocatePermission | null>(null);
  const [drop, setDrop] = useState<LatLon | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [fly, setFly] = useState<(SpotFly & { for: string }) | null>(null);
  // pick mode is made FOR the Map tab's root (as Show's framing is): the move there keeps it, any other move ends it
  const [pickFor, setPickFor] = useState<string | null>(null);
  const [flights, setFlights] = useState(0);
  // the route moved: forget, during this render (React's "state from the previous render"), and draw nothing stale —
  // but a Show made for where the route went (the Map tab) is what that move was for
  const place = route === null ? "" : railPlace(route);
  const [madeAt, setMadeAt] = useState(place);
  const moved = madeAt !== place;
  const keepFly = fly !== null && fly.for === place;
  const keepPick = pickFor === place;
  if (moved) {
    setMadeAt(place);
    setDrop(null);
    setError(null);
    if (!keepFly) setFly(null);
    if (!keepPick) setPickFor(null);
  }
  const picking = pickFor !== null && pickFor === place;

  // asked each time the Spots tab is entered: the reader may have changed it in the system's settings meanwhile
  const onSpots = place === SPOTS_ROOT;
  useEffect(() => {
    if (!onSpots || !locate) return;
    let alive = true;
    locate.permission().then((p) => { if (alive) setPermission(p); }, () => { if (alive) setPermission("unavailable"); });
    return () => { alive = false; };
  }, [onSpots, locate]);

  const addAt = useCallback((at: LatLon) => {
    setDrop(null);
    go({ type: "addSpot", at });
  }, [go]);

  const flying = fly === null || (moved && !keepFly) ? null : fly;
  const tapPick = useCallback((lat: number, lon: number) => {
    if (!picking) return;
    setPickFor(null);
    setDrop({ lat, lon });
  }, [picking]);
  return {
    permission, drop: moved ? null : drop, error: moved ? null : error,
    fly: flying && { key: flying.key, bbox: flying.bbox, at: flying.at },
    pressLong: (lat, lon) => { setPickFor(null); setDrop({ lat, lon }); },
    cancelDrop: () => setDrop(null),
    continueDrop: () => { if (drop) addAt(drop); },
    picking,
    pick: () => {
      setDrop(null);
      setError(null);
      setFly(null);
      setPickFor(MAP_ROOT);
      if (place !== MAP_ROOT) go({ type: "tab", tab: "map" });
    },
    tapPick,
    cancelPick: () => setPickFor(null),
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
      setFly({ key: `spot:${spot.id}:${n}`, bbox: boxAround(at, SPOT_FLY_KM), at, for: MAP_ROOT });
      if (place !== MAP_ROOT) go({ type: "tab", tab: "map" });
    },
  };
}
