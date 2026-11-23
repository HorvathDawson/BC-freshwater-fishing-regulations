/**
 * (T48) SEARCH, OVER THE SAME MAP: the top bar's always-open field opens it (route `search`, on either map), and the
 * results sheet rises over the map that is already there — no second map. What the reader typed and picked is kept
 * here while search is open; a water opened from the results consumes it (back lands on the map). ONE session value
 * (`useSearchSession`) feeds both the sheet's rows and what the main map frames, lights and pins (T62 3.4: out of the
 * Shell, unchanged).
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Keyboard, useWindowDimensions, type TextInputInstance } from "react-native";
import type { ItemId, WaterSource } from "@app/data";
import type { StreamPlace } from "@app/data/regs/place";
import { NO_SEARCH, reportFailure, searchStep, useRecentSearches, useSearchSession, type SearchAction, type SearchMemory } from "@app/ui";
import { searchPanelWidth } from "../TopBar";
import { CLOSE_KEEPS_FOCUS } from "../searchFocus";
import { sheetRows } from "../search/rows";
import { keyStops } from "../search/keys";
import { useListKeys } from "../search/listKeys";
import type { ShellNav } from "./useShellNav";

/** (T250) the search field's handle. RN 0.88's `TextInputInstance` is an alias of a class its types do not export, so a
 *  hook returning a ref to it cannot have its type written out (TS2742); a local interface over it can. */
export interface SearchField extends TextInputInstance {}

export function useShellSearch({ source, nav, recentStore }: {
  source: WaterSource; nav: ShellNav;
  /** (T247) where recent searches are kept (device-only); absent: kept for this run */
  recentStore?: import("@app/ui").RecentStore | null;
}) {
  const { route, go, browse, opener } = nav;
  const searching = browse.search;
  /** The screen's width: a wide one draws search as a panel beside the map (`searchPanelWidth`). */
  const screenWidth = useWindowDimensions().width;
  const [searchMemory, setSearchMemory] = useState<SearchMemory>(NO_SEARCH);
  const session = useSearchSession(source, searchMemory, searching);
  const field = useRef<SearchField>(null);
  // (T247) a search the reader picked a result from is a recent search
  const recents = useRecentSearches(recentStore);
  /*
   * (T730, owner 2026-11-14) CLOSING SEARCH LEAVES THE MAP WHERE SEARCH MOVED IT, AND STARTS SEARCH AFRESH. "click on
   * thompson river it moves to show it but then if you click back it goes back to original location when it feels like
   * it should have stayed at the location it moved to Incase the user was just trying to get there" — a pick moves the
   * map (a water framed, a town flown to) and every close (‹, Escape, the system back, a swipe down, a tab, the wide
   * panel's ✕ or a click beside it) keeps that camera: nothing is sent back (T48 put the camera from before search back;
   * the owner's words replace that). And "if you highlight search again it goes back to where you were highlighting it
   * didn't reset search state": every close forgets the query and the pick, so the field opens empty, on the recents.
   * The keyboard drops. A water opened FROM A RESULT consumes search the same way (owner 2026-10-22 late, route
   * `fromSearch`) and the map comes back framed on that water (`arrive`, below). A water opened by a tap on the map
   * beside the half sheet does NOT close search (it stays under the water), so back from it returns to the results.
   */
  const wasSearching = useRef(searching);
  /*
   * (T69) FOCUSED BUT CLOSED. A close the keyboard asked for — Escape, the panel's ✕ — on a wide screen on the web hands
   * the focus back to the field instead of dropping it on the page body (`toField`); the field then holds it with
   * search closed. That focus must not open search again (`quiet`): typing, Enter or ↓ there does, or focusing it
   * afresh. A click on the map, back and a tab change leave the focus where they put it; a phone drops it (its keyboard).
   */
  const toField = useRef(false);
  const quiet = useRef(false);
  useEffect(() => {
    if (searching === wasSearching.current) return;
    wasSearching.current = searching;
    const keep = toField.current && CLOSE_KEEPS_FOCUS && searchPanelWidth(screenWidth) !== null;
    toField.current = false;
    if (searching) return;
    if (keep) {
      quiet.current = true;                   // the focus event fires inside focus(): it opens nothing
      try { field.current?.focus(); } finally { quiet.current = false; }
    } else {
      field.current?.blur();
      Keyboard.dismiss();
    }
    setSearchMemory(NO_SEARCH);               // (T730) the next open is a fresh search
  }, [searching]);
  /*
   * (Owner 2026-10-22 late, "back from a searched water lands on the map, at the water") A water opened from a result
   * consumes search — the route's `from` is the map with search closed — and the map comes back FRAMED ON THAT WATER:
   * the very framing search gave it (its own outline, `session.focus`; the bundle's box when search had not placed it
   * yet). Sent once, as the browse map returns (back or ×, from any water stacked above it), and forgotten at the next
   * move of the route, so a later return (a map tap's water, then back) is never dragged back to it.
   */
  const [arrive, setArrive] = useState<{ key: string; bbox: readonly [number, number, number, number];
                                         refine: boolean } | null>(null);
  const arrivals = useRef(0);
  const arrivedAt = useRef<typeof route | null>(null);
  useEffect(() => {
    if (!arrive) return;
    // landed (the browse map is back): note where; any later move of the route — another water included — forgets it
    if (arrivedAt.current === null) { if (route.mode === "browse") arrivedAt.current = route; }
    else if (arrivedAt.current !== route) { arrivedAt.current = null; setArrive(null); }
  }, [route, arrive]);
  /** A water picked from the results — (T927) a boundary place too: its water, at the part holding the place. */
  const openSearched = (id: ItemId, at: StreamPlace | null = null) => {
    const n = ++arrivals.current;
    const key = `arrive:${id}:${n}`;
    const f = session.focus?.key === `item:${id}` ? session.focus : null;
    arrivedAt.current = null;
    if (f?.bbox) setArrive({ key, bbox: f.bbox, refine: f.refine });
    else {
      setArrive(null);
      void source.waterBox(id).then((bbox) => { if (bbox && arrivals.current === n) setArrive({ key, bbox, refine: false }); },
                                    reportFailure);   // (T368) not framed: reported
    }
    // (T508) the search is consumed, its text with it — ONCE THE WATER HAS OPENED: consumed
    // before, the browse screen drew one more screen between the results and the pane (the field
    // emptied, the recent searches, the water's outline gone), and only then was the opened pane held over it
    const q = searchMemory.q;
    void opener.searched(id, at).then(() => { recents.push(q); setSearchMemory(NO_SEARCH); }, reportFailure);
  };
  /**
   * (T269, owner 2026-10-29) THE PAGE STARTED ON A WATER (a `/water/<slug>` page, a start link) or at one (`?show=`):
   * the map, whenever it comes back — the ‹ or × of that first water, the system back — comes back FRAMED ON THAT WATER,
   * as it stands under a tapped water's pane; nothing is lit (owner 2026-10-29: closing leaves the map as closing a tapped
   * pane does — the camera kept, no selection). The same arrival as a searched water's, once, forgotten at the next move.
   */
  const landOn = (id: ItemId) => {
    const n = ++arrivals.current;
    arrivedAt.current = null;
    void source.waterBox(id).then((bbox) => {
      if (bbox && arrivals.current === n) setArrive({ key: `arrive:${id}:${n}`, bbox, refine: false });
    }, reportFailure);   // (T368) not framed: reported
  };
  /**
   * (T1010) THE PAGE STARTED ON A BOX (`?fit=w,s,e,n`: a region page's "Open the map"): the browse map framed on it, once,
   * as a landing on a water is (`landOn`) — nothing lit, forgotten at the next move.
   */
  const landOnBox = (bbox: readonly [number, number, number, number]) => {
    const n = ++arrivals.current;
    arrivedAt.current = null;
    setArrive({ key: `fit:${bbox.join(",")}:${n}`, bbox, refine: false });
  };
  /** One step of search (`searchStep`): a pick lowers the sheet to half, so the keyboard goes with it. */
  const searchAct = (a: SearchAction) => {
    const next = searchStep(searchMemory.pick, a).pick;
    if (a.t === "select" || a.t === "go") recents.push(searchMemory.q);
    if (next.selected !== null || next.town !== null) { field.current?.blur(); Keyboard.dismiss(); }
    setSearchMemory({ q: searchMemory.q, pick: next });
  };
  const searchType = useCallback((q: string) => setSearchMemory((m) => ({ q, pick: searchStep(m.pick, { t: "typed" }).pick })),
    []);
  /** Close search: the phone's ‹, Escape, and on a wide screen the panel's ✕ and a click on the map beside it. */
  const closeSearch = useCallback(() => go({ type: "search", open: false }), [go]);
  /** (T69) A keyboard's close — Escape, the panel's ✕, the phone's ‹ — that gives the focus back to the field (wide web). */
  const closeToField = useCallback(() => { toField.current = true; closeSearch(); }, [closeSearch]);
  /** (owner 2026-10-22 late) A wide screen's search is a panel beside the map: a click on that map closes it. */
  const wideSearch = searching && searchPanelWidth(screenWidth) !== null;
  /** The focus event of the field: opens search, unless it is the field taking the focus back (`quiet`). */
  const onFieldFocus = () => { if (!quiet.current) go({ type: "search", open: true }); };
  /** (T247) a recent search tapped: it becomes the query again */
  const pickRecent = useCallback((q: string) => searchType(q), [searchType]);
  /*
   * (T858) THE KEYBOARD IN THE LIST, on the web: ↓ / ↑ from the field move a highlight through what the sheet draws
   * (`keyStops` over the very rows it draws), Enter presses it (listKeys.web.ts). A phone has none of it (the native
   * twin: no highlight, no key taken).
   */
  const stops = useMemo(() => searching ? keyStops(sheetRows(searchMemory.pick, session, recents.list), searchMemory.pick) : [],
                        [searching, searchMemory.pick, session, recents.list]);
  const keys = useListKeys({ open: searching, stops, query: searchMemory.q, field });
  return { searching, searchMemory, recents, pickRecent, session, field, arrive, openSearched, landOn, landOnBox, searchAct, searchType, closeSearch,
           closeToField, wideSearch, onFieldFocus, keys };
}

export type ShellSearch = ReturnType<typeof useShellSearch>;
