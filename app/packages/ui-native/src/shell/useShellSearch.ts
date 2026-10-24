/**
 * (T48) SEARCH, OVER THE SAME MAP: the top bar's always-open field opens it (route `search`, on either map), and the
 * results sheet rises over the map that is already there — no second map. What the reader typed and picked is kept
 * here while search is open; a water opened from the results consumes it (back lands on the map). ONE session value
 * (`useSearchSession`) feeds both the sheet's rows and what the main map frames, lights and pins (T62 3.4: out of the
 * Shell, unchanged).
 */
import { useCallback, useEffect, useRef, useState, type MutableRefObject } from "react";
import { Keyboard, useWindowDimensions, type TextInput } from "react-native";
import type { ItemId, WaterSource } from "@app/data";
import { NO_SEARCH, searchStep, useSearchSession, type SearchAction, type SearchMemory } from "@app/ui";
import type { Camera } from "@app/map";
import { searchPanelWidth } from "../TopBar";
import { CLOSE_KEEPS_FOCUS } from "../searchFocus";
import type { ShellNav } from "./useShellNav";

export function useShellSearch({ source, nav, camera }: {
  source: WaterSource; nav: ShellNav; camera: MutableRefObject<Camera>;
}) {
  const { route, go, browse, tab, opener } = nav;
  const searching = browse.search;
  /** The screen's width: a wide one draws search as a panel beside the map (`searchPanelWidth`). */
  const screenWidth = useWindowDimensions().width;
  const [searchMemory, setSearchMemory] = useState<SearchMemory>(NO_SEARCH);
  const session = useSearchSession(source, searchMemory, searching);
  const field = useRef<TextInput>(null);
  /*
   * (T48) SEARCH GIVES THE CAMERA BACK. Where it was when search opened is kept; a pick moves the map (a water framed, a
   * town flown to); closing search — ‹, Escape, the system back, a tab — puts it back EXACTLY (`fit.camera`, sent
   * once) and drops the keyboard. A water opened FROM A RESULT consumes search (owner 2026-10-22 late, route
   * `fromSearch`): nothing is restored — the map comes back framed on that water (`arrive`, below).
   */
  const searchCamera = useRef<Camera | null>(null);
  const searchMoved = useRef(false);
  const [restore, setRestore] = useState<{ key: string; camera: Camera } | null>(null);
  const restores = useRef(0);
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
    if (searching) { searchCamera.current = { ...camera.current }; searchMoved.current = false; return; }
    if (keep) {
      quiet.current = true;                   // the focus event fires inside focus(): it opens nothing
      try { field.current?.focus(); } finally { quiet.current = false; }
    } else {
      field.current?.blur();
      Keyboard.dismiss();
    }
    const back = searchCamera.current;
    if (searchMoved.current && back) {
      camera.current = back;                       // a map mounted later (a spot was opened) starts there too
      setRestore({ key: `restore:${++restores.current}`, camera: back });
    }
  }, [searching]);
  useEffect(() => { if (searching && session.focus?.bbox) searchMoved.current = true; }, [searching, session.focus?.key]);
  // sent once: the next render hands the map no fit, so a later remount is never dragged back
  useEffect(() => { if (restore) setRestore(null); }, [restore]);
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
  const openSearched = (id: ItemId) => {
    const n = ++arrivals.current;
    const key = `arrive:${id}:${n}`;
    const f = session.focus?.key === `item:${id}` ? session.focus : null;
    arrivedAt.current = null;
    if (f?.bbox) setArrive({ key, bbox: f.bbox, refine: f.refine });
    else {
      setArrive(null);
      void source.waterBox(id).then((bbox) => { if (bbox && arrivals.current === n) setArrive({ key, bbox, refine: false }); });
    }
    searchMoved.current = false;            // the camera from before search is not put back: the water is the subject
    setSearchMemory(NO_SEARCH);             // the search is consumed, its text with it
    opener.searched(id, tab === "conditions" ? "conditions" : "regs");
  };
  /** One step of search (`searchStep`): a pick lowers the sheet to half, so the keyboard goes with it. */
  const searchAct = (a: SearchAction) => {
    const next = searchStep(searchMemory.pick, a).pick;
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
  return { searching, searchMemory, session, field, restore, arrive, openSearched, searchAct, searchType, closeSearch,
           closeToField, wideSearch, onFieldFocus };
}

export type ShellSearch = ReturnType<typeof useShellSearch>;
