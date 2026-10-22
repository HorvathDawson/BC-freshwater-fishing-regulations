/**
 * THE MENU'S STATE (owner 2026-10-21): the hamburger's drawer, the page it opened, the glossary term open on it, and
 * where a problem report was asked from. Headless: `@app/ui-native` `MenuHost` draws it.
 *
 * NOT ROUTE FRAMES. A menu page is not a place on the map (it belongs in no deep link, no saved route) — like a sheet,
 * each open level registers as a BACK LAYER (`useBackLayer`, gestures.ts), so Android's back, the browser's back and a
 * phone browser's swipe-back close the top one first: the term, then the page, then the drawer. Picking a page closes
 * the drawer in the same step (a navigation drawer hands over to what it opened), so back from a page is the map.
 */
import { useCallback, useMemo, useReducer } from "react";
import { sheetDismiss, type GlossaryTerm, type MenuPageKey, type ReportWhere, type Statics } from "@app/core";
import type { RegsLoader } from "@app/data/regs";
import { useAsync, type Async } from "./async";
import { useRegsStore } from "./regs";
import { waterFrame, type Route } from "./route";

export interface MenuState {
  /** the drawer is out */
  readonly drawer: boolean;
  /** the page on screen, or none */
  readonly page: MenuPageKey | null;
  /** the glossary term open on the Glossary page */
  readonly term: string | null;
  /** a report asked from a regulations tab: where the reader was (prefilled into the report's diagnostics) */
  readonly report: ReportWhere | null;
}

export type MenuAction =
  | { type: "open" } | { type: "close" }
  | { type: "page"; page: MenuPageKey; report?: ReportWhere | null }
  | { type: "closePage" }
  | { type: "term"; id: string } | { type: "closeTerm" };

export const MENU_CLOSED: MenuState = { drawer: false, page: null, term: null, report: null };

export function menuReduce(s: MenuState, a: MenuAction): MenuState {
  switch (a.type) {
    case "open": return s.drawer ? s : { ...s, drawer: true };
    case "close": return s.drawer ? { ...s, drawer: false } : s;
    // the drawer hands over to the page it opened; a report keeps where it was asked from, any other page forgets it
    case "page": return { drawer: false, page: a.page, term: null, report: a.page === "report" ? a.report ?? null : null };
    case "closePage": return s.page === null ? s : { ...s, page: null, term: null, report: null };
    case "term": return s.page === "glossary" ? { ...s, term: a.id } : s;
    case "closeTerm": return s.term === null ? s : { ...s, term: null };
  }
}

export interface MenuControls {
  readonly state: MenuState;
  open(): void;
  close(): void;
  show(page: MenuPageKey): void;
  /** Report a problem, from wherever the reader is (a regulations tab passes the page's own words). */
  report(where?: ReportWhere | null): void;
  closePage(): void;
  openTerm(id: string): void;
  closeTerm(): void;
}

export function useMenu(): MenuControls {
  const [state, go] = useReducer(menuReduce, MENU_CLOSED);
  const open = useCallback(() => go({ type: "open" }), []);
  const close = useCallback(() => go({ type: "close" }), []);
  const show = useCallback((page: MenuPageKey) => go({ type: "page", page }), []);
  const report = useCallback((where?: ReportWhere | null) => go({ type: "page", page: "report", report: where ?? null }), []);
  const closePage = useCallback(() => go({ type: "closePage" }), []);
  const openTerm = useCallback((id: string) => go({ type: "term", id }), []);
  const closeTerm = useCallback(() => go({ type: "closeTerm" }), []);
  return useMemo(() => ({ state, open, close, show, report, closePage, openTerm, closeTerm }),
    [state, open, close, show, report, closePage, openTerm, closeTerm]);
}

/**
 * Where the reader was when they opened Report a problem from the menu (the route's own facts; `day` is the map's day
 * as the legend writes it, null = today and unsaid). Only what is known is set, so the report shows no empty lines.
 */
export function routeWhere(r: Route, day: string | null): ReportWhere {
  const f = waterFrame(r);
  const out: ReportWhere = { screen: f ? `water · ${f.view}` : r.mode === "browse" ? r.tab : null };
  if (f?.item) out.water = f.item;
  if (f && f.part !== null) out.part = f.part;
  if (f && f.tapSid !== null) out.section = f.tapSid;
  if (day) out.day = day;
  return out;
}

/** The drawer's sideways drag: it follows the finger LEFT only and closes past a third of its width or on a flick —
 *  the sheet's own rule (`sheetDismiss`), turned on its side. */
export interface DrawerDrag {
  offset(dx: number): number;
  release(dx: number, vx: number, width: number): boolean;
}

export function useDrawerDrag(onClose: () => void): DrawerDrag {
  return useMemo(() => ({
    offset: (dx: number) => Math.min(0, dx),
    release: (dx: number, vx: number, width: number) => {
      if (!sheetDismiss(-dx, -vx, width)) return false;
      onClose();
      return true;
    },
  }), [onClose]);
}

/** The glossary as the release carries it: its terms (`answers.glossary`) and the printed → PDF page map. */
export interface Glossary { readonly terms: readonly GlossaryTerm[]; readonly pdfPages: Statics["pdfPages"] }

/**
 * The release's glossary — the SAME terms the regs page's `gl:` sheets open (never a second copy of the words). Read only
 * while `on` (the page is open): the first read opens the regs store's water file. ready(null): no loader (the dev
 * bundle holds no regulations). A term's pages link through the store's own printed → PDF map (`statics.pdfPages`).
 */
export function useGlossary(loader: RegsLoader | null, on: boolean): Async<Glossary | null> {
  const store = useRegsStore(on ? loader : null);
  const st = store.value ?? null;
  const asked = useAsync<Glossary | null>(async () => {
    if (!st) return null;
    const statics = await st.statics();
    return { terms: statics.glossary.terms, pdfPages: statics.pdfPages };
  }, `glossary:${loader?.release ?? ""}:${st ? "store" : "none"}`, on && store.state === "ready");
  if (store.state === "failed") return store;
  if (!on || store.state === "loading") return LOADING;
  return asked;
}

const LOADING = { state: "loading", value: null, error: null } as const;
