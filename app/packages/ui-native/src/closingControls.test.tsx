/**
 * T988 — EVERY CLOSING CONTROL CLOSES ITS SURFACE. A Done, Close, Back or scrim press, mounted under the app's REAL back
 * layers (`useBack`, the provider Shell wraps the app in), must take the surface out of the tree — not only call a spy.
 * One table over every sheet, dialog and menu page: each row opens the surface, presses one control, and the control
 * must be gone. A new closing control joins the table; one that stops closing fails here.
 */
import { afterEach, describe, it, expect } from "vitest";
import { useState, type ReactNode } from "react";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { BackLayerContext, historySync, LayoutWidthContext, useBack, useRoute, type HistoryPort } from "@app/ui";
import { dayRange, joinTestAsk, SHEET_WORDS, WEB_SPOTS_WORDS, type RegsPage, type SheetKey, type SheetVM } from "@app/core";
import { DaySheet } from "./regs/DaySheet";
import { SpotDropSheet } from "./SpotDropSheet";
import { SyncErrorSheet } from "./SyncErrorSheet";
import { JoinTestCard } from "./JoinTestCard";
import { Sheet } from "./Sheet";
import { SheetHost } from "./regs/SheetHost";
import { MenuPage } from "./menu/MenuPage";
import { FirstRunNotice } from "./FirstRunNotice";
import { InSeasonNotice } from "./InSeasonNotice";
import { LIGHT } from "./theme";
import vedder1010 from "./regs/fixtures/vedder_1010.json";

afterEach(cleanup);

/**
 * A model of the browser's history, driven as the web glue does (`historySync`): a traversal lands later and fires
 * `popstate`. null: no system back (the layers alone, as a phone's `BackHandler` leaves them).
 */
function browser() {
  const entries: (number | null)[] = [null];
  let i = 0;
  const listeners = new Set<() => void>();
  const queue: number[] = [];
  const port: HistoryPort = {
    index: () => entries[i] ?? null,
    push: (n) => { entries.splice(i + 1); entries.push(n); i = entries.length - 1; },
    go: (d) => { queue.push(d); },
    listen: (f) => { listeners.add(f); return () => { listeners.delete(f); }; },
  };
  const flush = () => act(() => {
    while (queue.length) {
      const j = i + queue.shift()!;
      if (j < 0 || j >= entries.length || j === i) continue;
      i = j;
      for (const f of [...listeners]) f();
    }
  });
  return { port, flush, at: () => i };
}
let web: ReturnType<typeof browser> | null = null;

/** the app's back layers, as Shell provides them */
function Layers({ children }: { children: ReactNode }) {
  const [route, go] = useRoute();
  const { layers } = useBack(route, go, web ? (back) => historySync(web!.port, back) : null);
  return <BackLayerContext.Provider value={layers}>{children}</BackLayerContext.Provider>;
}
/** the owner of the open flag, as every caller is: the surface is drawn while `open` */
function Owner({ draw }: { draw: (open: boolean, close: () => void) => ReactNode }) {
  const [open, setOpen] = useState(true);
  return <>{draw(open, () => setOpen(false))}</>;
}

type Frozen = Omit<RegsPage, "sheet"> & { sheets: Record<string, SheetVM | null> };
const FROZEN = vedder1010 as unknown as Frozen;
const VEDDER = { ...FROZEN, sheet: (k: SheetKey) => FROZEN.sheets[k] ?? null } as unknown as RegsPage;
const SOURCES_KEY = Object.keys(FROZEN.sheets).find((k) => FROZEN.sheets[k]?.title === "Sources") as SheetKey | undefined;

interface Row { name: string; wide?: number; draw: (open: boolean, close: () => void) => ReactNode; press: string[] }
const P = LIGHT;
const NOW = new Date(2026, 10, 19, 9, 30);
const RANGE = dayRange(NOW, null);
const DONE_AND_SCRIM = [SHEET_WORDS.done, SHEET_WORDS.close];
const ROWS: Row[] = [
  { name: "Sheet, phone", press: DONE_AND_SCRIM,
    draw: (open, close) => <Sheet open={open} onClose={close} title="Sheet title" palette={P}><></></Sheet> },
  { name: "Sheet, wide dialog", wide: 1024, press: DONE_AND_SCRIM,
    draw: (open, close) => <Sheet open={open} onClose={close} title="Sheet title" palette={P} dialog><></></Sheet> },
  ...(SOURCES_KEY ? [{ name: "regs Sources sheet", press: DONE_AND_SCRIM,
    draw: (open: boolean, close: () => void) => <SheetHost page={VEDDER} open={open ? SOURCES_KEY : null} onOpen={() => {}} onClose={close} palette={P} /> }] : []),
  { name: "regs sheet, wide", wide: 1280, press: DONE_AND_SCRIM,
    draw: (open, close) => <SheetHost page={VEDDER} open={open ? "pos:pos" : null} onOpen={() => {}} onClose={close} palette={P} /> },
  { name: "day sheet, phone", press: DONE_AND_SCRIM,
    draw: (open, close) => <DaySheet open={open} onClose={close} palette={P} day={null} onDay={() => {}} range={RANGE} now={NOW} anchor="regs-date" /> },
  { name: "day popover, desktop", wide: 1440, press: DONE_AND_SCRIM,
    draw: (open, close) => <DaySheet open={open} onClose={close} palette={P} day={null} onDay={() => {}} range={RANGE} now={NOW} anchor="regs-date" /> },
  { name: "spot drop sheet", press: DONE_AND_SCRIM,
    draw: (open, close) => <SpotDropSheet at={open ? { lat: 49, lon: -123 } : null} palette={P} onCancel={close} onContinue={() => {}} /> },
  { name: "sync error sheet", press: DONE_AND_SCRIM,
    draw: (open, close) => <SyncErrorSheet open={open} failed={[{ k: "feed", checkedAt: null, updatedAt: null, error: "x", failedAt: 1, offline: false } as never]} palette={P} checking={false} onRetry={() => {}} onClose={close} /> },
  { name: "join-the-test card", press: [WEB_SPOTS_WORDS.join.popup.close, SHEET_WORDS.close],
    draw: (open, close) => open ? <JoinTestCard store="iphone" ask={joinTestAsk("iphone", "a@b.ca")} palette={P} onClose={close} /> : null },
  { name: "menu page", press: ["Back"],
    draw: (open, close) => <MenuPage open={open} title="Menu title" onBack={close} palette={P}><></></MenuPage> },
  { name: "bell", press: DONE_AND_SCRIM,
    draw: (open, close) => <InSeasonNotice open={open} palette={P} onClose={close} /> },
  { name: "first-run notice", press: DONE_AND_SCRIM,
    draw: (open, close) => <FirstRunNotice state={{ open, accept: close, close }} palette={P} /> },
];

/**
 * react-native-web's Modal unmounts when its exit animation ends; jsdom runs no animation, so the end is sent by hand to
 * each Modal portal's animated box (as SheetWide.test's `shown()` does for the entry).
 */
function settle() {
  for (const portal of [...document.body.children]) {
    const box = portal.firstElementChild;
    if (box) act(() => { for (const t of ["animationend", "webkitAnimationEnd"]) box.dispatchEvent(new Event(t, { bubbles: true })); });
  }
}

/** Shell's first-run notice and its "Sources and licences" page, wired as Shell wires them */
function NoticeAndPage() {
  const [notice, setNotice] = useState(true);
  const [page, setPage] = useState(false);
  return (
    <>
      <MenuPage open={page} title="Sources and licences" onBack={() => setPage(false)} palette={P}><></></MenuPage>
      <FirstRunNotice state={{ open: notice && !page, accept: () => setNotice(false), close: () => setNotice(false) }}
                      palette={P} onSources={() => setPage(true)} />
    </>
  );
}

describe("T988 the first-run notice's Sources page and its Done", () => {
  for (const system of ["layers", "browser history"] as const) {
    it(`${system}: Sources opens the page, Back returns to the notice, Done closes it, nothing is left`, async () => {
      web = system === "browser history" ? browser() : null;
      render(<Layers><NoticeAndPage /></Layers>);
      const gone = (label: string) => waitFor(() => { settle(); web?.flush(); expect(screen.queryAllByLabelText(label)).toHaveLength(0); }, { timeout: 3000 });
      act(() => { fireEvent.click(screen.getByTestId("first-run-sources")); });
      web?.flush();
      await gone(SHEET_WORDS.done);                                    // the notice stepped aside
      expect(screen.getByRole("heading", { name: "Sources and licences" })).toBeTruthy();
      act(() => { fireEvent.click(screen.getAllByLabelText("Back")[0]!); });
      web?.flush();
      await gone("Back");                                              // the page is gone
      await waitFor(() => expect(screen.getAllByLabelText(SHEET_WORDS.done)).toHaveLength(1));   // the notice is back
      act(() => { fireEvent.click(screen.getAllByLabelText(SHEET_WORDS.done)[0]!); });
      web?.flush();
      await gone(SHEET_WORDS.done);
      if (web) expect(web.at()).toBe(0);
    });
  }
});

describe("T988 every closing control takes its surface out of the tree", () => {
  it("the regs fixture has a Sources sheet to pin", () => { expect(SOURCES_KEY).toBeDefined(); });
  // each control twice: on a phone (the layers alone) and in a browser (the layers in step with its history)
  for (const system of ["layers", "browser history"] as const) for (const r of ROWS) for (const control of r.press) {
    it(`${system}: ${r.name}: ${control}`, async () => {
      web = system === "browser history" ? browser() : null;
      const tree = <Layers><Owner draw={r.draw} /></Layers>;
      render(r.wide ? <LayoutWidthContext.Provider value={r.wide}>{tree}</LayoutWidthContext.Provider> : tree);
      const btn = screen.getAllByLabelText(control)[0]!;
      act(() => { fireEvent.click(btn); });
      web?.flush();
      await waitFor(() => { settle(); web?.flush(); expect(screen.queryAllByLabelText(control)).toHaveLength(0); }, { timeout: 3000 });
      // a closed surface leaves no history entry of its own behind
      if (web) expect(web.at()).toBe(0);
    });
  }
});
