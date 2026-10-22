/**
 * The menu's state (ui/menu.ts) and the recent error log (ui/errorLog.ts).
 */
import { describe, expect, it } from "vitest";
import { MENU_CLOSED, menuReduce, routeWhere, type MenuAction, type MenuState } from "./menu";
import type { ItemId, SectionId } from "@app/data";
import type { Route } from "./route";
import { createErrorLog, teeConsoleErrors } from "./errorLog";

const run = (...as: MenuAction[]): MenuState => as.reduce(menuReduce, MENU_CLOSED);

describe("the menu's state", () => {
  it("opens and closes the drawer, and a repeat is the same state (no re-render)", () => {
    const open = run({ type: "open" });
    expect(open).toEqual({ drawer: true, page: null, term: null, report: null });
    expect(menuReduce(open, { type: "open" })).toBe(open);
    expect(menuReduce(MENU_CLOSED, { type: "close" })).toBe(MENU_CLOSED);
  });

  it("hands the drawer over to the page it opened, so back from the page is the map", () => {
    expect(run({ type: "open" }, { type: "page", page: "glossary" }))
      .toEqual({ drawer: false, page: "glossary", term: null, report: null });
  });

  it("opens a term only on the glossary; closing the page forgets the term", () => {
    expect(run({ type: "page", page: "about" }, { type: "term", id: "bait" }).term).toBeNull();
    const t = run({ type: "page", page: "glossary" }, { type: "term", id: "bait" });
    expect(t.term).toBe("bait");
    expect(menuReduce(t, { type: "closeTerm" })).toEqual({ drawer: false, page: "glossary", term: null, report: null });
    expect(menuReduce(t, { type: "closePage" })).toEqual(MENU_CLOSED);
  });

  it("keeps where a report was asked from, and only for a report", () => {
    const where = { screen: "water · regs", tab: "Gear", place: "Vedder River" };
    expect(run({ type: "page", page: "report", report: where }).report).toEqual(where);
    expect(run({ type: "page", page: "about", report: where }).report).toBeNull();
    expect(run({ type: "page", page: "report", report: where }, { type: "page", page: "report" }).report).toBeNull();
  });
});

describe("the recent error log", () => {
  it("keeps the last N lines, oldest first, stamped with the local time", () => {
    const at = new Date(2026, 9, 21, 9, 5).getTime();
    const log = createErrorLog(3, 240, () => at);
    for (const m of ["one", "two", "three", "four"]) log.record(m);
    expect(log.recent()).toEqual(["09:05 two", "09:05 three", "09:05 four"]);
  });

  it("words an Error, joins the logged values and cuts a long line", () => {
    const at = new Date(2026, 9, 21, 18, 2).getTime();
    const log = createErrorLog(5, 24, () => at);
    log.record("map:", new Error("style failed"));
    log.record("x".repeat(40));
    log.record("   ");
    expect(log.recent()).toEqual(["18:02 map: style failed", `18:02 ${"x".repeat(17)}…`]);
  });

  it("tees console.error once, still calling the console, and undoes", () => {
    const seen: unknown[][] = [];
    const c = { error: (...a: unknown[]) => { seen.push(a); } };
    const log = createErrorLog(5, 240, () => new Date(2026, 0, 1, 7, 30).getTime());
    const undo = teeConsoleErrors(c, log);
    teeConsoleErrors(c, log);                       // a second tee is a no-op
    c.error("regs store:", new Error("fetch failed"));
    expect(seen).toHaveLength(1);
    expect(log.recent()).toEqual(["07:30 regs store: fetch failed"]);
    undo();
    c.error("after");
    expect(log.recent()).toHaveLength(1);
    expect(seen).toHaveLength(2);
  });
});

describe("where the reader was (the hamburger's report)", () => {
  const browse = { mode: "browse", tab: "map", spot: null, adding: null, shown: null } as const;

  it("on a browse screen: the tab, and the day the map answers for", () => {
    expect(routeWhere(browse, null)).toEqual({ screen: "map" });
    expect(routeWhere({ ...browse, tab: "conditions" }, "Sat, Oct 25")).toEqual({ screen: "conditions", day: "Sat, Oct 25" });
  });

  it("on a water: its view, the water, the part and the section tapped", () => {
    const r: Route = { mode: "water", from: browse, stack: [
      { item: "1" as ItemId, part: 0, tapSid: 5 as SectionId, at: null, view: "regs" },
      { item: "356308001" as ItemId, part: 2, tapSid: 812345 as SectionId, at: { lat: 49.1, lon: -122 }, view: "conditions" }] };
    expect(routeWhere(r, null)).toEqual({ screen: "water · conditions", water: "356308001", part: 2, section: 812345 });
  });

  it("an unnamed reach has no water and no part", () => {
    const r: Route = { mode: "water", from: browse, stack: [{ item: null, part: null, tapSid: 9 as SectionId, at: null, view: "conditions" }] };
    expect(routeWhere(r, "Today")).toEqual({ screen: "water · conditions", section: 9, day: "Today" });
  });
});

