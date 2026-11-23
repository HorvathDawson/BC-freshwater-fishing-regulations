import { describe, expect, it, vi } from "vitest";
import { START_NOTICE_WORDS } from "@app/ui";
import { applyAppLink, shownNotice, type OpenRequest } from "./appLinkOpen";

const D0 = { day: null, past: false, horizon: null };
const P0 = { regs: { tab: "keep" as const, sheet: null, scroll: 0 }, who: null, whole: false };
const run = (o: Parameters<typeof applyAppLink>[0], prev: OpenRequest | null = null) => {
  const setTheme = vi.fn(), setWho = vi.fn(), setNotice = vi.fn();
  let req: OpenRequest | null = null;
  applyAppLink(o, { setTheme, setWho, setNotice, setOpenRequest: (f) => { req = f(prev); } });
  return { setTheme, setWho, setNotice, req: req as OpenRequest | null };
};

describe("(T1034, T1035) a tapped link in the app", () => {
  it("its theme is applied, its part and view reach the open request, and the count moves on", () => {
    const { setTheme, req } = run({ item: "gnis:3062" as never, view: "map", part: 2, theme: "dark", ...D0, ...P0 }, { item: "x", n: 4 });
    expect(setTheme).toHaveBeenCalledWith("dark");
    expect(req).toEqual({ item: "gnis:3062", view: "map", part: 2, day: null, horizon: 0, regs: P0.regs, n: 5 });
  });
  it("no theme in the link: the theme is left alone; a box link carries no theme or part", () => {
    expect(run({ item: "gnis:3062" as never, view: "regs", part: null, theme: null, ...D0, ...P0 }).setTheme).not.toHaveBeenCalled();
    const box = run({ fit: [-118, 49, -114, 52], ...D0 });
    expect([box.setTheme.mock.calls.length, box.req]).toEqual([0, { item: null, fit: [-118, 49, -114, 52], day: null, horizon: 0, n: 1 }]);
  });
});

describe("(T1038) a tapped link hands the Shell what the web's start link hands it", () => {
  const XMAS = new Date(2026, 11, 24, 12);
  it("its day, horizon, page start and whole reach the open request; its angler is the session's", () => {
    const { setWho, setNotice, req } = run({ item: "gnis:3062" as never, view: "map", part: null, theme: null, day: XMAS, past: false,
                                             horizon: 3, regs: { tab: "gear", sheet: "pos:pos", scroll: 40 },
                                             who: { age: "u16", residency: "resident" }, whole: true });
    expect(req).toEqual({ item: "gnis:3062", view: "map", part: null, day: XMAS, horizon: 3,
                          regs: { tab: "gear", sheet: "pos:pos", scroll: 40 }, whole: true, n: 1 });
    expect(setWho).toHaveBeenCalledWith({ age: "u16", residency: "resident" });
    expect(setNotice).toHaveBeenCalledWith(null);
  });
  it("no angler in the link: the session's stands; a day gone by is said in the web's words; a box carries its day", () => {
    const w = run({ item: "gnis:3062" as never, view: "regs", part: null, theme: null, ...D0, past: true, ...P0 });
    expect([w.setWho.mock.calls.length, w.setNotice.mock.calls]).toEqual([0, [[START_NOTICE_WORDS["day-past"]]]]);
    expect(run({ fit: [-118, 49, -114, 52], day: XMAS, past: false, horizon: "change" }).req)
      .toEqual({ item: null, fit: [-118, 49, -114, 52], day: XMAS, horizon: "change", n: 1 });
  });
});

describe("(T1038) the notice line", () => {
  it("the start's notice first, else the link's; none once shut", () => {
    expect([shownNotice("a", "b", false), shownNotice(null, "b", false), shownNotice(null, null, false), shownNotice("a", "b", true)])
      .toEqual(["a", "b", null, null]);
  });
});
