import { describe, expect, it, vi } from "vitest";
import { applyAppLink, type OpenRequest } from "./appLinkOpen";

const run = (o: Parameters<typeof applyAppLink>[0], prev: OpenRequest | null = null) => {
  const setTheme = vi.fn();
  let req: OpenRequest | null = null;
  applyAppLink(o, { setTheme, setOpenRequest: (f) => { req = f(prev); } });
  return { setTheme, req: req as OpenRequest | null };
};

describe("(T1034, T1035) a tapped link in the app", () => {
  it("its theme is applied, its part and view reach the open request, and the count moves on", () => {
    const { setTheme, req } = run({ item: "gnis:3062" as never, view: "map", part: 2, theme: "dark" }, { item: "x", n: 4 });
    expect(setTheme).toHaveBeenCalledWith("dark");
    expect(req).toEqual({ item: "gnis:3062", view: "map", part: 2, n: 5 });
  });
  it("no theme in the link: the theme is left alone; a box link carries no theme or part", () => {
    expect(run({ item: "gnis:3062" as never, view: "regs", part: null, theme: null }).setTheme).not.toHaveBeenCalled();
    const box = run({ fit: [-118, 49, -114, 52] });
    expect([box.setTheme.mock.calls.length, box.req]).toEqual([0, { item: null, fit: [-118, 49, -114, 52], n: 1 }]);
  });
});
