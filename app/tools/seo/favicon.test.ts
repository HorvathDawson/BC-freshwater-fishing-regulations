import { describe, expect, it } from "vitest";
import { withFavicon } from "./favicon";
import { pngSize } from "./share";
import { ICON_SIZES, TOUCH_PATH, TOUCH_SIZE, iconPath } from "../web-icons.mjs";

describe("the site's favicon (T244, T1042)", () => {
  const run = () => {
    const files = new Map([["a/index.html", "<!doctype html><html><head><title>x</title></head>"],
                           ["b/index.html", `<html><head><link rel="icon" href="/x.ico"></head>`], ["robots.txt", "<head>"]]);
    const binary = new Map<string, Uint8Array>();
    withFavicon(files, binary);
    return { files, binary };
  };
  it("adds favicon.svg and one icon link to each HTML page without one; leaves the rest", () => {
    const { files } = run();
    expect(files.get("favicon.svg")).toContain("<svg");
    expect(files.get("a/index.html")).toContain(`<link rel="icon" type="image/svg+xml" href="/favicon.svg">`);
    expect(files.get("b/index.html")).toContain(`<link rel="icon" href="/x.ico">`);
    expect(files.get("b/index.html")).not.toContain("favicon.svg");
    expect(files.get("robots.txt")).toBe("<head>");
  });
  it("(T1042) every HTML page links the 48 / 96 / 192 px PNGs and the touch icon, once; a page that has them is unchanged", () => {
    const { files } = run();
    for (const p of ["a/index.html", "b/index.html"]) {
      const h = files.get(p)!;
      for (const n of ICON_SIZES) expect(h).toContain(`<link rel="icon" type="image/png" sizes="${n}x${n}" href="/${iconPath(n)}">`);
      expect(h).toContain(`<link rel="apple-touch-icon" sizes="${TOUCH_SIZE}x${TOUCH_SIZE}" href="/${TOUCH_PATH}">`);
      expect(h.split("<!--icons-->").length).toBe(2);
    }
    const again = new Map(files);
    withFavicon(again, new Map());
    expect(again.get("a/index.html")).toBe(files.get("a/index.html"));
  });
  it("(T1042) the files exist at the sizes the links state (Google Search: a square PNG, larger than 48 px recommended)", () => {
    const { binary } = run();
    for (const n of ICON_SIZES) expect(pngSize(binary.get(iconPath(n))!)).toEqual({ width: n, height: n });
    expect(pngSize(binary.get(TOUCH_PATH)!)).toEqual({ width: TOUCH_SIZE, height: TOUCH_SIZE });
    expect(ICON_SIZES).toEqual([48, 96, 192]);
  });
});
