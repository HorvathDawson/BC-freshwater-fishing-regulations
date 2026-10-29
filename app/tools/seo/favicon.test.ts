import { describe, expect, it } from "vitest";
import { withFavicon } from "./favicon";

describe("the site's favicon (T244)", () => {
  it("adds favicon.svg and one icon link to each HTML page without one; leaves the rest", () => {
    const files = new Map([["a/index.html", "<!doctype html><html><head><title>x</title></head>"],
                           ["b/index.html", `<html><head><link rel="icon" href="/x.ico"></head>`], ["robots.txt", "<head>"]]);
    withFavicon(files);
    expect(files.get("favicon.svg")).toContain("<svg");
    expect(files.get("a/index.html")).toContain(`<head><link rel="icon" type="image/svg+xml" href="/favicon.svg"><title>`);
    expect(files.get("b/index.html")).toBe(`<html><head><link rel="icon" href="/x.ico"></head>`);
    expect(files.get("robots.txt")).toBe("<head>");
  });
});
