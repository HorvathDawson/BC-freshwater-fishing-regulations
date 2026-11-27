/** (T1194) A gauge's route and address: `?gauge=<station>` is the same screen the tap opens; the address round-trips. */
import { describe, expect, it } from "vitest";
import type { StationId } from "@app/data";
import { HOME, openGauge, parseStartLink, reduce, routeSearch, type Route } from "./index";

const tapped = (s: string): Route => reduce(reduce(HOME, { type: "tab", tab: "conditions" }), openGauge(s as StationId));

describe("(T1194) ?gauge=<station>", () => {
  it("opens the SAME route a tap on the dot does (one load path), over the Conditions map", () => {
    expect(parseStartLink("?gauge=08MH024")!.route).toEqual(tapped("08MH024"));
  });
  it("the address of the page is its station, and reads back to the same route", () => {
    const q = routeSearch(tapped("08MH024"), "?theme=dark");
    expect(q).toBe("?theme=dark&gauge=08MH024");
    expect(parseStartLink(q)!.route).toEqual(tapped("08MH024"));
  });
  it("leaving the page drops the address (back on the map says no gauge)", () => {
    expect(routeSearch(reduce(tapped("08MH024"), { type: "back" }), "?gauge=08MH024")).toBe("?browse=conditions");
  });
  it("a malformed station, or a gauge beside another place, is refused by name", () => {
    expect(() => parseStartLink("?gauge=../x")).toThrow(/gauge=/);
    expect(() => parseStartLink("?gauge=08MH024&show=gnis:1")).toThrow(/gauge=/);
  });
  it("(review 2026-11-27) a gauge beside a water or a place is refused by name, never one dropped silently", () => {
    expect(() => parseStartLink("?gauge=08MH024&regs=gnis:3062")).toThrow(/gauge=08MH024 is not a station beside a water/);
    expect(() => parseStartLink("?gauge=08MH024&blk=1&m=2")).toThrow(/gauge=/);
    expect(() => parseStartLink("?gauge=08MH024&browse=map")).toThrow(/gauge=/);
  });
  it("a station the release does not hold still opens the route (the page shows the error, never the map)", () => {
    expect(parseStartLink("?gauge=ZZZZZZZ")!.route).toMatchObject({ mode: "gauge", station: "ZZZZZZZ" });
  });
});
