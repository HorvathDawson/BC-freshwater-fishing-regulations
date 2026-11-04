// @vitest-environment jsdom
/**
 * (T385) SHARE A WATER: the link is the page's reload address on the production site, and it opens back — through the
 * web's own start link (`parseStartLink`, and `resolveStartLink` for a place) — on the same frame and day. A press sends
 * it through the platform; "Link copied" is said; a dismissed sheet says and reports nothing; a failure is said and
 * reported.
 */
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SHARE_WORDS, SITE_ORIGIN, setFailureSink } from "@app/core";
import type { ItemId, SectionId } from "@app/data";
import type { NavRows, PartRow } from "@app/data/regs";
import { resolveStartLink, type PlaceReads } from "./place";
import { HOME, reduce, waterFrame, type Route } from "./route";
import { openWater } from "./openWater";
import { parseStartLink } from "./startLink";
import { shareLink, useShare, type SharePort } from "./share";

const NOW = new Date(2026, 10, 4, 9, 30);
const RIVER = "gnis:1" as ItemId, BLK = 360000001;
/** A release of one unnamed reach (section 40, a line 0–1000 m on BLK): the reads a place is made from and resolved by. */
const PLACES: PlaceReads = {
  hasLines: async () => true,
  itemOrd: async () => null,
  parts: async () => [] as PartRow[],
  navRows: async () => ({ geo: [], marks: [], exit: null, continuesAs: null, continuesFrom: null, sections: [], stretches: [] }) as NavRows,
  sectionPart: async () => null,
  sectionLine: async (h) => (h === 40 ? { blk: BLK, from: 0, to: 1000 } : null),
  sectionAt: async (blk, m) => (blk === BLK && m >= 0 && m < 1000 ? 40 : null),
};
const searchOf = (url: string) => { expect(url.startsWith(`${SITE_ORIGIN}/`)).toBe(true); return url.slice(SITE_ORIGIN.length + 1); };

afterEach(() => { vi.restoreAllMocks(); });

describe("(T385) the shared link is the reload address, and parses back to the same frame", () => {
  it("a named water's Conditions view on another day, at a horizon: the same frame, day and horizon", async () => {
    const r = reduce(HOME, openWater({ item: RIVER, part: null, view: "conditions" }));
    const day = new Date(2026, 6, 1, 12);
    const url = (await shareLink(null, r, { day, horizon: 3 }, undefined, NOW))!;
    expect(url).toBe(`${SITE_ORIGIN}/?regs=${RIVER}&view=conditions&date=2026-07-01&horizon=3`);
    const back = parseStartLink(searchOf(url), NOW)!;
    expect([waterFrame(back.route), back.day?.toDateString(), back.horizon]).toEqual([waterFrame(r), day.toDateString(), 3]);
  });

  it("today is unsaid, and nothing of the sender's own (angler, theme) is in it", async () => {
    const r = reduce(HOME, openWater({ item: RIVER, part: null, view: "regs" }));
    expect(await shareLink(null, r, { day: NOW, horizon: 0 }, undefined, NOW)).toBe(`${SITE_ORIGIN}/?regs=${RIVER}`);
  });

  it("an unnamed reach shares its place, and the place opens the same reach", async () => {
    const r = reduce(HOME, openWater({ item: null, part: null, view: "regs", tapSid: 40 as SectionId, at: { lat: 49.1, lon: -122.2 } }));
    const url = (await shareLink(PLACES, r, null, undefined, NOW))!;
    expect(url).toBe(`${SITE_ORIGIN}/?blk=${BLK}&m=500&here=49.10000,-122.20000`);
    const opened = await resolveStartLink(parseStartLink(searchOf(url), NOW)!, PLACES, { itemForSection: async () => null });
    expect(waterFrame(opened.route)).toEqual(waterFrame(r));
  });

  it("browsing has no water to share; an unnamed reach with no place has no link", async () => {
    const reach = reduce(HOME, openWater({ item: null, part: null, view: "regs", tapSid: 40 as SectionId }));
    expect([await shareLink(null, HOME, null), await shareLink(null, reach, null)]).toEqual([null, null]);
  });
});

describe("(T385) a press", () => {
  const WATER: Route = reduce(HOME, openWater({ item: RIVER, part: null, view: "regs" }));
  const mount = (port: SharePort, r: Route = WATER) =>
    renderHook(() => useShare(port, null, r, { day: null, horizon: 0 }));

  it("sends the link through the platform; copied says so", async () => {
    const send = vi.fn(async () => "copied" as const);
    const { result } = mount({ send });
    await act(async () => { result.current!.press(); });
    await waitFor(() => expect(result.current!.said).toBe(SHARE_WORDS.copied));
    expect(send).toHaveBeenCalledWith({ url: `${SITE_ORIGIN}/?regs=${RIVER}`, title: SHARE_WORDS.title });
  });

  it("a dismissed sheet (resolved, or a browser's AbortError) says nothing and reports nothing", async () => {
    const sink = vi.fn();
    const undo = setFailureSink(sink);
    const abort = Object.assign(new Error("Share canceled"), { name: "AbortError" });
    for (const send of [vi.fn(async () => "dismissed" as const), vi.fn(async () => { throw abort; })]) {
      const { result, unmount } = mount({ send });
      await act(async () => { result.current!.press(); });
      await waitFor(() => expect(send).toHaveBeenCalled());
      await waitFor(() => expect(result.current!.busy).toBe(false));
      expect(result.current!.said).toBeNull();
      unmount();
    }
    expect(sink).not.toHaveBeenCalled();
    undo();
  });

  it("a real failure is said under the control and reported", async () => {
    const sink = vi.fn();
    const undo = setFailureSink(sink);
    const boom = new Error("share: this browser has neither a share sheet nor a clipboard");
    const { result } = mount({ send: async () => { throw boom; } });
    await act(async () => { result.current!.press(); });
    await waitFor(() => expect(result.current!.said).toBe(SHARE_WORDS.failed));
    expect(sink).toHaveBeenCalledWith(boom);
    undo();
  });

  it("no control while browsing, or with no platform share", () => {
    expect(renderHook(() => useShare({ send: async () => "sent" }, null, HOME, { day: null, horizon: 0 })).result.current).toBeNull();
    expect(renderHook(() => useShare(null, null, WATER, { day: null, horizon: 0 })).result.current).toBeNull();
  });
});
