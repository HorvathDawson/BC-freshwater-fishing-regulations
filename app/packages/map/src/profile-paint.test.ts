/**
 * THE PROFILE COLOURING'S SEQUENCES, call by call (colour-latency AP; the verifier's condition 1).
 *
 * The log below is every call the colouring makes on the map, in order: colour paints (summarised as `plain` or
 * `day:<closed>|<own>|<base>` so a wrong day or a stale one reads in the string), feature-state sets and clears, and the
 * renderer's restore. Each scenario asserts the WHOLE log, because the failure being prevented is an order — a clear
 * that comes after the new labels, a day installed before the tiles are labelled, a highlight never put back.
 */
import { describe, expect, it } from "vitest";
import { dayOfYear, type StatusCode, type StatusIndex } from "@app/core";
import { baseAdapter, paintFor, type MapHandle } from "./adapters/contract";
import { ProfileColouring, coloursEachTile, type ProfileRenderer, type ProfileTarget, type TileSections } from "./profile-paint";
import { resolveTheme } from "./style";

/** A status index with the given profile per section and code per profile per day (default: the profile's first). */
function fakeIndex(name: string, bySection: Record<number, number>,
                   codes: Record<number, StatusCode | Record<number, StatusCode>>): StatusIndex & { name: string } {
  const profiles = Object.keys(codes).length;
  const code = (p: number, on: Date): StatusCode => {
    const c = codes[p]!;
    return typeof c === "string" ? c : c[dayOfYear(on)] ?? "base";
  };
  return {
    name, handles: "0".repeat(16), reachDigest: "0".repeat(16), sections: Object.keys(bySection).length,
    waters: 0, profiles,
    profileOf: (s) => bySection[Number(s)] ?? null,
    profileCodeOn: code,
    codeOn: (s, on) => (bySection[Number(s)] === undefined ? "base" : code(bySection[Number(s)]!, on)),
    waterCodeOn: () => "base",
  };
}

const JUN1 = new Date(2025, 5, 1), JAN1 = new Date(2026, 0, 1);
/** A: section 10 closed in January and own otherwise (p0), 11 own all year (p1), 12 outside B.C. (p2). */
const A = fakeIndex("A", { 10: 0, 11: 1, 12: 2 },
                    { 0: { [dayOfYear(JAN1)]: "closed", [dayOfYear(JUN1)]: "own" }, 1: "own", 2: "outside" });
/** B, another release: the same handles numbered differently — p0 now means "closed all year". */
const B = fakeIndex("B", { 11: 0, 13: 1 }, { 0: "closed", 1: "own" });

function rig() {
  const log: string[] = [];
  const t = resolveTheme("light");
  const plain: Record<string, unknown> = {};
  for (const l of ["stream", "lake", "lake__edge"]) plain[l] = paintFor(l, "plain", t)[l === "lake" ? "fill-color" : "line-color"];
  /** A colour paint, summarised: `plain`, or the day's label sets as the match carries them. */
  const describe = (layer: string, v: unknown): string => {
    if (JSON.stringify(v) === JSON.stringify(plain[layer])) return "plain";
    const s = JSON.stringify(v);
    const sets = [t["color.status.closed"], t["color.status.own"], t["color.status.base"]].map((c) => {
      const re = new RegExp(`(\\[[^\\[\\]]*\\]),"${String(c)}"`);
      return (re.exec(s)?.[1] ?? "[]").replace(/"/g, "");
    });
    return `day:${sets.join("|")}`;
  };
  const h: MapHandle = {
    setVisibility: () => {},
    setPaint: (l, p, v) => { if (p === "line-color" || p === "fill-color") log.push(`paint ${l} ${describe(l, v)}`); },
    setFeatureState: () => {},
    clearFeatureStates: () => {},
  };
  const target: ProfileTarget = {
    setState: (l, id, s) => log.push(`state ${l} ${id} ${JSON.stringify(s)}`),
    clearAll: (l) => log.push(`clear ${l}`),
  };
  let tiles: TileSections[] = [];
  const renderer: ProfileRenderer = { loaded: () => tiles, restore: () => log.push("restore") };
  const adapter = baseAdapter("web");
  // the Map tab: rivers and lakes in the profile mode (the lake's edge follows)
  adapter.setLayerMode(h, "stream", "profile", "light");
  adapter.setLayerMode(h, "lake", "profile", "light");
  const c = new ProfileColouring(adapter);
  const take = () => log.splice(0);
  return { h, target, renderer, adapter, c, take, setTiles: (x: TileSections[]) => { tiles = x; } };
}

describe("profile colouring: what each change does to the map", () => {
  it("with no day the profile layers are the plain map — not grey, not coloured", () => {
    const r = rig();
    expect(r.take()).toEqual(["paint stream plain", "paint lake plain", "paint lake__edge plain"]);
  });

  it("the first index labels every loaded tile, THEN installs the day; nothing to clear", () => {
    const r = rig();
    r.take();
    r.setTiles([{ stream: [10, 11], lake: [12] }, { stream: [11, 99] }]);
    r.c.set(r.h, r.target, r.renderer, { index: A, day: JUN1 }, "light");
    expect(r.take()).toEqual([
      "paint stream plain", "paint lake plain", "paint lake__edge plain",
      'state stream 10 {"p":"p0"}', 'state stream 11 {"p":"p1"}', 'state lake 12 {"p":"p2"}',
      'state stream 99 {"p":"b"}',
      "paint stream day:[]|[p0,p1]|[b]", "paint lake day:[]|[p0,p1]|[b]", "paint lake__edge day:[]|[p0,p1]|[b]",
    ]);
    expect(r.c.calls).toBe(4);
  });

  it("a new tile labels only the ids not labelled yet, per source-layer; a day change sets NO state", () => {
    const r = rig();
    r.setTiles([{ stream: [10] }]);
    r.c.set(r.h, r.target, r.renderer, { index: A, day: JUN1 }, "light");
    r.take();
    expect(r.c.label(r.target, { stream: [10, 11], lake: [10] })).toBe(2);
    expect(r.take()).toEqual(['state stream 11 {"p":"p1"}', 'state lake 10 {"p":"p0"}']);
    r.c.set(r.h, r.target, r.renderer, { index: A, day: JAN1 }, "light");
    expect(r.take()).toEqual(
      ["paint stream day:[p0]|[p1]|[b]", "paint lake day:[p0]|[p1]|[b]", "paint lake__edge day:[p0]|[p1]|[b]"]);
    // the same reading day again (another Date, same day) is no change at all
    r.c.set(r.h, r.target, r.renderer, { index: A, day: new Date(2026, 0, 1, 18) }, "light");
    expect(r.take()).toEqual([]);
  });

  it("RELEASE SWAP: plain, clear all three source-layers, restore, forget, relabel, then the new day", () => {
    const r = rig();
    r.setTiles([{ stream: [10, 11] }]);
    r.c.set(r.h, r.target, r.renderer, { index: A, day: JUN1 }, "light");
    r.take();
    r.setTiles([{ stream: [10, 11, 13] }]);
    r.c.set(r.h, r.target, r.renderer, { index: B, day: JUN1 }, "light");
    expect(r.take()).toEqual([
      "paint stream plain", "paint lake plain", "paint lake__edge plain",
      "clear stream", "clear lake", "clear wetland",
      "restore",
      // 10 and 11 were labelled under A and are labelled again: the de-dupe was reset with the clear
      'state stream 10 {"p":"b"}', 'state stream 11 {"p":"p0"}', 'state stream 13 {"p":"p1"}',
      "paint stream day:[p0]|[p1]|[b]", "paint lake day:[p0]|[p1]|[b]", "paint lake__edge day:[p0]|[p1]|[b]",
    ]);
  });

  it("INDEX WITHDRAWN (the pair no longer proven): plain, clear, restore — and no tile is labelled after", () => {
    const r = rig();
    r.setTiles([{ wetland: [10] }]);
    r.c.set(r.h, r.target, r.renderer, { index: A, day: JUN1 }, "light");
    r.take();
    r.c.set(r.h, r.target, r.renderer, null, "light");
    expect(r.take()).toEqual([
      "paint stream plain", "paint lake plain", "paint lake__edge plain",
      "clear stream", "clear lake", "clear wetland", "restore",
    ]);
    expect(r.c.active).toBe(false);
    expect(r.c.label(r.target, { stream: [10, 11] })).toBe(0);
    expect(r.take()).toEqual([]);
    // and coming back labels from scratch, with nothing left to clear
    r.c.set(r.h, r.target, r.renderer, { index: A, day: JUN1 }, "light");
    expect(r.take()).toEqual([
      "paint stream plain", "paint lake plain", "paint lake__edge plain",
      'state wetland 10 {"p":"p0"}',
      "paint stream day:[]|[p0,p1]|[b]", "paint lake day:[]|[p0,p1]|[b]", "paint lake__edge day:[]|[p0,p1]|[b]",
    ]);
  });

  it("a theme or mode change paints the day already installed — it never drops to grey or plain", () => {
    const r = rig();
    r.c.set(r.h, r.target, r.renderer, { index: A, day: JAN1 }, "light");
    r.take();
    r.adapter.setLayerMode(r.h, "stream", "plain", "light");
    r.adapter.setLayerMode(r.h, "stream", "profile", "light");
    r.adapter.applyTheme(r.h, "plain", "light");      // the view preset: every layer plain, as the app renders it
    r.adapter.setLayerMode(r.h, "lake", "profile", "light");
    expect(r.take().filter((x) => /^paint (stream|lake|lake__edge) /.test(x))).toEqual([
      "paint stream plain", "paint stream day:[p0]|[p1]|[b]",
      "paint stream plain", "paint lake plain", "paint lake__edge plain",
      "paint lake day:[p0]|[p1]|[b]", "paint lake__edge day:[p0]|[p1]|[b]",
    ]);
  });
});

describe("coloursEachTile: which renderer takes the profile mode (colour-ap review)", () => {
  it("the web map does; the native map (N2: no per-tile event, reads only pushed data) does not, nor anything unknown", () => {
    expect(coloursEachTile("web")).toBe(true);
    for (const p of ["android", "ios", "native", "windows"]) expect(coloursEachTile(p)).toBe(false);
  });
});
