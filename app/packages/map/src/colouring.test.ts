/**
 * THE COLOURING ENGINE'S SEQUENCES, call by call (colour-latency AP; the verifier's condition 1) — for BOTH channels:
 * the day's status (Map tab) and the Conditions standings.
 *
 * The log below is every call the engine makes on the map, in order: colour paints (summarised as `plain`, `missing`,
 * `day:<closed>|<own>|<base>` or `table:<the values>`, so a wrong or stale value reads in the string), feature-state sets
 * and clears, and the renderer's restore. Each scenario asserts the WHOLE log, because the failure being prevented is
 * an order — a clear after the new labels, values installed before the tiles are labelled, a highlight never put back,
 * a feed refresh that touches a section.
 */
import { describe, expect, it } from "vitest";
import { readingDayOf, type LakeFacts, type StatusCode, type StatusIndex, type SurveyState } from "@app/core";
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { paintFor } from "./paint";
import { Colouring, coloursEachTile, standingInput, statusInput, stockedInput, surveyInput, type LabelTarget,
         type StandingFeed,
         type TileRenderer, type TileSections } from "./colouring";
import { resolveTheme, type StandingValues } from "./style";

/** A status index with the given profile per section and code per profile per day (default: the profile's first). */
function fakeIndex(name: string, bySection: Record<number, number>,
                   codes: Record<number, StatusCode | Record<number, StatusCode>>): StatusIndex & { name: string } {
  const profiles = Object.keys(codes).length;
  const code = (p: number, on: Date): StatusCode => {
    const c = codes[p]!;
    return typeof c === "string" ? c : c[readingDayOf(on)] ?? "base";
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
                    { 0: { [readingDayOf(JAN1)]: "closed", [readingDayOf(JUN1)]: "own" }, 1: "own", 2: "outside" });
/** B, another release: the same handles numbered differently — p0 now means "closed all year". */
const B = fakeIndex("B", { 11: 0, 13: 1 }, { 0: "closed", 1: "own" });

/** Standing classes: sections 10 and 11 share class 0, 12 is class 1, 13 has none (nothing speaks for it). */
const CLASSES = { classOf: (s: number) => ({ 10: 0, 11: 0, 12: 1 } as Record<number, number>)[s] ?? null };
/** Another release's classes: 11 is class 1 now, 13 class 0. */
const CLASSES2 = { classOf: (s: number) => ({ 11: 1, 13: 0 } as Record<number, number>)[s] ?? null };
const table = (xs: number[]) => (k: number) => xs[k] ?? NaN;
const FEED1: StandingValues = { section: table([42.5, -1]), basins: { "08MF": 12 } };
const FEED2: StandingValues = { section: table([80, NaN]), basins: {} };
const standing = (labels: StandingFeed["labels"], values: StandingValues | null): StandingFeed => ({ labels, values });

const COLOUR_LAYERS = ["stream", "lake", "lake__edge", "lake_flowing", "wetland_flowing", "basin"];

function rig(modes: Record<string, string> = { stream: "profile", lake: "profile" }) {
  const log: string[] = [];
  const t = resolveTheme("light");
  const plain: Record<string, string> = {}, missing: Record<string, string> = {};
  const prop = (l: string) => (["lake", "lake_flowing", "wetland_flowing", "basin"].includes(l) ? "fill-color" : "line-color");
  for (const l of COLOUR_LAYERS) {
    plain[l] = JSON.stringify(paintFor(l, "plain", t)[prop(l)]);
    missing[l] = JSON.stringify(t["color.water.ungauged"]);
  }
  /** A colour paint, summarised: `plain`, `missing`, the day's label sets, or the table the lookup carries. */
  const describe = (layer: string, v: unknown): string => {
    const s = JSON.stringify(v);
    if (s === missing[layer]) return "missing";     // first: the field's plain IS the unmeasured grey
    if (s === plain[layer]) return "plain";
    const table = /\["literal",(\[[^\]]*\]|\{[^}]*\})\]/.exec(s);
    if (table) return `table:${table[1]!.replace(/"/g, "")}`;
    const sets = [t["color.status.closed"], t["color.status.own"], t["color.status.base"]].map((c) => {
      const re = new RegExp(`(\\[[^\\[\\]]*\\]),"${String(c)}"`);
      return (re.exec(s)?.[1] ?? "[]").replace(/"/g, "");
    });
    return `day:${sets.join("|")}`;
  };
  const h: MapHandle = {
    setVisibility: () => {},
    setPaint: (l, p, v) => {
      if (p === "line-color" || p === "fill-color") { if (COLOUR_LAYERS.includes(l)) log.push(`paint ${l} ${describe(l, v)}`); }
      else if (l === "stream-nobaseline" && p === "line-opacity") log.push(`dash ${JSON.stringify(v) === "0" ? "off" : "on"}`);
    },
    setFeatureState: (l, id, s) => log.push(`${l} ${id} ${JSON.stringify(s)}`),
    clearFeatureStates: () => {},
  };
  const target: LabelTarget = {
    setState: (l, id, s) => log.push(`state ${l} ${id} ${JSON.stringify(s)}`),
    clearAll: (l) => log.push(`clear ${l}`),
  };
  let tiles: TileSections[] = [];
  const renderer: TileRenderer = { loaded: () => tiles, restore: () => log.push("restore") };
  const adapter = baseAdapter("web");
  for (const [l, m] of Object.entries(modes)) adapter.setLayerMode(h, l, m, "light");
  const c = new Colouring(adapter);
  const take = () => log.splice(0);
  return { h, target, renderer, adapter, c, take, setTiles: (x: TileSections[]) => { tiles = x; } };
}

/** The Conditions tab: every flow layer in the `standing` mode (the lake's edge follows). */
const CONDITIONS = { stream: "standing", lake: "standing", lake_flowing: "standing", wetland_flowing: "standing",
                     basin: "standing" };

describe("the status channel: what each change does to the map", () => {
  it("with no day the profile layers are the plain map — not grey, not coloured", () => {
    const r = rig();
    expect(r.take()).toEqual(["paint stream plain", "dash off", "paint lake plain", "paint lake__edge plain"]);
  });

  it("the first index labels every loaded tile, THEN installs the day; nothing to clear", () => {
    const r = rig();
    r.take();
    r.setTiles([{ stream: [10, 11], lake: [12] }, { stream: [11, 99] }]);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: A, day: JUN1 }), "light");
    expect(r.take()).toEqual([
      "paint stream plain", "dash off", "paint lake plain", "paint lake__edge plain",
      'state stream 10 {"p":"p0"}', 'state stream 11 {"p":"p1"}', 'state lake 12 {"p":"p2"}',
      'state stream 99 {"p":"b"}',
      "paint stream day:[]|[p0,p1]|[b]", "dash off", "paint lake day:[]|[p0,p1]|[b]",
      "paint lake__edge day:[]|[p0,p1]|[b]",
    ]);
    expect(r.c.calls).toBe(4);
  });

  it("a new tile labels only the ids not labelled yet, per source-layer; a day change sets NO state", () => {
    const r = rig();
    r.setTiles([{ stream: [10] }]);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: A, day: JUN1 }), "light");
    r.take();
    expect(r.c.label(r.target, { stream: [10, 11], lake: [10] })).toBe(2);
    expect(r.take()).toEqual(['state stream 11 {"p":"p1"}', 'state lake 10 {"p":"p0"}']);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: A, day: JAN1 }), "light");
    expect(r.take()).toEqual(
      ["paint stream day:[p0]|[p1]|[b]", "dash off", "paint lake day:[p0]|[p1]|[b]", "paint lake__edge day:[p0]|[p1]|[b]"]);
    // the same reading day again (another Date, same day) is no change at all
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: A, day: new Date(2026, 0, 1, 18) }), "light");
    expect(r.take()).toEqual([]);
  });

  it("RELEASE SWAP: plain, clear all three source-layers, restore, forget, relabel, then the new day", () => {
    const r = rig();
    r.setTiles([{ stream: [10, 11] }]);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: A, day: JUN1 }), "light");
    r.take();
    r.setTiles([{ stream: [10, 11, 13] }]);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: B, day: JUN1 }), "light");
    expect(r.take()).toEqual([
      "paint stream plain", "dash off", "paint lake plain", "paint lake__edge plain",
      "clear stream", "clear lake", "clear wetland",
      "restore",
      // 10 and 11 were labelled under A and are labelled again: the de-dupe was reset with the clear
      'state stream 10 {"p":"b"}', 'state stream 11 {"p":"p0"}', 'state stream 13 {"p":"p1"}',
      "paint stream day:[p0]|[p1]|[b]", "dash off", "paint lake day:[p0]|[p1]|[b]", "paint lake__edge day:[p0]|[p1]|[b]",
    ]);
  });

  it("INDEX WITHDRAWN (the pair no longer proven): plain, clear, restore — and no tile is labelled after", () => {
    const r = rig();
    r.setTiles([{ wetland: [10] }]);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: A, day: JUN1 }), "light");
    r.take();
    r.c.set(r.h, r.target, r.renderer, "status", null, "light");
    expect(r.take()).toEqual([
      "paint stream plain", "dash off", "paint lake plain", "paint lake__edge plain",
      "clear stream", "clear lake", "clear wetland", "restore",
    ]);
    expect(r.c.active).toBe(false);
    expect(r.c.label(r.target, { stream: [10, 11] })).toBe(0);
    expect(r.take()).toEqual([]);
    // and coming back labels from scratch, with nothing left to clear
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: A, day: JUN1 }), "light");
    expect(r.take()).toEqual([
      "paint stream plain", "dash off", "paint lake plain", "paint lake__edge plain",
      'state wetland 10 {"p":"p0"}',
      "paint stream day:[]|[p0,p1]|[b]", "dash off", "paint lake day:[]|[p0,p1]|[b]", "paint lake__edge day:[]|[p0,p1]|[b]",
    ]);
  });

  it("a theme or mode change paints the day already installed — it never drops to grey or plain", () => {
    const r = rig();
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: A, day: JAN1 }), "light");
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

describe("the standing channel: the same engine, the Conditions map's values", () => {
  /** The calls a standing change makes: feature-state only — a paint here would re-lay out every loaded tile. */
  const states = (log: string[]) => log.filter((x) => !x.startsWith("paint ") && !x.startsWith("dash "));

  it("the first values label every loaded tile through each section's class (none for a section with no class)", () => {
    const r = rig(CONDITIONS);
    r.take();
    r.setTiles([{ stream: [10, 11, 13], lake: [12] }]);
    r.c.set(r.h, r.target, r.renderer, "standing", standingInput(standing(CLASSES, FEED1)), "light");
    expect(r.take()).toEqual([
      'state stream 10 {"standing":42.5}', 'state stream 11 {"standing":42.5}', 'state lake 12 {"standing":-1}',
      'basin 08MF {"standing":12}',
    ]);
  });

  it("a stream's polygon (a slough, a canal: the `lake` source-layer) wears its line's standing — the class decides, not the layer", () => {
    // OWNER 2026-10-22 (late): Nicomen Slough's polygon 1265712 sits between its lines 59621 and 59622; the bundle gives
    // the polygon its line's panel (build_panels.stream_polygons), so the two share a class and the engine — the ONE
    // colouring path — labels them alike, whichever source-layer the feature is in.
    const SLOUGH = { classOf: (s: number) => ({ 59621: 0, 1265712: 0, 59622: 1 } as Record<number, number>)[s] ?? null };
    const r = rig(CONDITIONS);
    r.take();
    r.setTiles([{ stream: [59621, 59622], lake: [1265712] }]);
    r.c.set(r.h, r.target, r.renderer, "standing", standingInput(standing(SLOUGH, FEED1)), "light");
    expect(r.take()).toEqual([
      'state stream 59621 {"standing":42.5}', 'state stream 59622 {"standing":-1}',
      'state lake 1265712 {"standing":42.5}', 'basin 08MF {"standing":12}',
    ]);
  });

  it("before the feed answers nothing is labelled; when it does, the loaded tiles are labelled", () => {
    const r = rig(CONDITIONS);
    r.take();
    r.setTiles([{ stream: [10, 12] }]);
    r.c.set(r.h, r.target, r.renderer, "standing", standingInput(standing(CLASSES, null)), "light");
    expect(r.take()).toEqual([]);
    expect(r.c.label(r.target, { stream: [11] })).toBe(0);
    r.c.set(r.h, r.target, r.renderer, "standing", standingInput(standing(CLASSES, FEED1)), "light");
    expect(r.take()).toEqual(['state stream 10 {"standing":42.5}', 'state stream 12 {"standing":-1}',
                              'basin 08MF {"standing":12}']);
  });

  it("A NEW FEED ALONE (same release, same tiles): the loaded sections relabelled — no paint, no clear, no restore", () => {
    const r = rig(CONDITIONS);
    r.setTiles([{ stream: [10, 11, 13], lake: [12] }]);
    r.c.set(r.h, r.target, r.renderer, "standing", standingInput(standing(CLASSES, FEED1)), "light");
    r.take();
    r.c.set(r.h, r.target, r.renderer, "standing", standingInput(standing(CLASSES, FEED2)), "light");
    expect(r.take()).toEqual([
      // a class with nothing to say today CLEARS the value it had (null: `missing`); a basin that lost its value too
      'state stream 10 {"standing":80}', 'state stream 11 {"standing":80}', 'state lake 12 {"standing":null}',
      'basin 08MF {"standing":null}',
    ]);
    // a tile arriving after is labelled from the new values
    expect(r.c.label(r.target, { lake: [10] })).toBe(1);
    expect(r.take()).toEqual(['state lake 10 {"standing":80}']);
    // the same values object again (a re-render, a poll that brought the same index): nothing at all
    r.c.set(r.h, r.target, r.renderer, "standing", standingInput(standing(CLASSES, FEED2)), "light");
    expect(r.take()).toEqual([]);
  });

  it("BOTH CHANNELS: one state call per section carries every label it lacks; a standing swap keeps the status", () => {
    const r = rig({ stream: "profile", lake: "profile" });
    r.setTiles([{ stream: [10, 11] }]);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: A, day: JUN1 }), "light");
    r.c.set(r.h, r.target, r.renderer, "standing", standingInput(standing(CLASSES, FEED1)), "light");
    r.take();
    expect(r.c.label(r.target, { stream: [12, 13] })).toBe(2);
    expect(r.take()).toEqual(['state stream 12 {"p":"p2","standing":-1}', 'state stream 13 {"p":"b"}']);
    // another release's classes: clear, restore, and every channel relabelled — the status too, which the clear took
    r.setTiles([{ stream: [10, 11, 12, 13] }]);
    r.c.set(r.h, r.target, r.renderer, "standing", standingInput(standing(CLASSES2, FEED1)), "light");
    expect(states(r.take())).toEqual([
      'basin 08MF {"standing":null}',
      "clear stream", "clear lake", "clear wetland", "restore",
      'state stream 10 {"p":"p0"}', 'state stream 11 {"p":"p1","standing":-1}', 'state stream 12 {"p":"p2"}',
      'state stream 13 {"p":"b","standing":42.5}',
      'basin 08MF {"standing":12}',
    ]);
  });

  it("STANDINGS WITHDRAWN (the pair no longer proven): the field cleared, clear, restore — the status put back", () => {
    const r = rig(CONDITIONS);
    r.setTiles([{ stream: [10] }]);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: A, day: JUN1 }), "light");
    r.c.set(r.h, r.target, r.renderer, "standing", standingInput(standing(CLASSES, FEED1)), "light");
    r.take();
    r.c.set(r.h, r.target, r.renderer, "standing", null, "light");
    expect(states(r.take())).toEqual([
      'basin 08MF {"standing":null}', "clear stream", "clear lake", "clear wetland", "restore",
      'state stream 10 {"p":"p0"}',
    ]);
  });

  it("the stream's no-baseline dash follows the stream's mode: on in standing, off in every other", () => {
    const r = rig({ stream: "profile" });
    r.take();
    r.adapter.setLayerMode(r.h, "stream", "standing", "light");
    r.adapter.setLayerMode(r.h, "stream", "plain", "light");
    expect(r.take().filter((x) => x.startsWith("dash"))).toEqual(["dash on", "dash off"]);
  });
});

describe("the lake channels: survey and stocked, the Depth and Stocked modes' values (owner 2026-10-21)", () => {
  const TODAY = 46314;                 // 2026-10-21, days since 1900-01-01
  const lakeFacts = (name: string, survey: Record<number, SurveyState>, stocked: Record<number, number>): LakeFacts =>
    ({ handles: name, asof: TODAY, sections: 3, surveyed: Object.keys(survey).length,
       stocked: Object.keys(stocked).length, survey: (s) => survey[s] ?? null, stockedDay: (s) => stocked[s] ?? null });
  /** 12 a sheet stocked 30 days ago, 14 digitised and never stocked, 15 stocked 400 days ago; 16 nothing. */
  const F1 = lakeFacts("F1", { 12: "sheet", 14: "digitised" }, { 12: TODAY - 30, 15: TODAY - 400 });
  /** The next release: 15 has been surveyed. */
  const F2 = lakeFacts("F2", { 12: "sheet", 14: "digitised", 15: "sheet" }, { 12: TODAY - 30, 15: TODAY - 400 });
  const lakes = (facts: LakeFacts, day = TODAY) => ({ facts, day });

  it("each labels the loaded lakes with its own key — and paints NOTHING: the values are the labels", () => {
    const r = rig({ lake: "stocked" });
    r.take();
    r.setTiles([{ stream: [10, 11], lake: [12, 14, 15, 16] }]);
    r.c.set(r.h, r.target, r.renderer, "survey", surveyInput(lakes(F1)), "light");
    r.c.set(r.h, r.target, r.renderer, "stocked", stockedInput(lakes(F1)), "light");
    expect(r.take()).toEqual([
      'state lake 12 {"survey":"sheet"}', 'state lake 14 {"survey":"digitised"}',
      'state lake 12 {"stocked":"season"}', 'state lake 15 {"stocked":"recent"}',
    ]);
    // a tile arriving after: ONE call per lake, carrying both keys
    expect(r.c.label(r.target, { lake: [22, 12] })).toBe(0);
    const r2 = rig({ lake: "surveyed" });
    r2.c.set(r2.h, r2.target, r2.renderer, "survey", surveyInput(lakes(F1)), "light");
    r2.c.set(r2.h, r2.target, r2.renderer, "stocked", stockedInput(lakes(F1)), "light");
    r2.take();
    expect(r2.c.label(r2.target, { lake: [12, 14, 15, 16] })).toBe(3);
    expect(r2.take()).toEqual(['state lake 12 {"survey":"sheet","stocked":"season"}',
                               'state lake 14 {"survey":"digitised"}', 'state lake 15 {"stocked":"recent"}']);
  });

  it("A NEW DAY: the stocked bands relabelled on the loaded lakes — feature-state only; the survey untouched", () => {
    const r = rig({ lake: "stocked" });
    r.setTiles([{ lake: [12, 14, 15] }]);
    r.c.set(r.h, r.target, r.renderer, "survey", surveyInput(lakes(F1)), "light");
    r.c.set(r.h, r.target, r.renderer, "stocked", stockedInput(lakes(F1)), "light");
    r.take();
    r.c.set(r.h, r.target, r.renderer, "survey", surveyInput(lakes(F1, TODAY + 100)), "light");
    r.c.set(r.h, r.target, r.renderer, "stocked", stockedInput(lakes(F1, TODAY + 100)), "light");
    expect(r.take()).toEqual(['state lake 12 {"stocked":"year"}', 'state lake 15 {"stocked":"recent"}']);
    r.c.set(r.h, r.target, r.renderer, "stocked", stockedInput(lakes(F1, TODAY + 100)), "light");
    expect(r.take()).toEqual([]);
  });

  it("A NEW RELEASE ALONE (same tiles): the lakes relabelled from it — no paint; the status put back", () => {
    const r = rig({ stream: "profile", lake: "stocked" });
    r.setTiles([{ stream: [10], lake: [12, 15] }]);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: A, day: JUN1 }), "light");
    r.c.set(r.h, r.target, r.renderer, "survey", surveyInput(lakes(F1)), "light");
    r.c.set(r.h, r.target, r.renderer, "stocked", stockedInput(lakes(F1)), "light");
    r.take();
    r.c.set(r.h, r.target, r.renderer, "survey", surveyInput(lakes(F2)), "light");
    expect(r.take()).toEqual([
      "clear stream", "clear lake", "clear wetland", "restore",
      'state stream 10 {"p":"p0"}', 'state lake 12 {"p":"p2","stocked":"season","survey":"sheet"}',
      'state lake 15 {"p":"b","stocked":"recent","survey":"sheet"}',
    ]);
  });

  it("WITHDRAWN (the pair no longer proven): cleared and restored — the lakes wear nothing, so the modes paint `missing`", () => {
    const r = rig({ lake: "surveyed" });
    r.setTiles([{ lake: [12] }]);
    r.c.set(r.h, r.target, r.renderer, "survey", surveyInput(lakes(F1)), "light");
    r.take();
    r.c.set(r.h, r.target, r.renderer, "survey", null, "light");
    expect(r.take()).toEqual(["clear stream", "clear lake", "clear wetland", "restore"]);
    expect(r.c.active).toBe(false);
  });
});

describe("coloursEachTile: which renderer takes the per-tile modes (colour-ap review)", () => {
  it("the web map does; the native map (N2: no per-tile event, reads only pushed data) does not, nor anything unknown", () => {
    expect(coloursEachTile("web")).toBe(true);
    for (const p of ["android", "ios", "native", "windows"]) expect(coloursEachTile(p)).toBe(false);
  });
});
