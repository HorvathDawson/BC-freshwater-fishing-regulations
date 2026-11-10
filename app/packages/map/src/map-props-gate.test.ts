/**
 * (T621, G-gate 2) BOTH MAP HALVES HONOUR EVERY `MapProps` KEY, or the key is named here as one platform's by design (or
 * as an open gap), with the reason. A new prop that only one half reads fails this test until it is wired or listed.
 *
 * "Honours" is read from the source: the key is taken out of the props AND used in the body (not renamed to an
 * underscore name, the way a phone accepts `onRegion` and never calls it).
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const src = (f: string) => readFileSync(new URL(f, import.meta.url), "utf8");

/** The keys of `interface MapProps`, at its top level. */
function propKeys(): string[] {
  const t = src("./map-props.ts");
  const body = t.slice(t.indexOf("export interface MapProps"));
  return [...body.matchAll(/^ {2}([a-zA-Z]+)\??[:(<]/gm)].map((m) => m[1]!);
}

/** What a half's `Map` function takes out of its props, and the rest of its body. */
function half(file: string): { params: string; body: string } {
  const t = src(file);
  const end = t.indexOf("}: MapProps)");
  const start = t.lastIndexOf("function ", end);
  return { params: t.slice(start, end), body: t.slice(end) };
}

/** Honoured: destructured under its own name, and read somewhere in the body. */
function honours(h: { params: string; body: string }, key: string): boolean {
  const named = new RegExp(`[({,\\s]${key}(?=\\s*(?:[,=}]|$))`).test(h.params);
  return named && new RegExp(`\\b${key}\\b`).test(h.body);
}

/** Keys the phone does not honour, and why. */
const NATIVE_NOT: Record<string, string> = {
  onRegion: "web only by design: it feeds the web's tile prefetch; a phone holds the release installed",
  onImageryError: "web only by design: the binding has no per-tile error event; a phone learns of unreachable imagery from useImageryReach's probe",
  trace: "OPEN GAP (T608B G5): the route overlay is not drawn on a phone. Its lines are read from the atlas archive by a JS pmtiles " +
         "fetch (range requests), which cannot read the installed archive on a phone; needs a native tile reader, tested on a device",
};
/** Keys the web does not honour, and why. */
const WEB_NOT: Record<string, string> = {};

describe("(T621) both map halves honour every MapProps key", () => {
  const keys = propKeys();
  it("reads the keys of the interface", () => {
    expect(keys.length).toBeGreaterThan(40);
    expect(keys).toEqual(expect.arrayContaining(["at", "onPressFeature", "zoomControls", "trace", "pins"]));
  });
  it("the web half honours every key it does not list", () => {
    const h = half("./Map.web.tsx");
    expect(keys.filter((k) => !(k in WEB_NOT) && !honours(h, k))).toEqual([]);
  });
  it("the phone half honours every key it does not list", () => {
    const h = half("./Map.native.tsx");
    expect(keys.filter((k) => !(k in NATIVE_NOT) && !honours(h, k))).toEqual([]);
  });
  it("a listed key is really not honoured (so a fix removes its line here), and every list entry is a key", () => {
    const h = half("./Map.native.tsx");
    expect(Object.keys(NATIVE_NOT).filter((k) => honours(h, k))).toEqual([]);
    expect([...Object.keys(NATIVE_NOT), ...Object.keys(WEB_NOT)].filter((k) => !keys.includes(k))).toEqual([]);
  });
});
