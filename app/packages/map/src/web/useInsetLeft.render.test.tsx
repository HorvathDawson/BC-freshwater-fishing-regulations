/**
 * (T856) THE FLOATING RAIL'S LEFT INSET IS ON EVERY MAP BUILT: `useMapInstance` builds a NEW map when the archives
 * change (the release's tiles replacing the dev ones), and a new map starts with no padding. The inset hook is keyed on
 * the same archives, so the rebuilt map gets the left inset again without the inset itself changing.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, renderHook } from "@testing-library/react";
import type { MapProps } from "../map-props";
import type { WebMapRefs } from "./refs";
import { useInsetLeft } from "./useInsetLeft";

type Padding = { top: number; bottom: number; left: number; right: number };
const fakeMap = () => {
  let pad: Padding = { top: 0, bottom: 0, left: 0, right: 0 };
  return { getPadding: () => pad, setPadding: (p: Padding) => { pad = p; } };
};
const at = (atlas: string) => ({ atlas, basemap: "b", glyphs: "g", sprite: "s", assets: "a" }) as unknown as MapProps["at"];

afterEach(cleanup);

describe("T856 useInsetLeft", () => {
  it("puts the left inset on the map, and again on a map rebuilt for new archives", () => {
    const first = fakeMap();
    const refs = { map: { current: first } } as unknown as WebMapRefs;
    const { rerender } = renderHook(({ a }: { a: MapProps["at"] }) => useInsetLeft(refs, 92, a), { initialProps: { a: at("dev") } });
    expect(first.getPadding().left).toBe(92);
    // the archives change: useMapInstance (declared before this hook) has swapped in a new map, with no padding
    const second = fakeMap();
    (refs.map as { current: unknown }).current = second;
    rerender({ a: at("release") });
    expect(second.getPadding().left).toBe(92);
  });

  it("leaves the other sides' padding as they were", () => {
    const m = fakeMap();
    m.setPadding({ top: 40, bottom: 120, left: 0, right: 0 });
    const refs = { map: { current: m } } as unknown as WebMapRefs;
    renderHook(() => useInsetLeft(refs, 92, at("dev")));
    expect(m.getPadding()).toEqual({ top: 40, bottom: 120, left: 92, right: 0 });
  });
});
