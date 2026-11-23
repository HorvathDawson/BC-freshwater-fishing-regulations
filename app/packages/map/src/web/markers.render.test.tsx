/**
 * (T1005, crash kind c77d892e10eefbd1, web 0.8.5) THE MARKS' CLEANUP NEVER TOUCHES A REMOVED MAP. A release applied in the
 * session changes the archives, `useMapInstance` builds a new map and removes the old one; the marks' effect is re-run at
 * the next render and its cleanup ran on the REMOVED map, whose `getSource` reads a style that is gone ("Cannot read
 * properties of null"). On the map still in use, a cleanup still clears the marks' source.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, renderHook } from "@testing-library/react";
import type { MapProps } from "../map-props";
import type { WebMapRefs } from "./refs";
import { useMarks } from "./markers";

const fakeMap = () => {
  const cleared: unknown[] = [];
  const m = {
    removed: false, cleared,
    on: () => m, off: () => m, once: () => m,
    getLayer: () => undefined,
    getSource: () => {
      if (m.removed) throw new TypeError("Cannot read properties of null (reading 'getSource')");
      return { setData: (d: unknown) => { cleared.push(d); } };
    },
  };
  return m;
};
const furniture = { ink: "ink", card: "card" } as unknown as Parameters<typeof useMarks>[3];
const mark = (lon: number) => ({ lon, lat: 49, open: true, label: null }) as unknown as NonNullable<MapProps["marks"]>[number];

afterEach(cleanup);

describe("T1005 useMarks cleanup", () => {
  it("does not touch the map a rebuild removed", () => {
    const first = fakeMap();
    const refs = { map: { current: first } } as unknown as WebMapRefs;
    const { rerender } = renderHook(({ k }: { k: MapProps["marks"] }) => useMarks(refs, k, undefined, furniture),
                                    { initialProps: { k: [mark(-121)] } });
    // the archives changed: the map was rebuilt and the old one removed (useMapInstance's cleanup)
    first.removed = true;
    (refs.map as { current: unknown }).current = fakeMap();
    expect(() => rerender({ k: [mark(-121)] })).not.toThrow();
  });

  it("clears the marks' source on the map still in use", () => {
    const m = fakeMap();
    const refs = { map: { current: m } } as unknown as WebMapRefs;
    const { rerender } = renderHook(({ k }: { k: MapProps["marks"] }) => useMarks(refs, k, undefined, furniture),
                                    { initialProps: { k: [mark(-121)] } });
    rerender({ k: [mark(-122)] });
    expect(m.cleared).toHaveLength(1);
  });
});
