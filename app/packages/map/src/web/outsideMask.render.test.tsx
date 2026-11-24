/**
 * (T1016) The outside-BC mask that will not load is reported with its status and its address: a 404 of a tile folder the
 * live release no longer names (a tab older than the release) is told from a defect by them (core `notFoundUrl`).
 */
import { afterEach, expect, it, vi } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { notFoundUrl, setFailureSink } from "@app/core";
import { useOutsideMask } from "./outsideMask";

afterEach(() => { vi.unstubAllGlobals(); });

it("a 404: reported with status 404 and the mask's url, and nothing drawn", async () => {
  const url = "https://data.test/release/tiles/t.1/bc_outside.geojson";
  vi.stubGlobal("fetch", vi.fn(async () => new Response("Not Found", { status: 404 })));
  const seen: unknown[] = [];
  const off = setFailureSink((e) => { seen.push(e); });
  try {
    const { result } = renderHook(() => useOutsideMask(url));
    await waitFor(() => expect(seen).toHaveLength(1));
    expect([(seen[0] as Error).message, (seen[0] as { status?: number }).status, notFoundUrl(seen[0])]).toEqual(["outside mask: HTTP 404", 404, url]);
    expect(result.current).toBeUndefined();
  } finally { off(); }
});
