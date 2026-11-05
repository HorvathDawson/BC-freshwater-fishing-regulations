/**
 * (T287) An applied update never leaves the map uncoloured for a frame: the pairing of a manifest that names its tiles is
 * answered in the very render the release changes, and the day's status index of a release in the same section
 * numbering stays on screen until the new one is in.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { parseManifest, type Held, type Manifest } from "@app/data";
import type { StatusIndex } from "@app/core";
import { releaseStatus, usePairing, useStatusIndex, type PairingInput } from "./release";

afterEach(cleanup);

const H = "d768d6e135e62b99";
const M = (release: string): Manifest => parseManifest({
  format: "app-manifest/2", data_release: release,
  tiles: { compatible: [`${H}.*`], assets: null, mask: null,
           current: { id: `${H}.aaa`, url: `tiles/a/atlas-a.pmtiles`, bytes: 1, sha256: "x" } },
  basemap: null, files: {}, regulations: { valid_until: null, versions: null },
});
const held: Held = { tiles: `${H}.aaa`, basemap: null, release: "r-1", files: {} };
const input = (release: string): PairingInput => ({ kind: "release", manifest: M(release), held });

describe("usePairing across an applied update", () => {
  it("is proven in every render: never the not-established answer between two releases", () => {
    const seen: (boolean | null)[] = [];
    const h = renderHook(({ i }) => { const v = usePairing(i); seen.push(v.ok); return v; }, { initialProps: { i: input("r-1") } });
    h.rerender({ i: input("r-2") });
    expect(h.result.current).toMatchObject({ ok: true, data: "r-2" });
    expect(seen.every((ok) => ok === true)).toBe(true);
  });
});

describe("useStatusIndex across an applied update", () => {
  const A = { day: "a" } as unknown as StatusIndex, B = { day: "b" } as unknown as StatusIndex;
  const loader = (release: string, handles: string | undefined, index: StatusIndex, gate: Promise<void>) =>
    ({ release, handles, paint: async () => { await gate; return { status: index }; } });

  it("the same numbering: the old index stays until the new one is in (no null frame)", async () => {
    let open!: () => void;
    const gate = new Promise<void>((r) => { open = r; });
    const seen: (StatusIndex | null)[] = [];
    const h = renderHook(({ s }) => { const v = useStatusIndex(s); seen.push(v); return v; },
                         { initialProps: { s: releaseStatus(loader("t287-1", H, A, Promise.resolve())) } });
    await waitFor(() => expect(h.result.current).toBe(A));
    seen.length = 0;
    h.rerender({ s: releaseStatus(loader("t287-2", H, B, gate)) });
    expect(h.result.current).toBe(A);
    await act(async () => { open(); await gate; });
    await waitFor(() => expect(h.result.current).toBe(B));
    expect(seen.includes(null)).toBe(false);
  });
  it("another numbering is a new question: nothing drawn until the new index is in", async () => {
    let open!: () => void;
    const gate = new Promise<void>((r) => { open = r; });
    const h = renderHook(({ s }) => useStatusIndex(s),
                         { initialProps: { s: releaseStatus(loader("t287-3", H, A, Promise.resolve())) } });
    await waitFor(() => expect(h.result.current).toBe(A));
    h.rerender({ s: releaseStatus(loader("t287-4", "0123456789abcdef", B, gate)) });
    await waitFor(() => expect(h.result.current).toBeNull());
    await act(async () => { open(); await gate; });
    await waitFor(() => expect(h.result.current).toBe(B));
  });
});
