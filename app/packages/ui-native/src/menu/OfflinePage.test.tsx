/**
 * (T238) THE OFFLINE MAPS PAGE, drawn: on the web over the real release source and a Cache Storage stand-in — Save a
 * region (the shared data saved with it, storage used grows), Remove it, Remove everything; on a phone every region "On
 * this phone", no per-region actions, the release download's progress and Resume, the Wi-Fi-only switch (T231's setting,
 * inverted). Every press named and reaching 44 × 44 (the T40 audits).
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { parseManifest, sha256Hex, type UpdateSnapshot } from "@app/data";
import { releaseLoader, type ReleaseLoader } from "@app/data/release";
import { useOfflineMaps } from "@app/ui";
import { releaseCache, releaseSource } from "@app/data/release/platform";
import { LIGHT } from "../theme";
import { pressAudit } from "../regs/testing/a11yAudit";
import { OfflinePage } from "./OfflinePage";

const enc = (s: string) => new TextEncoder().encode(s);
const H = "d768d6e135e62b99";
const BODY: Record<string, string> = { core: "c".repeat(400_000), "regs-2": "r".repeat(300_000), "regs-5": "f".repeat(200_000) };
const MANIFEST = parseManifest({
  format: "app-manifest/2", data_release: "r-1",
  tiles: { compatible: [`${H}.*`], current: { id: `${H}.1`, url: "tiles/a.pmtiles", bytes: 185_000_000, sha256: "x" },
           assets: null, mask: null },
  basemap: { id: "b", url: "basemap/b.pmtiles", bytes: 274_000_000, sha256: "z" },
  files: Object.fromEntries(Object.entries(BODY).map(([k, body]) => [k, {
    url: `r-1/${k}-x.bin.gz`, bytes: enc(body).length, gz: 0, sha256: sha256Hex(enc(body)), format: `${k}/1`,
    section_handles: H }])),
  regulations: { valid_until: null, versions: null },
});

const store = new Map<string, Response>();
const g = globalThis as { caches?: unknown };
let had: unknown;
beforeEach(() => {
  store.clear();
  had = g.caches;
  g.caches = { open: async () => ({ match: async (u: string) => store.get(u)?.clone(),
                                    put: async (u: string, r: Response) => { store.set(u, r); },
                                    delete: async (u: string) => store.delete(u) }) };
});
afterEach(() => { cleanup(); g.caches = had; });

async function webLoader(): Promise<ReleaseLoader> {
  const fetchImpl = (async (url: string) => new Response(enc(BODY[url.replace(/^.*\/r-1\//, "").replace(/-x\.bin\.gz$/, "")]!))) as typeof fetch;
  return releaseLoader(MANIFEST, releaseSource(MANIFEST, { base: "https://r2.test/", cache: await releaseCache(), held: null,
                                                           fetch: fetchImpl }), async () => { throw new Error("no"); });
}

function Harness(p: Parameters<typeof useOfflineMaps>[0]) {
  const c = useOfflineMaps(p);
  return c ? <OfflinePage c={c} palette={LIGHT} /> : null;
}
const text = (id: string) => screen.getByTestId(id).textContent;

describe("the web: the release cache, region by region", () => {
  it("Save Region 2: saved with the shared data; storage used grows; Remove takes it and the shared with it", async () => {
    render(<Harness loader={await webLoader()} open platform="web" />);
    await waitFor(() => expect(screen.getByLabelText("Save Region 2 · Lower Mainland")).toBeTruthy());
    expect([text("offline-total"), text("offline-state-2"), text("offline-state-map")])
      .toEqual(["Storage used0.0 MB", "0.3 MB · Not saved", "459.0 MB · Read online as you move the map"]);
    expect(pressAudit(screen.getByTestId("offline-page")).bad).toEqual([]);
    await act(async () => { fireEvent.click(screen.getByLabelText("Save Region 2 · Lower Mainland")); });
    await waitFor(() => expect(text("offline-state-2")).toBe("0.3 MB · Saved"));
    expect([text("offline-total"), text("offline-state-shared"), text("offline-state-5")])
      .toEqual(["Storage used0.7 MB", "0.4 MB · Saved", "0.2 MB · Not saved"]);
    expect(screen.getByTestId("offline-clear")).toBeTruthy();
    await act(async () => { fireEvent.click(screen.getByLabelText("Remove Region 2 · Lower Mainland")); });
    await waitFor(() => expect(text("offline-state-2")).toBe("0.3 MB · Not saved"));
    expect([text("offline-total"), store.size]).toEqual(["Storage used0.0 MB", 0]);
  });

  it("Remove everything saved drops every file", async () => {
    render(<Harness loader={await webLoader()} open platform="web" />);
    await waitFor(() => screen.getByLabelText("Save Region 5 · Cariboo"));
    await act(async () => { fireEvent.click(screen.getByLabelText("Save Region 5 · Cariboo")); });
    await waitFor(() => expect(text("offline-state-5")).toBe("0.2 MB · Saved"));
    await act(async () => { fireEvent.click(screen.getByTestId("offline-clear")); });
    await waitFor(() => expect(store.size).toBe(0));
    expect(screen.queryByTestId("offline-clear")).toBeNull();
  });
});

describe("the phone: the installed set", () => {
  const snap = (release: UpdateSnapshot["release"], allowMetered = false): UpdateSnapshot =>
    ({ checking: false, manual: null, feeds: [], release, allowMetered });
  const status = { id: "release" as const, checkedAt: null, updatedAt: null, error: null, failedAt: null, offline: false,
                   failures: 0, target: "r-2", targetDate: null };

  it("every region on the phone, no actions; storage is the whole set; the Wi-Fi switch is the mobile-data setting inverted", async () => {
    const set: boolean[] = [];
    render(<Harness loader={await webLoader()} open platform="phone" updates={snap(null)} check={() => {}}
                    allowMetered={(on) => set.push(on)} />);
    expect([text("offline-total"), text("offline-state-2"), screen.queryByLabelText(/^Save /)])
      .toEqual(["Storage used459.9 MB", "0.3 MB · On this phone", null]);
    const sw = screen.getByTestId("offline-wifi");
    expect([sw.getAttribute("role"), sw.getAttribute("aria-checked")]).toEqual(["switch", "true"]);
    fireEvent.click(sw);
    expect(set).toEqual([true]);
    expect(pressAudit(screen.getByTestId("offline-page")).bad).toEqual([]);
  });

  it("a download in progress shows its progress; one waiting for Wi-Fi offers Resume now (a check)", async () => {
    const L = await webLoader();
    const { rerender } = render(<Harness loader={L} open platform="phone" check={() => {}} allowMetered={() => {}}
      updates={snap({ ...status, phase: "downloading", progress: { bytes: 1, of: 4 } })} />);
    expect(text("offline-update")).toBe("Downloading new map data · 25%");
    expect(screen.getByRole("progressbar").getAttribute("aria-valuenow")).toBe("25");
    let checked = 0;
    rerender(<Harness loader={L} open platform="phone" check={() => { checked++; }} allowMetered={() => {}}
      updates={snap({ ...status, phase: "waiting-wifi", progress: null })} />);
    fireEvent.click(screen.getByLabelText("Resume now"));
    expect(checked).toBe(1);
  });
});
