/**
 * (T238, owner 2026-10-29: regulations never per region) THE OFFLINE MAPS PAGE, drawn: on the web over the real release
 * source and a Cache Storage stand-in — the regulations' fixed row with what the browser holds, no Save / Remove; on a
 * phone the regulations always on it, the regions part of the province map, the release download's progress and Resume, the Wi-Fi-only switch (T231's setting,
 * inverted). Every press named and reaching 44 × 44 (the T40 audits).
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
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

describe("the web: the regulations kept so far, never an action", () => {
  it("the fixed first row says all of B.C., always offline, and how much this browser holds; no Save / Remove anywhere", async () => {
    const L = await webLoader();
    await L.prefetch("regs-2");
    render(<Harness loader={L} open platform="web" />);
    await waitFor(() => expect(text("offline-total")).toBe("Storage used0.3 MB"));
    expect([text("offline-state-regs"), text("offline-state-map"), text("offline-state-5")]).toEqual([
      "Always available offline · 0.3 MB of 0.9 MB kept in this browser so far", "Read online as you move the map",
      "Part of the province map"]);
    expect(screen.getByTestId("offline-row-regs").getAttribute("aria-label"))
      .toBe("Regulations: all of B.C.: 0.9 MB. Always available offline · 0.3 MB of 0.9 MB kept in this browser so far.");
    expect(screen.queryAllByRole("button")).toEqual([]);
    expect(pressAudit(screen.getByTestId("offline-page")).bad).toEqual([]);
  });
});

describe("the phone: the installed set", () => {
  const snap = (release: UpdateSnapshot["release"], allowMetered = false): UpdateSnapshot =>
    ({ checking: false, manual: null, feeds: [], release, allowMetered, mobileData: allowMetered ? "always" : "ask",
       mobileDataAnswered: allowMetered });
  const status = { id: "release" as const, checkedAt: null, updatedAt: null, error: null, failedAt: null, offline: false,
                   failures: 0, target: "r-2", targetDate: null };

  it("every region on the phone, no actions; storage is the whole set; the Wi-Fi switch is the mobile-data setting inverted", async () => {
    const set: boolean[] = [];
    render(<Harness loader={await webLoader()} open platform="phone" updates={snap(null)} check={() => {}}
                    allowMetered={(on) => set.push(on)} />);
    expect([text("offline-total"), text("offline-state-regs"), text("offline-state-2"), screen.queryAllByRole("button")])
      .toEqual(["Storage used459.9 MB", "Always available offline", "Part of the province map · on this phone", []]);
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
