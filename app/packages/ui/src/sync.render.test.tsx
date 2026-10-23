/**
 * The sync state carries HYDAT's notice off the SAME feed-index read the sync icon makes (owner 2026-10-22 late:
 * "similar to bundle changes where the app checks when in service for updates") — re-read on the poll and on a tap,
 * so a notice the publisher writes reaches the reader with no app change. Mounted (jsdom) because it is a hook.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { GaugeFeed } from "@app/data";
import type { HydatNotice } from "@app/core";
import { useSync, type SyncState } from "./sync";

afterEach(cleanup);

const DUE = { kind: "hydat", since: "2026-10-22T21:00:00Z", current: "20260717", latest: "20261020",
              action: "scripts/update-hydat.sh" };

function feedOf(index: () => unknown): GaugeFeed {
  return { index: async () => index(), refresh: () => {} } as unknown as GaugeFeed;
}

function Probe({ feed, out }: { feed: GaugeFeed; out: { s?: SyncState } }) {
  out.s = useSync({ feed, release: null });
  return null;
}

const settle = () => act(async () => { for (let i = 0; i < 5; i++) await Promise.resolve(); });

describe("useSync: HYDAT's notice rides the feed-index read", () => {
  it("due, current, an index from before notices, and no feed answer", async () => {
    const got: (HydatNotice | null)[] = [];
    for (const idx of [
      { fetchedAt: "2026-10-22T21:00:00Z", stations: {}, hydat: { release: "20260717", latest: "20261020", stale: true },
        maintenance: [DUE] },
      { fetchedAt: "2026-10-22T21:00:00Z", stations: {}, hydat: { release: "20260717", latest: "20260717", stale: false },
        maintenance: [] },
      { fetchedAt: "2026-10-22T21:00:00Z", stations: {}, hydat: { release: "20260717", latest: "20261020", stale: true } },
      null,
    ]) {
      const out: { s?: SyncState } = {};
      render(<Probe feed={feedOf(() => idx)} out={out} />);
      await settle();
      got.push(out.s!.hydatNotice);
      cleanup();
    }
    expect(got).toEqual(["due", { current: "2026-07-17" }, null, null]);
  });

  it("a tap re-reads it: the notice clears once the publisher has caught up", async () => {
    let idx: unknown = { fetchedAt: "2026-10-22T21:00:00Z", stations: {},
                         hydat: { release: "20260717", latest: "20261020", stale: true }, maintenance: [DUE] };
    const out: { s?: SyncState } = {};
    render(<Probe feed={feedOf(() => idx)} out={out} />);
    await settle();
    const before = out.s!.hydatNotice;
    idx = { fetchedAt: "2026-10-22T22:00:00Z", stations: {},
            hydat: { release: "20261020", latest: "20261020", stale: false }, maintenance: [] };
    act(() => out.s!.refresh());
    // a tap shows "checking" for at least SYNC_MIN_CHECK_MS (600 ms)
    await act(async () => { await new Promise((r) => setTimeout(r, 700)); });
    expect([before, out.s!.hydatNotice, out.s!.newRelease]).toEqual(["due", { current: "2026-10-20" }, null]);
  });
});

describe("useSync: (T68) the newest forecast run's issue time rides the same read", () => {
  it("a stamp, none held (null), an index from before the field (undefined), and no feed answer (undefined)", async () => {
    const got: (string | null | undefined)[] = [];
    for (const idx of [
      { fetchedAt: "2026-10-22T21:00:00Z", stations: {}, forecastIssuedAt: "2026-10-22T12:53:00" },
      { fetchedAt: "2026-10-22T21:00:00Z", stations: {}, forecastIssuedAt: null },
      { fetchedAt: "2026-10-22T21:00:00Z", stations: {} },
      null,
    ]) {
      const out: { s?: SyncState } = {};
      render(<Probe feed={feedOf(() => idx)} out={out} />);
      await settle();
      got.push(out.s!.forecastIssuedAt);
      cleanup();
    }
    expect(got).toEqual(["2026-10-22T12:53:00", null, undefined, undefined]);
  });
});
