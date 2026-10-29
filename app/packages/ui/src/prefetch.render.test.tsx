/**
 * (T186) THE WEB PREFETCH HOOK: nothing before the map's first region report (first paint); then the region's file at
 * once and core only when the browser says it is idle — never on Save-Data or a slow connection; a phone (null loader)
 * gets no callback at all, so its map is not asked.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render } from "@testing-library/react";
import type { BackgroundPrefetch, PrefetchSnapshot, ReleaseLoader } from "@app/data/release";
import { BACKGROUND_DELAY_MS, batteryLow, connectionSignals, useBackgroundPrefetch, useReleasePrefetch,
         webNetSignals } from "./prefetch";

afterEach(cleanup);

type G = { requestIdleCallback?: unknown; cancelIdleCallback?: unknown; navigator?: unknown };
function withGlobals(idle: (() => void)[], connection: unknown, fn: () => void) {
  const g = globalThis as unknown as G;
  const had = { r: g.requestIdleCallback, c: g.cancelIdleCallback, n: Object.getOwnPropertyDescriptor(globalThis, "navigator") };
  g.requestIdleCallback = (cb: () => void) => idle.push(cb);
  g.cancelIdleCallback = () => {};
  Object.defineProperty(globalThis, "navigator", { value: { connection }, configurable: true });
  try { fn(); } finally {
    g.requestIdleCallback = had.r; g.cancelIdleCallback = had.c;
    if (had.n) Object.defineProperty(globalThis, "navigator", had.n); else delete g.navigator;
  }
}

function fakeLoader() {
  const asked: string[] = [];
  const loader = {
    prefetch: async (n: string) => { asked.push(`fetch ${n}`); },
    prefetched: (n: string) => asked.includes(`fetch ${n}`),
    db: async (n: string) => { asked.push(`open ${n}`); return {}; },
    opened: () => asked.filter((a) => a.startsWith("open ")).map((a) => a.slice(5)),
  } as unknown as ReleaseLoader;
  return { loader, asked };
}

function Probe({ loader, out, onIdle }: { loader: ReleaseLoader | null; out: { cb?: ((r: unknown) => void) | undefined };
                onIdle?: () => void }) {
  out.cb = useReleasePrefetch(loader, onIdle);
  return null;
}

describe("useReleasePrefetch", () => {
  it("after first paint: the region's file now, core when idle; a new region's file as the view moves", () => {
    const idle: (() => void)[] = [];
    withGlobals(idle, { effectiveType: "4g", saveData: false }, () => {
      const { loader, asked } = fakeLoader(), out: { cb?: (r: unknown) => void } = {};
      render(<Probe loader={loader} out={out} />);
      expect([asked, idle.length]).toEqual([[], 0]);         // nothing before the map's first report
      out.cb!("2");
      expect([asked, idle.length]).toEqual([["fetch regs-2"], 1]);
      idle[0]!();
      expect(asked).toEqual(["fetch regs-2", "open core"]);
      out.cb!(null);                                          // over the sea: nothing new, nothing forgotten
      out.cb!("7A");
      out.cb!("2");
      expect(asked).toEqual(["fetch regs-2", "open core", "fetch regs-7a"]);
    });
  });

  for (const [what, connection] of [["Save-Data", { saveData: true, effectiveType: "4g" }],
                                    ["a 3g connection", { effectiveType: "3g" }],
                                    ["a slow-2g connection", { effectiveType: "slow-2g" }]] as const)
    it(`${what}: the region's file only, idle or not`, () => {
      const idle: (() => void)[] = [];
      withGlobals(idle, connection, () => {
        const { loader, asked } = fakeLoader(), out: { cb?: (r: unknown) => void } = {};
        render(<Probe loader={loader} out={out} />);
        out.cb!("3");
        idle[0]!();
        expect(asked).toEqual(["fetch regs-3"]);
      });
    });

  it("T182: onIdle once, after the idle prefetch started (Save-Data too) — the colourings wait for it, not for paint", () => {
    for (const connection of [{ effectiveType: "4g" }, { saveData: true }]) {
      const idle: (() => void)[] = [];
      withGlobals(idle, connection, () => {
        const { loader, asked } = fakeLoader(), out: { cb?: (r: unknown) => void } = {};
        render(<Probe loader={loader} out={out} onIdle={() => asked.push("idle")} />);
        out.cb!("2");
        expect(asked).toEqual(["fetch regs-2"]);              // first paint: not yet
        idle[0]!();
        out.cb!("3");
        expect(asked.filter((a) => a === "idle")).toEqual(["idle"]);
        expect(asked.indexOf("idle")).toBeGreaterThan(asked.indexOf("fetch regs-2"));
        if (!("saveData" in connection)) expect(asked.indexOf("idle")).toBeGreaterThan(asked.indexOf("open core"));
      });
    }
  });

  it("a phone (no loader handed): no callback, so the map is never asked", () => {
    const out: { cb?: (r: unknown) => void } = {};
    render(<Probe loader={null} out={out} />);
    expect(out.cb).toBeUndefined();
  });

  it("connectionSignals: what the browser says, and nothing said is neither Save-Data nor slow", () => {
    expect([connectionSignals(undefined), connectionSignals({}), connectionSignals({ connection: { saveData: true, effectiveType: "2g" } })])
      .toEqual([{ saveData: false, effectiveType: null }, { saveData: false, effectiveType: null },
                { saveData: true, effectiveType: "2g" }]);
  });
});

describe("(T253) the background tier", () => {
  function fakeBg() {
    const calls: string[] = [];
    const bg = {
      start: () => calls.push("start"), view: (r: readonly string[]) => calls.push(`view ${r.join()}`),
      signals: () => {}, cancel: () => calls.push("cancel"), subscribe: () => () => {},
      snapshot: () => ({}) as PrefetchSnapshot,
    } as unknown as BackgroundPrefetch;
    return { bg, calls };
  }
  function BgProbe({ loader, bg, out }: { loader: ReleaseLoader; bg: BackgroundPrefetch; out: { cb?: (r: unknown) => void } }) {
    out.cb = useReleasePrefetch(loader, undefined, bg);
    return null;
  }

  it("told each region the map settles in; started BACKGROUND_DELAY_MS after the idle tier, never before", () => {
    vi.useFakeTimers();
    try {
      const idle: (() => void)[] = [];
      withGlobals(idle, { effectiveType: "4g" }, () => {
        const { loader } = fakeLoader(), { bg, calls } = fakeBg(), out: { cb?: (r: unknown) => void } = {};
        render(<BgProbe loader={loader} bg={bg} out={out} />);
        out.cb!("2");
        expect(calls).toEqual(["view 2"]);
        vi.advanceTimersByTime(BACKGROUND_DELAY_MS * 3);
        expect(calls).toEqual(["view 2"]);                     // not idle yet: not started
        idle[0]!();
        vi.advanceTimersByTime(BACKGROUND_DELAY_MS - 1);
        expect(calls).toEqual(["view 2"]);
        vi.advanceTimersByTime(1);
        out.cb!("7A");
        expect(calls).toEqual(["view 2", "start", "view 7a"]);
      });
    } finally { vi.useRealTimers(); }
  });

  function StatusProbe({ loader, out }: { loader: ReleaseLoader | null; out: { s?: ReturnType<typeof useBackgroundPrefetch> } }) {
    out.s = useBackgroundPrefetch(loader);
    return null;
  }

  it("useBackgroundPrefetch: a scheduler for a loader; none on a phone (no loader) or with the walk's switch off", () => {
    const { loader } = fakeLoader();
    (loader as unknown as { manifest: unknown }).manifest = { files: {} };
    const out: { s?: ReturnType<typeof useBackgroundPrefetch> } = {};
    const r = render(<StatusProbe loader={loader} out={out} />);
    expect(out.s!.scheduler).not.toBeNull();
    expect(out.s!.status.state).toBe("waiting");
    r.unmount();
    expect(out.s!.scheduler!.snapshot().state).toBe("cancelled");   // the screen went: cancelled
    render(<StatusProbe loader={null} out={out} />);
    expect(out.s!.scheduler).toBeNull();
    const g = globalThis as { __releasePrefetchBackground?: boolean };
    g.__releasePrefetchBackground = false;
    try {
      render(<StatusProbe loader={loader} out={out} />);
      expect(out.s!.scheduler).toBeNull();
    } finally { delete g.__releasePrefetchBackground; }
  });

  it("webNetSignals: online, Save-Data, speed, cellular = metered (the web has no mobile-data setting), hidden", () => {
    expect(webNetSignals({} as never)).toEqual({ online: true, saveData: false, effectiveType: null, metered: false,
                                                  allowMetered: false, hidden: false });
    expect(webNetSignals({ navigator: { onLine: false, connection: { saveData: true, effectiveType: "3g", type: "cellular" } },
                           document: { visibilityState: "hidden" } } as never))
      .toEqual({ online: false, saveData: true, effectiveType: "3g", metered: true, allowMetered: false, hidden: true });
  });

  it("batteryLow: at or under 20 % and not charging", () => {
    expect([batteryLow({ charging: false, level: 0.2 }), batteryLow({ charging: true, level: 0.05 }),
            batteryLow({ charging: false, level: 0.5 })]).toEqual([true, false, false]);
  });
});
