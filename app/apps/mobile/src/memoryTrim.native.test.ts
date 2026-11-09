/**
 * (T360) The phone's answer to a trim: the databases' page caches are shrunk FIRST, then the connections idle a minute are
 * closed (a close before the shrink would shrink nothing); a failure of either reaches the caller's `failed`.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ReleaseLoader } from "@app/data/release";

type Listener = (e: { level: number; maps: number; purged: number }) => void;
const listeners: Listener[] = [];
vi.mock("expo", () => ({
  requireOptionalNativeModule: () => ({
    addListener: (_: "trim", f: Listener) => { listeners.push(f); return { remove: () => listeners.splice(listeners.indexOf(f), 1) }; },
  }),
}));
vi.mock("react-native", () => ({ AppState: { addEventListener: vi.fn() }, Platform: { OS: "android" } }));

import { trimOnPressure } from "./memoryTrim.native";

/** memoryTrim.native.ts `IDLE_MS`: kept unexported, as the web twin has none (check-platform). */
const IDLE_MS = 60_000;
const flush = () => new Promise((r) => setTimeout(r, 0));

describe("trimOnPressure (native)", () => {
  beforeEach(() => { listeners.length = 0; vi.spyOn(console, "info").mockImplementation(() => {}); });

  it("shrinks, then closes what has been idle a minute; stops listening when told", async () => {
    const calls: string[] = [];
    const loader = {
      trim: vi.fn(async () => { await flush(); calls.push("trim"); return 3; }),
      closeIdle: vi.fn(async (ms: number) => { calls.push(`closeIdle ${ms}`); return 2; }),
    } as unknown as ReleaseLoader;
    const failed = vi.fn();
    const stop = trimOnPressure(loader, failed);
    listeners[0]!({ level: 15, maps: 1, purged: 2 });
    await flush(); await flush();
    expect([calls, failed.mock.calls]).toEqual([["trim", `closeIdle ${IDLE_MS}`], []]);
    expect(console.info).toHaveBeenCalledWith("[trim] level 15: 1 map(s), purge 2, 3 database(s) shrunk, 2 idle closed");
    stop();
    expect(listeners).toEqual([]);
  });

  it("a failed shrink is reported and nothing is closed; a failed close is reported", async () => {
    const closeIdle = vi.fn(async () => 0);
    const failed = vi.fn();
    trimOnPressure({ trim: async () => { throw new Error("core: shrink"); }, closeIdle } as unknown as ReleaseLoader, failed);
    listeners[0]!({ level: 15, maps: 0, purged: 2 });
    await flush();
    expect([closeIdle.mock.calls.length, String(failed.mock.calls[0]?.[0])]).toEqual([0, "Error: core: shrink"]);
    const failed2 = vi.fn();
    trimOnPressure({ trim: async () => 1, closeIdle: async () => { throw new Error("water: close"); } } as unknown as ReleaseLoader, failed2);
    listeners[1]!({ level: 15, maps: 0, purged: 2 });
    await flush(); await flush();
    expect(String(failed2.mock.calls[0]?.[0])).toBe("Error: water: close");
  });

  it("a build without the purge library says so (reported), and still trims", async () => {
    const trim = vi.fn(async () => 0);
    const failed = vi.fn();
    trimOnPressure({ trim, closeIdle: async () => 0 } as unknown as ReleaseLoader, failed);
    listeners[0]!({ level: 15, maps: 0, purged: -1 });
    await flush();
    expect([trim.mock.calls.length, String(failed.mock.calls[0]?.[0])])
      .toEqual([1, "Error: memory-trim: libmemorytrim is not in this build (no purge on a trim)"]);
  });
});
