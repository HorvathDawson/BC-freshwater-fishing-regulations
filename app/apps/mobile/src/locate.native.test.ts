/** (T1093) the phone's `here()` and `watch()` when its Location is off: marked the environment's where it is thrown. */
import { describe, expect, it, vi } from "vitest";
import { isEnvironment } from "@app/core";

const OFF = Object.assign(new Error("Location request failed due to unsatisfied device settings"), { code: "ERR_LOCATION_SETTINGS_UNSATISFIED" });
const mocks = vi.hoisted(() => ({ watch: vi.fn(), current: vi.fn(), last: vi.fn() }));
vi.mock("expo-location", () => ({
  Accuracy: { High: 4 },
  requestForegroundPermissionsAsync: async () => ({ granted: true }),
  getForegroundPermissionsAsync: async () => ({ granted: true }),
  getLastKnownPositionAsync: mocks.last,
  getCurrentPositionAsync: mocks.current,
  watchPositionAsync: mocks.watch,
}));
vi.mock("react-native", () => ({ AppState: { addEventListener: () => ({ remove: () => {} }) } }));

describe("locate.native, the phone's Location off", () => {
  it("here(): a refused last-known or current fix is the environment's, as it came", async () => {
    const { locate } = await import("./locate.native");
    mocks.last.mockRejectedValueOnce(OFF);
    const a = await locate!.here().catch((e: unknown) => e);
    mocks.last.mockResolvedValueOnce(null); mocks.current.mockRejectedValueOnce(OFF);
    const b = await locate!.here().catch((e: unknown) => e);
    expect([a === OFF, isEnvironment(a), isEnvironment(b)]).toEqual([true, true, true]);
  });
  it("watch(): a start the phone refuses, and an error the watch reports, reach the caller marked the environment's", async () => {
    const { locate } = await import("./locate.native");
    const got: unknown[] = [];
    mocks.watch.mockRejectedValueOnce(OFF);
    const stop = locate!.watch!(() => {}, (e) => got.push(e));
    await new Promise((r) => setTimeout(r, 5));
    stop();
    mocks.watch.mockImplementationOnce(async (_o: unknown, _cb: unknown, onError: (r: string) => void) => { onError("Location is currently unavailable."); return { remove: () => {} }; });
    const stop2 = locate!.watch!(() => {}, (e) => got.push(e));
    await new Promise((r) => setTimeout(r, 5));
    stop2();
    expect(got.map((e) => isEnvironment(e))).toEqual([true, true]);
  });
});
