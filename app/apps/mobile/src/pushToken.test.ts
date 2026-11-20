/** (T739) The push-token seam per platform: iOS always (its APNs environment by the build); Android never (owner 2026-11-16). */
import { afterEach, describe, expect, it, vi } from "vitest";

const listeners: ((t: { type: string; data: string }) => void)[] = [];
vi.mock("expo-notifications", () => ({
  getDevicePushTokenAsync: vi.fn(async () => ({ type: "x", data: "tok-1" })),
  addPushTokenListener: (f: (t: { type: string; data: string }) => void) => { listeners.push(f); return { remove: () => listeners.splice(listeners.indexOf(f), 1) }; },
}));

afterEach(() => { vi.unstubAllEnvs(); vi.resetModules(); listeners.length = 0; });

describe("pushToken.android (T739)", () => {
  it("is null whatever the build says: no token asked, nothing registered (owner 2026-11-16: no Android push)", async () => {
    vi.stubEnv("EXPO_PUBLIC_FCM", "1");   // the old switch: it turns nothing on
    const n = await import("expo-notifications");
    const { pushSeam } = await import("./pushToken.android");
    expect(pushSeam).toBeNull();
    expect(n.getDevicePushTokenAsync).not.toHaveBeenCalled();
    expect(listeners).toEqual([]);
  });
});

describe("pushToken.ios (T739)", () => {
  it("sandbox for a simulator / development build, production when the TestFlight build says so", async () => {
    expect((await import("./pushToken.ios")).pushSeam).toMatchObject({ platform: "ios", env: "sandbox" });
    vi.resetModules();
    vi.stubEnv("EXPO_PUBLIC_APNS_ENV", "production");
    expect((await import("./pushToken.ios")).pushSeam).toMatchObject({ platform: "ios", env: "production" });
  });

  it("hands on a new token, and its undo stops listening", async () => {
    const { pushSeam } = await import("./pushToken.ios");
    expect(await pushSeam!.token()).toBe("tok-1");
    const got: string[] = [];
    const undo = pushSeam!.onToken((t) => got.push(t));
    listeners[0]!({ type: "ios", data: "tok-2" });
    undo();
    expect(got).toEqual(["tok-2"]);
    expect(listeners).toEqual([]);
  });
});
