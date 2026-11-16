/**
 * (T722) ONE BINDING FOR BOTH SYSTEM DOWNLOADERS: Android's module has the notification pair and no backup exclusion,
 * iOS's the reverse. Each call goes through the module object; what a module lacks is absent, never faked.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";

const mod = vi.hoisted(() => ({ current: null as Record<string, unknown> | null }));
vi.mock("expo", () => ({ requireOptionalNativeModule: () => mod.current }));

const base = () => ({
  jobs: vi.fn(async () => []), enqueue: vi.fn(async () => 7), remove: vi.fn(async () => {}),
  claim: vi.fn(async () => {}), sha256: vi.fn(async () => "ab"),
});

async function load() {
  vi.resetModules();
  return import("./bgDownload");
}

describe("bgDownload binding (T722)", () => {
  beforeEach(() => { mod.current = null; });

  it("no module (the web, an old client): nothing", async () => {
    const b = await load();
    expect([b.backgroundDownloads, b.excludeFromBackup]).toEqual([null, null]);
  });

  it("Android: the notification pair, no backup exclusion", async () => {
    const m = { ...base(), notificationsAllowed: vi.fn(async () => false), askNotifications: vi.fn(async () => true) };
    mod.current = m;
    const b = await load();
    expect(b.excludeFromBackup).toBeNull();
    expect(await b.backgroundDownloads!.notificationsAllowed!()).toBe(false);
    expect(await b.backgroundDownloads!.askNotifications!()).toBe(true);
    expect(await b.backgroundDownloads!.enqueue("u", "n", "t", "x", false)).toBe(7);
    expect(m.enqueue).toHaveBeenCalledWith("u", "n", "t", "x", false);
  });

  it("iOS: the backup exclusion, and no notification to ask for (the boot asks nothing)", async () => {
    const m = { ...base(), excludeFromBackup: vi.fn(async () => {}) };
    mod.current = m;
    const b = await load();
    expect(b.backgroundDownloads!.notificationsAllowed).toBeUndefined();
    expect(b.backgroundDownloads!.askNotifications).toBeUndefined();
    await b.excludeFromBackup!("file:///doc/release/");
    expect(m.excludeFromBackup).toHaveBeenCalledWith("file:///doc/release/");
    expect(await b.backgroundDownloads!.sha256("file:///x")).toBe("ab");
  });
});
