/**
 * THE PHONE'S WORD ON BEING ONLINE (owner 2026-10-21 late: "Add network-status detection on phones (netinfo)"):
 * `@react-native-community/netinfo`'s state, read as `Connectivity`. NetInfo itself is replaced by a switch here (its
 * native module does not exist in jsdom); what is pinned is how its state is read, and the fallback when the module
 * is not linked (a dev client built before it was added): no source, the tile probe the only signal, as before.
 */
import { describe, expect, it } from "vitest";
import { netInfoConnectivity, onlineFrom, type NetInfoLike, type NetInfoStateLike } from "./netInfoConnectivity";

/** A NetInfo that reports what a test says, and records its listeners coming and going. */
function fakeNetInfo() {
  const log: string[] = [];
  let listener: ((s: NetInfoStateLike) => void) | null = null;
  const ni: NetInfoLike = {
    addEventListener(on) {
      log.push("listen");
      listener = on;
      return () => { log.push("unlisten"); listener = null; };
    },
  };
  return { ni, log, emit: (s: NetInfoStateLike) => listener?.(s) };
}

describe("onlineFrom: NetInfo's state as one yes/no", () => {
  it("offline only on a definite no; an unknown (null) is left to the tile probe", () => {
    const cases: NetInfoStateLike[] = [
      { isConnected: true, isInternetReachable: true }, { isConnected: true, isInternetReachable: null },
      { isConnected: null, isInternetReachable: null }, { isConnected: false, isInternetReachable: null },
      { isConnected: true, isInternetReachable: false }, { isConnected: false, isInternetReachable: false },
      { isConnected: true },
    ];
    expect(cases.map(onlineFrom)).toEqual([true, true, true, false, false, false, true]);
  });
});

describe("netInfoConnectivity", () => {
  it("watches; says each CHANGE once, starting from online; the subscription is NetInfo's listener", () => {
    const f = fakeNetInfo();
    const c = netInfoConnectivity(() => f.ni);
    const heard: string[] = [];
    const off = c.subscribe((on) => heard.push(`${on} (online() ${c.online()})`));
    f.emit({ isConnected: true, isInternetReachable: null });            // the first report: no change
    f.emit({ isConnected: true, isInternetReachable: false });           // wifi with no internet: offline
    f.emit({ isConnected: false, isInternetReachable: false });          // still offline: not said again
    f.emit({ isConnected: true, isInternetReachable: true });            // back
    off();
    f.emit({ isConnected: false, isInternetReachable: false });          // nobody listening: nothing heard
    expect([c.watches, c.probeWhileUp, heard, f.log]).toEqual([
      true, true, ["false (online() false)", "true (online() true)"], ["listen", "unlisten"]]);
  });

  it("the device already offline when the screen starts: online() says so from the first report", () => {
    const f = fakeNetInfo();
    const c = netInfoConnectivity(() => f.ni);
    const heard: boolean[] = [];
    c.subscribe((on) => heard.push(on));
    f.emit({ isConnected: false, isInternetReachable: false });
    expect([heard, c.online()]).toEqual([[false], false]);
  });

  it("NetInfo's module not linked (its import throws): no source — the tile probe is the phone's only signal", () => {
    const c = netInfoConnectivity(() => { throw new Error("NativeModule.RNCNetInfo is null"); });
    const heard: boolean[] = [];
    const off = c.subscribe((on) => heard.push(on));
    off();
    expect([c.watches, c.probeWhileUp, c.online(), heard]).toEqual([false, true, true, []]);
  });
});

describe("(T231) metered: cellular or an expensive connection holds a large download for Wi-Fi", () => {
  it("reads the type and the system's word; each change is told", async () => {
    const { meteredFrom } = await import("./netInfoConnectivity");
    expect([{ isConnected: true, type: "wifi" }, { isConnected: true, type: "cellular" },
            { isConnected: true, type: "wifi", details: { isConnectionExpensive: true } }].map(meteredFrom))
      .toEqual([false, true, true]);
    const listeners: ((s: NetInfoStateLike) => void)[] = [];
    const ni: NetInfoLike = { addEventListener(on) { listeners.push(on); return () => {}; } };
    const c = netInfoConnectivity(() => ni);
    expect(listeners.length).toBe(0);                       // nothing followed until it is asked
    expect(c.metered!()).toBe(false);
    const heard: boolean[] = [];
    c.watchMetered!((m) => heard.push(m));
    const emit = (s: NetInfoStateLike) => listeners.forEach((l) => l(s));
    emit({ isConnected: true, type: "cellular" });
    expect(c.metered!()).toBe(true);
    emit({ isConnected: true, type: "cellular" });
    emit({ isConnected: true, type: "wifi" });
    expect(heard).toEqual([true, false]);
  });
});
