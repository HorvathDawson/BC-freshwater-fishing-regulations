/**
 * (T459) A TEST-BED SWITCH, for shots that show an owner two looks from one build: the link
 * `canifishthis://testbed/flag?name=<name>&on=0|1` (`adb shell am start -d …`) sets `name`, and every view reading it
 * draws again. Only on the test bed's build (`globalThis.__panProbe`, `EXPO_PUBLIC_PAN_PROBE=1`); anywhere else the
 * default, and nothing listens. Removed with the choice it was made for.
 */
import { useEffect, useState } from "react";
import { Linking } from "react-native";

const on = () => !!(globalThis as { __panProbe?: boolean }).__panProbe;
const flags = new Map<string, boolean>();
const subs = new Set<() => void>();
let listening = false;

/** The flag's value: `dflt` until a link sets it (test bed only). */
export function useTestbedFlag(name: string, dflt: boolean): boolean {
  const [, bump] = useState(0);
  useEffect(() => {
    if (!on()) return;
    if (!listening) {
      listening = true;
      Linking.addEventListener("url", ({ url }) => {
        const m = /^canifishthis:\/\/testbed\/flag\?(.*)$/.exec(url);
        if (!m) return;
        const q = new URLSearchParams(m[1]);
        const n = q.get("name");
        if (!n) return;
        flags.set(n, q.get("on") === "1");
        console.log(`[flag] ${JSON.stringify({ name: n, on: flags.get(n) })}`);
        for (const s of subs) s();
      });
    }
    const s = () => bump((x) => x + 1);
    subs.add(s);
    return () => { subs.delete(s); };
  }, []);
  return flags.get(name) ?? dflt;
}
