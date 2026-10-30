/**
 * (T307) A SPOT SAVES AT ONCE, ONLINE OR NOT. Capturing a spot or a visit asks the network for the gauge's reading, its
 * daily series, the weather and the live feed; offline each of those used to wait out its own timeout before the
 * spot could be saved. Here the device's online word decides first: offline, those asks are not made at all and answer
 * "could not read" (null / a refusal) at once — the same answers a failed fetch gives, so the spot keeps them pending
 * and the reconnect refresh (`useReconnectRefresh`) fills them in. Online but flaky: each ask is given `DEADLINE_MS`.
 *
 * Only the NETWORK asks are wrapped; the atlas lookups (panels, lake stations, the route) are on the device.
 */
import type { WaterSource } from "../index";
import type { WeatherSource } from "./capture";

/** how long a flaky line may hold the save, per ask (they run in parallel, so this is roughly the whole wait) */
export const DEADLINE_MS = 2_500;


/** `p`, or `fallback` at once when offline, or `fallback` when it has not answered in `ms`. A rejection stays a rejection. */
export function fastFail<T>(p: () => Promise<T>, online: (() => boolean) | undefined, fallback: () => T | Promise<T>,
                            ms = DEADLINE_MS): Promise<T> {
  if (online && !online()) return Promise.resolve().then(fallback);
  return new Promise<T>((resolve, reject) => {
    const t = setTimeout(() => { Promise.resolve().then(fallback).then(resolve, reject); }, ms);
    p().then((v) => { clearTimeout(t); resolve(v); }, (e) => { clearTimeout(t); reject(e); });
  });
}

export interface FastPorts {
  source: WaterSource;
  weather: WeatherSource | undefined;
  feed: { index(): Promise<any> } | undefined;
}

/** The ports for a capture: the same, with the network asks failing fast. Absent ports stay absent. */
export function fastPorts<F extends { index(): Promise<any> }>(
  ports: { source: WaterSource; weather?: WeatherSource; feed?: F }, online?: () => boolean, ms = DEADLINE_MS):
  { source: WaterSource; weather: WeatherSource | undefined; feed: F | undefined } {
  const { source, weather, feed } = ports;
  const refuse = () => Promise.reject(new Error("offline"));
  const fast: WaterSource = new Proxy(source, {
    get(target, key, recv) {
      const v = Reflect.get(target, key, recv);
      if (typeof v !== "function") return v;
      if (key === "gaugeNow") return (...a: unknown[]) => fastFail(() => v.apply(target, a), online, () => null, ms);
      if (key === "gaugeSeries") return (...a: unknown[]) => fastFail(() => v.apply(target, a), online, () => null, ms);
      if (key === "gaugeParameters") return (...a: unknown[]) => fastFail(() => v.apply(target, a), online, refuse, ms);
      return v.bind(target);
    },
  });
  const sky: WeatherSource | undefined = weather && {
    at: (...a) => fastFail(() => weather.at(...a), online, () => null, ms),
    ...(weather.days ? { days: (...a: Parameters<NonNullable<WeatherSource["days"]>>) =>
      fastFail(() => weather.days!(...a), online, () => null, ms) } : {}),
    ...(weather.ahead ? { ahead: (...a: Parameters<NonNullable<WeatherSource["ahead"]>>) =>
      fastFail(() => weather.ahead!(...a), online, () => null, ms) } : {}),
  };
  const live = feed && ({ ...feed, index: () => fastFail(() => feed.index(), online, refuse, ms) } as F);
  return { source: fast, weather: sky, feed: live };
}
