/**
 * (T238) THE OFFLINE MAPS PAGE, headless: the page's view model (`@app/core` `offlineVM`) over the release's sizes and
 * what this device keeps, and its actions. Web: the release cache through `@app/data` `createOfflineStore` (scanned when
 * the page opens; Save / Remove / Resume / Remove everything). Phone: the installed set (every region), the update
 * service's release download (progress; Resume = a check now) and T231's large-updates-on-mobile-data setting, shown
 * as "Wi-Fi only".
 */
import { useEffect, useMemo, useSyncExternalStore } from "react";
import { offlineVM, type OfflineVM, type UpdateEvent } from "@app/core";
import { createOfflineStore, offlineSizes, type OfflineSnapshot, type ReleaseLoader } from "@app/data/release";
import type { UpdateSnapshot } from "@app/data";

export interface OfflineControls {
  readonly vm: OfflineVM;
  save(region: string): void;
  remove(region: string): void;
  clear(): void;
  /** phone: fetch a paused release download now */
  resume(): void;
  /** phone: large downloads on Wi-Fi only (the inverse of T231's mobile-data setting) */
  setWifiOnly(on: boolean): void;
}

const NO_SUB = () => () => {};

export function useOfflineMaps(o: {
  loader: ReleaseLoader | null;
  /** the page is open: the cache is scanned then (web) */
  open: boolean;
  platform: "phone" | "web";
  /** phone: the update service's state, its check and its mobile-data setting (null: no service) */
  updates?: UpdateSnapshot | null;
  check?: (() => void) | null;
  allowMetered?: ((on: boolean) => void) | null;
  onEvent?: (e: UpdateEvent) => void;
}): OfflineControls | null {
  const { loader, platform } = o;
  const web = platform === "web";
  const store = useMemo(() => (loader && web ? createOfflineStore(loader, { onEvent: o.onEvent }) : null), [loader, web]);
  const snap: OfflineSnapshot | null = useSyncExternalStore(store ? store.subscribe : NO_SUB,
                                                            () => store?.snapshot() ?? null, () => store?.snapshot() ?? null);
  useEffect(() => { if (store && o.open) void store.scan(); }, [store, o.open]);
  if (!loader) return null;
  const m = loader.manifest;
  const sizes = offlineSizes(m);
  const u = o.updates ?? null;
  const vm = web
    ? offlineVM({ platform: "web", regions: snap?.regions ?? sizes.regions.map((r) => ({ ...r, held: 0, state: "none" as const })),
                  shared: snap?.shared ?? { bytes: sizes.shared, held: 0 }, map: sizes.map, job: snap?.job ?? null,
                  storable: snap ? snap.storable : true })
    : offlineVM({ platform: "phone", regions: sizes.regions.map((r) => ({ ...r, held: r.bytes, state: "kept" as const })),
                  shared: { bytes: sizes.shared, held: sizes.shared }, map: sizes.map,
                  update: u?.release ? { phase: u.release.phase, bytes: u.release.progress?.bytes ?? 0,
                                         of: u.release.progress?.of ?? 0 } : null,
                  wifiOnly: u && o.allowMetered ? !u.allowMetered : null });
  return {
    vm,
    save: (r) => { void store?.save(r as never); },
    remove: (r) => { void store?.remove(r as never); },
    clear: () => { void store?.clear(); },
    resume: () => { o.check?.(); },
    setWifiOnly: (on) => { o.allowMetered?.(!on); },
  };
}
