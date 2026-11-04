/**
 * (T238) THE OFFLINE MAPS PAGE, headless (owner 2026-10-29: the regulations are never per region): the page's view model
 * (`@app/core` `offlineVM`) over the release's sizes — the regulations for all of B.C., the province-wide map, the
 * regions as parts of it — and, on the web, how much of the regulations the release cache holds (asked when the page
 * opens); on a phone the update service's release download (progress; Resume = a check now) and T231's
 * large-updates-on-mobile-data setting, shown as "Wi-Fi only". No action can drop the regulations.
 */
import { useEffect, useState } from "react";
import { reportFailure, offlineVM, type OfflineVM } from "@app/core";
import { offlineSizes, regsKept, type ReleaseLoader } from "@app/data/release";
import type { UpdateSnapshot } from "@app/data";

export interface OfflineControls {
  readonly vm: OfflineVM;
  /** phone: fetch a paused release download now */
  resume(): void;
  /** phone: large downloads on Wi-Fi only (the inverse of T231's mobile-data setting) */
  setWifiOnly(on: boolean): void;
}

export function useOfflineMaps(o: {
  loader: ReleaseLoader | null;
  /** the page is open: the cache is asked then (web) */
  open: boolean;
  platform: "phone" | "web";
  /** phone: the update service's state, its check and its mobile-data setting (null: no service) */
  updates?: UpdateSnapshot | null;
  check?: (() => void) | null;
  allowMetered?: ((on: boolean) => void) | null;
}): OfflineControls | null {
  const { loader, platform } = o;
  const web = platform === "web";
  const [held, setHeld] = useState<number | null>(null);
  useEffect(() => {
    if (!loader || !web || !o.open) return;
    let live = true;
    void regsKept(loader).then((n) => { if (live) setHeld(n); }, reportFailure);   // (T368) the size unsaid, reported
    return () => { live = false; };
  }, [loader, web, o.open]);
  if (!loader) return null;
  const sizes = offlineSizes(loader.manifest);
  const regions = Object.keys(loader.manifest.files).filter((f) => /^regs-(?!sid$)/.test(f)).map((f) => f.slice(5)).sort();
  const u = o.updates ?? null;
  const vm = offlineVM({
    platform, regions, regs: { bytes: sizes.regs, held: web ? held : null }, map: sizes.map, book: sizes.book,
    ...(web ? {} : {
      update: u?.release ? { phase: u.release.phase, bytes: u.release.progress?.bytes ?? 0, of: u.release.progress?.of ?? 0 } : null,
      wifiOnly: u && o.allowMetered ? !u.allowMetered : null,
    }),
  });
  return {
    vm,
    resume: () => { o.check?.(); },
    setWifiOnly: (on) => { o.allowMetered?.(!on); },
  };
}
