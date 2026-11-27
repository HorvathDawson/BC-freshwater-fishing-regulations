/**
 * THE RELEASE (P08/P10/P11), platform-free: the loader, install/update, and the reader over them. The platform half —
 * how a file is fetched, cached, installed and opened — is `@app/data/release/platform` (`./drivers`).
 */
export { releaseLoader, warmLike, type DbName, type ReleaseLoader } from "./loader";
export { heldPath, POINTER, synopsisPath, type Progress, type ReleaseFs } from "./install";
export { readGaugeClim, readGaugePage, readGaugeRows, type GaugePageFacts } from "./gaugeRows";
export { makeReleaseSource } from "./source";
export { assetsDigest } from "./assets";
export { bootRelease, bootSaid, releaseTiles, BootError, SpaceError, UnreadableError, type BootOptions, type Booted, type MobileDataGate, type ReleasePlatform } from "./boot";
export type { BackgroundNative, BgJob } from "./bgDownload";
export { prefetchPlan, regionOfTile, runPrefetch, type PrefetchSignals } from "./prefetch";
export { createBackgroundPrefetch, LOW_BATTERY, othersAhead, readerDownloading, type BackgroundPrefetch, type NetSignals, type PrefetchSnapshot }
  from "./background";
export { offlineSizes, regsKept } from "./offline";
export { offlineImagery } from "./offlineImagery";
export { createReplacedProbe, type ReplacedProbe } from "./replaced";
