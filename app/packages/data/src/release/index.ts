/**
 * THE RELEASE (P08/P10/P11), platform-free: the loader, install/update, and the reader over them. The platform half —
 * how a file is fetched, cached, installed and opened — is `@app/data/release/platform` (`./drivers`).
 */
export { releaseLoader, ReleaseError, ALL_REGIONS, type DbName, type FileName, type Opener, type ReleaseLoader,
         type ReleaseSource } from "./loader";
export { install, prune, heldFrom, heldPath, pointerFor, sameSet, rangedDownload, POINTER, type Download,
         type DownloadTo, type PartFs, type Progress, type ReleaseFs } from "./install";
export { makeReleaseSource } from "./source";
export { bootRelease, releaseTiles, BootError, type BootOptions, type BootStage, type Booted, type RolledBack,
         type ReleasePlatform } from "./boot";
export { heldManifest } from "./install";
export { CROSS, META, RELEASE_QUERIES } from "./queries";
