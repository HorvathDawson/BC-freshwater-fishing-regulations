/**
 * THE RELEASE (P08/P10/P11), platform-free: the loader, install/update, and the reader over them. The platform half —
 * how a file is fetched, cached, installed and opened — is `@app/data/release/platform` (`./drivers`).
 */
export { releaseLoader, ReleaseError, ALL_REGIONS, type DbName, type FileName, type Opener, type ReleaseLoader,
         type ReleaseSource } from "./loader";
export { install, prune, heldFrom, heldPath, rangedDownload, POINTER, type Download, type DownloadTo, type PartFs,
         type Progress, type ReleaseFs } from "./install";
