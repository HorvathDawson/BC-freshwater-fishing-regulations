/**
 * `@app/data/regs` — the regulations half of a release, read: T02's blob codec, T03's store over the release files
 * (water + regs-<region> + regs-sid, through the `ReleaseLoader`). T04/T09 append `reads`, `inputs`.
 */
export * from "./blob";
export * from "./store";
