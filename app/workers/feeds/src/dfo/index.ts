// T225: the DFO salmon live feed, ported from pipeline/regs/dfo_salmon/live.py (`run`). See live.ts.
export { runDfo, liveFetcher, typePage, FORMAT, FAIL_NOTIFY } from "./live";
export type { DfoDeps, DfoResult, EntrySource, Fetcher } from "./live";
export { REAL_SLUGS, ALL_SLUGS, PAGES } from "./fetch";
