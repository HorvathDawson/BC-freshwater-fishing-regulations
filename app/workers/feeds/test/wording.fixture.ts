// T324: the Worker has no compiled-in wording registry (one copy, R2). Tests read THE curated file from the repo — the
// same bytes `pipeline.deliver.sync_inputs` puts in R2 — so they type cells exactly as a synced Worker does.
import { loadRegistry, type Registry, type WordingEntry } from "../src/dfo/wording";
import { REGISTRY_TEXT } from "./dfo.scenario";

export { REGISTRY_TEXT };
export const REG: Registry = loadRegistry(JSON.parse(REGISTRY_TEXT) as { format: string; entries: WordingEntry[] });
