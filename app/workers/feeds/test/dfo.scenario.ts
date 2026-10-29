// T225 DFO parity: run one recorded scenario through the TS port (`runDfo`) and through the Python reference
// (`parity/dfo/harness.py`), step by step, so the test can diff index.json / unplaced.json / notices byte for byte.
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { Store } from "../src/io";
import { runDfo } from "../src/dfo";

export const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "../../../..");
export const FAIL_MSG = "fixture: the page could not be fetched";

export interface Step {
  name: string;
  now: string;
  fetched?: string | null;
  regions?: string[] | null;
  pages: Record<string, string | null>;
}
export interface Scenario {
  entries: string;
  initial?: { index?: string; unplaced?: string };
  steps: Step[];
}
export interface StepOut {
  name: string;
  index: string;
  unplaced: string;
  notices: { key: string; text: string }[];
}

export class MemStore implements Store {
  m = new Map<string, string>();
  async get(k: string) {
    return this.m.has(k) ? this.m.get(k)! : null;
  }
  async put(k: string, body: string) {
    this.m.set(k, body);
  }
}

/** `pythonParity` (default true): the Python's behaviour byte for byte; false = the Worker's robust mode. */
export async function runTs(scenarioPath: string, pythonParity = true): Promise<StepOut[]> {
  const base = dirname(scenarioPath);
  const sc: Scenario = JSON.parse(readFileSync(scenarioPath, "utf-8"));
  const feed = new MemStore();
  const priv = new MemStore();
  for (const name of ["index", "unplaced"] as const) {
    const p = sc.initial?.[name];
    if (p) feed.m.set(`${name}.json`, readFileSync(join(base, p), "utf-8"));
  }
  const entries = async (slug: string) => {
    const p = join(base, sc.entries, `region-${slug}.json`);
    return existsSync(p) ? readFileSync(p, "utf-8") : null;
  };
  const out: StepOut[] = [];
  for (const step of sc.steps) {
    const got = await runDfo({
      fetch: async () => {
        throw new Error("no network in parity");
      },
      feed,
      priv,
      entries,
      now: step.now,
      regions: step.regions ?? null,
      notifyUrl: null,
      pythonParity,
      fetcher: async (slug) => {
        const p = step.pages[slug];
        if (p === null || p === undefined) throw new Error(FAIL_MSG);
        return [readFileSync(join(base, p), "utf-8"), step.fetched ?? null];
      },
    });
    out.push({ name: step.name, index: feed.m.get("index.json")!, unplaced: feed.m.get("unplaced.json")!, notices: got.notices });
  }
  return out;
}

export function runPy(scenarioPath: string): StepOut[] {
  const out = mkdtempSync(join(tmpdir(), "dfo-parity-"));
  execFileSync(join(REPO, ".venv/bin/python"), [join(REPO, "app/workers/feeds/parity/dfo/harness.py"), scenarioPath, out], {
    stdio: ["ignore", "ignore", "pipe"],
    env: { ...process.env, FEED_NOTIFY_URL: "" },
  });
  const sc: Scenario = JSON.parse(readFileSync(scenarioPath, "utf-8"));
  return sc.steps.map((s, n) => {
    const d = join(out, `${n}-${s.name}`);
    return {
      name: s.name,
      index: readFileSync(join(d, "index.json"), "utf-8"),
      unplaced: readFileSync(join(d, "unplaced.json"), "utf-8"),
      notices: JSON.parse(readFileSync(join(d, "notices.json"), "utf-8")),
    };
  });
}
