/**
 * (0.9.0 release) THE GAUGE CHART'S SCRIPT BUILDS FROM ANY WORKING DIRECTORY. `deploy/staging/site-all.sh` runs
 * `node app/tools/build-seo.mjs` from the repo root; esbuild resolves an alias (react-native -> react-native-web) in its
 * working directory, so the chart built under `pnpm -C app` and failed in the deploy (53 "Could not resolve
 * react-native-web") after 19,000 water pages. The build runs here in a child process started at the repo root, as the
 * deploy starts it (esbuild refuses a DOM-like environment, and a worker thread cannot change directory).
 */
import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const APP = join(dirname(fileURLToPath(import.meta.url)), "../..");
const SCRIPT = `
const { makeRunner } = await import(${JSON.stringify(join(APP, "tools/seo/runner.mjs"))});
const r = await makeRunner();
let code = 0;
try { const m = await r.run(r.app + "tools/seo/gauges.ts"); const b = await m.chartScript(); console.log("built " + b.js.length); }
catch (e) { console.log("failed: " + String(e && e.message || e).split("\\n")[0]); code = 1; }
process.exit(code);
`;

describe("the gauge chart's script", () => {
  it("builds when the command starts at the repo root (as site-all.sh starts it)", () => {
    const run = spawnSync(process.execPath, ["--input-type=module", "-e", SCRIPT], { cwd: join(APP, ".."), encoding: "utf8" });
    const said = `${run.stdout}${run.stderr}`.trim().split("\n").filter((l) => /^(built|failed)/.test(l));
    expect([run.status, said.length, /^built \d{6,}$/.test(said[0] ?? "")], said.join(" | ") || run.stderr.slice(-400)).toEqual([0, 1, true]);
  }, 180_000);
});
