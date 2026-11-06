// (T431) THE FEEDS WORKER'S TESTS GATE CI AND THE STAGING DEPLOY. The worker (app/workers/feeds) is outside the pnpm
// workspace, so `pnpm check` never runs its suites (the DFO and gauge parity with the Python among them); for months
// they ran nowhere and `deploy/staging/feeds.sh` deployed with no test step. This pins both gates, and that the
// deploy refuses a worker without its required REPORT_SALT.
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const REPO = new URL("../../", import.meta.url);
const read = (p: string) => readFileSync(new URL(p, REPO), "utf8");
const WORKER_TESTS = "npx vitest run --root .";

describe("the feeds worker's suites gate CI and the staging deploy", () => {
  it("app-ci.yml runs them in app/workers/feeds, after the venv the Python parity needs", () => {
    const ci = read(".github/workflows/app-ci.yml");
    const step = /- name: Feeds worker tests\n\s+working-directory: app\/workers\/feeds\n\s+run: (.+)\n/.exec(ci);
    expect(step?.[1]).toBe(WORKER_TESTS);
    expect(ci.indexOf("python -m venv .venv")).toBeLessThan(step!.index);
  });

  it("deploy/staging/feeds.sh runs them (set -e) and checks REPORT_SALT before it deploys", () => {
    const sh = read("deploy/staging/feeds.sh");
    expect(sh).toMatch(/^set -euo pipefail$/m);
    const tests = sh.indexOf(`(cd app/workers/feeds && ${WORKER_TESTS})`);
    const salt = sh.indexOf("wrangler secret list");
    const deploy = sh.indexOf("wrangler deploy");
    expect(tests).toBeGreaterThan(0);
    expect(salt).toBeGreaterThan(tests);
    expect(deploy).toBeGreaterThan(salt);
  });

  it("the worker's package test script is the same command", () => {
    expect(JSON.parse(read("app/workers/feeds/package.json")).scripts.test).toBe("vitest run --root .");
  });
});
