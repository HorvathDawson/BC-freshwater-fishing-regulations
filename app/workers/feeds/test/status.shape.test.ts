// (T757 item 7) THE DEPLOY GATE READS THE SHAPE THE WORKER SERVES. `deploy/lib/worker-shape.mjs` bundles `status.ts`
// and prints its `DFO_SHAPE`; the T759 merge left the served shape in `dfo/live.ts` only, the reader printed null and the
// staging deploy was refused ("does not emit them") for a worker that does. This holds the two together.
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { DFO_SHAPE } from "../src/status";
import { SHAPE } from "../src/dfo/live";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../../..");

describe("(T757 item 7) the feed shape the deploy gate reads", () => {
  it("is the shape /status.json serves, and it emits closed-area pieces", () => {
    expect(DFO_SHAPE).toEqual(SHAPE);
    expect(DFO_SHAPE.pieces).toBeGreaterThanOrEqual(1);
  });
  it("is what the deploy's reader prints", () => {
    const out = execFileSync("node", [path.join(ROOT, "deploy/lib/worker-shape.mjs")], { encoding: "utf-8" }).trim();
    expect(JSON.parse(out)).toEqual(DFO_SHAPE);
  });
});
