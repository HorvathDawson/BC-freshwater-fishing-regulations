/**
 * T917 review — a build thread that ends without an 'error' event (process.exit, a native crash) fails the build loudly:
 * its job and every waiting one are rejected, never left to hang. The threads here are stand-ins that die (data: modules).
 */
import { describe, expect, it } from "vitest";
import { JobPool } from "./jobs";
import { PanePool } from "./panePool";
import type { ThreadData } from "./paneWorker";

const mod = (src: string) => new URL(`data:text/javascript,${encodeURIComponent(src)}`);
const CARD = { fontFiles: [], font: "x", width: 1, height: 1 };
const DATA = { releaseDir: "", appDir: "", out: "/nowhere", origin: "", doc: { critical: "", cssHref: "", preload: [] } } as ThreadData;
/** a job thread that dies on its first job */
const DIES = mod(`import { parentPort } from "node:worker_threads"; parentPort.on("message", () => process.exit(3));`);

describe("T917: a thread that exits mid-job fails the build, never hangs it", () => {
  it("job threads: the running and the waiting jobs are rejected, later asks too", async () => {
    const jobs = new JobPool(1, CARD, DIES);
    try {
      const first = jobs.zip("/nowhere", "a/index.html", "<p>a</p>"), waiting = jobs.png("<svg/>");
      await expect(first).rejects.toThrow(/a job thread exited \(code 3\) during zip a\/index\.html; the build stops/);
      await expect(waiting).rejects.toThrow(/a job thread exited \(code 3\)/);
      await expect(jobs.zip("/nowhere", "b/index.html", "<p>b</p>")).rejects.toThrow(/exited \(code 3\)/);
    } finally { await jobs.close(); }
  }, 5000);

  it("render threads: the page it held and the queued pages are rejected, and so is the stylesheet ask", async () => {
    const jobs = new JobPool(1, CARD, DIES);
    const pool = new PanePool(1, DATA, jobs, mod(
      `import { parentPort } from "node:worker_threads"; parentPort.postMessage({ t: "ready" }); parentPort.on("message", () => process.exit(4));`));
    try {
      const page = pool.page("gnis:1", null, "", "water/a/index.html"), queued = pool.page("gnis:2", null, "", "water/b/index.html");
      await expect(page).rejects.toThrow(/a render thread exited \(code 4\) on .*gnis:1.*; the build stops/);
      await expect(queued).rejects.toThrow(/exited \(code 4\)/);
      await expect(pool.css()).rejects.toThrow(/exited \(code 4\)/);
    } finally { await pool.close(); await jobs.close(); }
  }, 5000);

  it("a thread that exits after its last page fails the stylesheet ask instead of hanging it", async () => {
    const jobs = new JobPool(1, CARD, DIES);
    const pool = new PanePool(1, DATA, jobs, mod(
      `import { parentPort } from "node:worker_threads"; parentPort.postMessage({ t: "ready" }); parentPort.on("message", (m) => { if (m.t === "css") process.exit(5); });`));
    try {
      await expect(pool.css()).rejects.toThrow(/exited \(code 5\) before its part of the stylesheet/);
    } finally { await pool.close(); await jobs.close(); }
  }, 5000);
});
