/**
 * T917 — HOW MANY THREADS THE SEO BUILD RUNS. A render thread (panePool.ts) holds its own module runner, renderer and
 * release reads: RENDER_THREAD_GB each at its peak (measured on the full build). A job thread (jobs.ts) holds next to
 * nothing. So the render threads are what the free memory pays for, and the job threads take the cores left (a page's
 * compression costs about half its render). `--threads N` asks for N render threads; `auto` (the default) asks for as many as
 * the cores keep busy. Either way the count is held to what fits in the memory free at the start with KEEP_FREE of the
 * machine still free at the peak (heavy.sh stops a job under 6 % free), and the result is said, never cut silently.
 */
import { execFileSync } from "node:child_process";
import { availableParallelism, freemem, platform, totalmem } from "node:os";

const GB = 1024 ** 3;
/** a render thread's share of the build's peak footprint (T917 measured: 9.0 GB on 4 threads, 15.0-16.4 GB on 10: ~1.25 GB each) */
export const RENDER_THREAD_GB = 1.3;
/** the main thread's own peak (the model, the hubs, the share cards held until written) */
export const MAIN_GB = 4.5;
/** the part of the machine's memory left free at the build's peak */
export const KEEP_FREE = 0.1;
/** job threads per render thread: a page's gzip + brotli takes about half its render (T917 profile) */
const JOBS_PER_RENDER = 0.6;

export interface ThreadCounts { readonly render: number; readonly jobs: number; readonly said: string }

/**
 * The memory free now, in bytes: macOS's own reading (`memory_pressure`, as heavy.sh reads it: free + reclaimable), or
 * the OS's available memory elsewhere (Linux: MemAvailable). Throws when macOS's reading cannot be read.
 */
export function freeBytes(): number {
  if (platform() !== "darwin") return freemem();
  const out = execFileSync("memory_pressure", ["-Q"], { encoding: "utf8" });
  const pct = /free percentage:\s*(\d+)%/.exec(out)?.[1];
  if (pct === undefined) throw new Error(`seo: --threads: cannot read the free memory from \`memory_pressure -Q\` (${JSON.stringify(out.slice(-120))}); give --threads N`);
  return (Number(pct) / 100) * totalmem();
}

/** The render and job thread counts for `asked` (`auto` or a whole number ≥ 1), the free memory, the RAM and the cores. */
export function threadCounts(asked: string, free = freeBytes(), total = totalmem(), cores = availableParallelism()): ThreadCounts {
  const want = asked === "auto" ? Math.max(1, Math.floor((cores - 1) / (1 + JOBS_PER_RENDER))) : Number(asked);
  if (!Number.isInteger(want) || want < 1) throw new Error(`seo: --threads ${asked}: give a whole number of render threads ≥ 1, or auto`);
  const fits = Math.floor((free - KEEP_FREE * total - MAIN_GB * GB) / (RENDER_THREAD_GB * GB));
  const gb = (b: number) => (b / GB).toFixed(1);
  const budget = `${gb(free)} GB free of ${gb(total)}: ${RENDER_THREAD_GB} GB a render thread, ${MAIN_GB} GB main, ${KEEP_FREE * 100} % kept free`;
  // (T917 review) not even one render thread fits: refused, never run anyway on a machine that may then be stopped mid-build
  if (fits < 1) throw new Error(`seo: --threads ${asked}: the free memory fits no render thread (${budget}); free some memory and run again`);
  const render = Math.min(want, fits);
  const jobs = Math.max(2, Math.min(Math.ceil(render * JOBS_PER_RENDER), cores - 1 - render));
  const why = render < want ? `held to ${render} by memory (${budget})` : `${gb(free)} GB free of ${gb(total)}, ${cores} cores`;
  return { render, jobs, said: `${render} render + ${jobs} job threads (--threads ${asked}: ${why})` };
}
