// T225 node-only helpers for the gauge parity test and timing script (never imported by src/): a directory-backed
// Store, a Fetch that serves a URL-keyed recording from memory, the seed copy and the Python harness call, and the
// output comparison.
import { execFileSync } from "node:child_process";
import { cpSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import type { Fetch, Store } from "../../src/io";

export const FEEDS = resolve(dirname(new URL(import.meta.url).pathname), "../..");
export const REPO = resolve(FEEDS, "../../..");
export const PY = join(REPO, ".venv/bin/python");
export const HARNESS = join(FEEDS, "parity/gauges/harness.py");

/** A Store over a directory (keys may hold "/"). `delete` too, like R2. */
export function dirStore(root: string): Store & { delete(key: string): Promise<void> } {
  mkdirSync(root, { recursive: true });
  return {
    async get(key) {
      const p = join(root, key);
      return existsSync(p) ? readFileSync(p, "utf-8") : null;
    },
    async put(key, body) {
      const p = join(root, key);
      mkdirSync(dirname(p), { recursive: true });
      writeFileSync(p, body);
    },
    async delete(key) {
      rmSync(join(root, key), { force: true });
    },
  };
}

/** A Store held in memory (the timing runs: no disk in the measured path). */
export function memStore(init: Map<string, string> = new Map()): Store & { delete(key: string): Promise<void>; data: Map<string, string> } {
  const data = new Map(init);
  return {
    data,
    async get(key) {
      return data.has(key) ? data.get(key)! : null;
    },
    async put(key, body) {
      data.set(key, body);
    },
    async delete(key) {
      data.delete(key);
    },
  };
}

export function readTree(root: string, prefix = ""): Map<string, string> {
  const out = new Map<string, string>();
  if (!existsSync(root)) return out;
  for (const name of readdirSync(root)) {
    const p = join(root, name);
    if (statSync(p).isDirectory()) for (const [k, v] of readTree(p, `${prefix}${name}/`)) out.set(k, v);
    else out.set(prefix + name, readFileSync(p, "utf-8"));
  }
  return out;
}

export interface Recording {
  now: string;
  urls: Map<string, { status: number; body: Uint8Array }>;
}

export function loadRecording(dir: string): Recording {
  const man = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf-8"));
  const urls = new Map<string, { status: number; body: Uint8Array }>();
  for (const [u, v] of Object.entries<any>(man.urls)) urls.set(u, { status: v.status, body: readFileSync(join(dir, "bodies", v.file)) });
  return { now: man.now, urls };
}

/** A Fetch over a recording: GETs served from memory, POSTs captured, unrecorded URLs a network error (and listed). */
export function fixtureFetch(rec: Recording): { fetch: Fetch; posts: Array<{ url: string; body: string }>; misses: string[] } {
  const posts: Array<{ url: string; body: string }> = [];
  const misses: string[] = [];
  const fetch: Fetch = async (url, init) => {
    if ((init?.method ?? "GET").toUpperCase() === "POST") {
      posts.push({ url, body: String(init?.body ?? "") });
      return new Response("ok", { status: 200 });
    }
    const got = rec.urls.get(url);
    if (!got) {
      misses.push(url);
      throw new TypeError("fetch failed (not recorded)");
    }
    if (got.status === 0) throw new TypeError("fetch failed (recorded network failure)");
    return new Response(got.body.length ? (got.body as unknown as BodyInit) : null, { status: got.status, headers: { "content-length": String(got.body.length) } });
  };
  return { fetch, posts, misses };
}

/** Run the PYTHON publisher over a fixture dir into `out` (feed/, priv/, posts.json, misses.json). */
export function runPython(fixtures: string, out: string, notify?: string): void {
  rmSync(out, { recursive: true, force: true });
  const args = [HARNESS, "replay", "--fixtures", fixtures, "--out", out];
  if (notify) args.push("--notify", notify);
  execFileSync(PY, args, { cwd: REPO, stdio: ["ignore", "ignore", "inherit"] });
}

export function seedInto(fixtures: string, out: string): void {
  rmSync(out, { recursive: true, force: true });
  cpSync(join(fixtures, "seed"), out, { recursive: true });
  rmSync(join(out, "edits.json"), { force: true });
}

// ------------------------------------------------------------------------------------------------ comparison ----

export interface FileDiff {
  file: string;
  kind: "missing-ts" | "missing-py" | "semantic" | "ulp" | "bytes-only" | "identical";
  detail?: string;
}

/** Deep equality of parsed JSON: numbers by value (-0 == 0), object keys as sets. First difference's path, or null. */
export function firstDiff(a: unknown, b: unknown, path = "$", ulps: string[] = []): string | null {
  if (typeof a === "number" && typeof b === "number") {
    if (a === b || (Number.isNaN(a) && Number.isNaN(b))) return null;
    // LIBM NOISE: Python's math.exp/log (the platform libm) and V8's (fdlibm) may differ in the last bit, and the
    // percentile's interpolation (value - lo) / span can magnify that a few times; within 1e-12 relative is that,
    // recorded ("ulp") and not a semantic difference
    if (Math.abs(a - b) <= 1e-12 * Math.max(Math.abs(a), Math.abs(b))) {
      ulps.push(`${path}: py ${a} ts ${b}`);
      return null;
    }
    return `${path}: py ${a} ts ${b}`;
  }
  if (a === null || b === null || typeof a !== "object" || typeof b !== "object") return a === b ? null : `${path}: py ${JSON.stringify(a)?.slice(0, 80)} ts ${JSON.stringify(b)?.slice(0, 80)}`;
  if (Array.isArray(a) !== Array.isArray(b)) return `${path}: array vs object`;
  if (Array.isArray(a)) {
    const bb = b as unknown[];
    if (a.length !== bb.length) return `${path}: length py ${a.length} ts ${bb.length}`;
    for (let i = 0; i < a.length; i++) {
      const d = firstDiff(a[i], bb[i], `${path}[${i}]`, ulps);
      if (d) return d;
    }
    return null;
  }
  const ka = Object.keys(a as object);
  const kb = Object.keys(b as object);
  const sa = new Set(ka);
  for (const k of kb) if (!sa.has(k)) return `${path}.${k}: only in ts`;
  for (const k of ka) {
    if (!(k in (b as object))) return `${path}.${k}: only in py`;
    const d = firstDiff((a as any)[k], (b as any)[k], `${path}.${k}`, ulps);
    if (d) return d;
  }
  return null;
}

/** The byte classes Python's json.dumps and JSON.stringify differ on, named for the report. */
export function byteClasses(py: string, ts: string): string[] {
  const out: string[] = [];
  if (/\d\.0(?=[,\]}])/.test(py)) out.push("integral float (5.0 vs 5)");
  if (/\de-0\d/.test(py)) out.push("exponent (1e-05 vs 0.00001)");
  if (/\\u[0-9a-f]{4}/.test(py)) out.push("ensure_ascii \\uXXXX");
  if (/": /.test(py) || /, "/.test(py)) out.push("default separators (', ' ': ')");
  if (!out.length) {
    // same tokens, different order: integer-like keys JS sorts ascending
    out.push("key order (integer-like keys)");
  }
  void ts;
  return out;
}

export function compareJson(file: string, py: string | undefined, ts: string | undefined): FileDiff {
  if (py === undefined) return { file, kind: "missing-py" };
  if (ts === undefined) return { file, kind: "missing-ts" };
  if (py === ts) return { file, kind: "identical" };
  const parse = (s: string) => (file.endsWith(".jsonl") ? s.split("\n").filter(Boolean).map((l) => JSON.parse(l)) : JSON.parse(s));
  const ulps: string[] = [];
  const d = firstDiff(parse(py), parse(ts), "$", ulps);
  if (d) return { file, kind: "semantic", detail: d };
  if (ulps.length) return { file, kind: "ulp", detail: ulps.join(" | ") };
  return { file, kind: "bytes-only", detail: byteClasses(py, ts).join("; ") };
}

export function compareTrees(py: Map<string, string>, ts: Map<string, string>): FileDiff[] {
  const files = [...new Set([...py.keys(), ...ts.keys()])].sort();
  return files.map((f) => compareJson(f, py.get(f), ts.get(f)));
}
