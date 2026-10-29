/**
 * (T199) THE RUN TIMING, over HTTP: `{base}/index.json` (runtiming-index/1) and the two files it names — the runs and
 * the paint index — each fetched VERIFIED against the index's decoded bytes and sha256 (`fetchVerified`). The local tile
 * server's `/runtiming/` in dev, R2's `runtiming/` on the data origin; the same bytes. NOT the release.
 *
 * READ LAZILY: nothing is asked until `latest()` is (the Map tab's Run timing mode, first chosen). THE LAST CAPTURE IS
 * KEPT: every whole set this device reads is written to `store` (the files are a few MB: the web keeps them in the
 * Cache API, a phone in a file); offline, or when a fetch fails, the last capture answers. An index whose files are
 * the capture's is answered from the capture without fetching them again. Every answer resolves, never rejects.
 */
import { checkRunTiming, parseRunTimingIndex, RunTimingFacts, type RunTimingIndex } from "@app/core";
import { fetchVerified } from "../fetchVerified";

/** Whole text under one name — async, because a few MB does not belong in localStorage. */
export interface RunTimingStore { read(): Promise<string | null>; write(text: string): Promise<void> }

export interface RunTimingData {
  readonly facts: RunTimingFacts;
  /** "live": fetched now; "capture": the last capture (offline, or the origin failed) */
  readonly from: "live" | "capture";
}
export interface RunTimingSource {
  /** The newest whole set: fetched now, else the last captured, else null. */
  latest(): Promise<RunTimingData | null>;
}

interface Captured { index: unknown; runs: unknown; paint: unknown }

const decoder = new TextDecoder();

export function httpRunTiming(base: string, store?: RunTimingStore, fetchImpl: typeof fetch = fetch): RunTimingSource {
  const root = base.replace(/\/$/, "");
  let held: Promise<RunTimingData | null> | null = null;
  const facts = (c: Captured): RunTimingFacts | null => {
    const index = parseRunTimingIndex(c.index);
    if (!index) return null;
    const { runs, paint } = checkRunTiming(c.runs, c.paint, index);
    return new RunTimingFacts(index, runs, paint);
  };
  const captured = async (): Promise<{ raw: Captured; facts: RunTimingFacts } | null> => {
    try {
      const t = await store?.read();
      if (!t) return null;
      const raw = JSON.parse(t) as Captured;
      const f = facts(raw);
      return f ? { raw, facts: f } : null;
    } catch { return null; }
  };
  const json = async (url: string, f: RunTimingIndex["files"]["runs"]): Promise<unknown> =>
    JSON.parse(decoder.decode(await fetchVerified(`${root}/${url}`, f, undefined, fetchImpl)));
  const get = async (): Promise<RunTimingData | null> => {
    const was = await captured();
    let index: RunTimingIndex | null = null;
    let rawIndex: unknown = null;
    try {
      const r = await fetchImpl(`${root}/index.json`, { cache: "no-store" });
      if (r.ok) { rawIndex = JSON.parse(await r.text()); index = parseRunTimingIndex(rawIndex); }
    } catch { /* offline: the capture answers */ }
    if (!index) return was ? { facts: was.facts, from: "capture" } : null;
    const same = was && JSON.stringify(parseRunTimingIndex(was.raw.index)?.files) === JSON.stringify(index.files);
    if (same) return { facts: was.facts, from: "live" };
    try {
      const [runs, paint] = await Promise.all([json(index.files.runs.url, index.files.runs),
                                               json(index.files.paint.url, index.files.paint)]);
      const raw: Captured = { index: rawIndex, runs, paint };
      const f = facts(raw);
      if (!f) return was ? { facts: was.facts, from: "capture" } : null;
      try { await store?.write(JSON.stringify(raw)); } catch { /* a cache: costs only the next offline open */ }
      return { facts: f, from: "live" };
    } catch {
      return was ? { facts: was.facts, from: "capture" } : null;
    }
  };
  return {
    latest() {
      held ??= get();
      return held;
    },
  };
}
