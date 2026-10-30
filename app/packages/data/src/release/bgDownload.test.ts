/**
 * (T298) The first install's big files through the platform's downloader (Android's DownloadManager), over a fake one:
 * queued at once, kept across a relaunch (the app was closed), re-queued under the other mobile-data choice, a failed
 * job tried again, the finished file claimed onto the `.part` and verified (sha256) before it is renamed into place.
 */
import { describe, expect, it, vi } from "vitest";
import { sha256Hex } from "../sha256";
import { background, type BackgroundNative, type BgJob } from "./bgDownload";
import { partDownload, type PartFill, type PartFs } from "./install";

function memFs(): PartFs & { files: Map<string, Uint8Array>; uri(p: string): string } {
  const files = new Map<string, Uint8Array>();
  return {
    files,
    uri: (p) => `file:///doc/${p}`,
    size: async (p) => files.get(p)?.length ?? 0,
    append: async () => { throw new Error("not used"); },
    chunks: async (p, each) => { each(files.get(p)!); },
    rename: async (a, b) => { files.set(b, files.get(a)!); files.delete(a); },
    remove: async (p) => { files.delete(p); },
  };
}

/** A downloader whose jobs advance one step per `jobs()` poll: pending → running (half) → done (whole). */
function fakeDm(fs: ReturnType<typeof memFs>, bodies: Record<string, Uint8Array>,
                o: { failFirst?: boolean; hold?: (j: BgJob) => boolean } = {}) {
  const jobs = new Map<number, BgJob>();
  let next = 1, failed = 0;
  const log: string[] = [];
  const dm: BackgroundNative = {
    jobs: async () => {
      for (const j of jobs.values()) {
        const body = bodies[j.url]!;
        if (o.hold?.(j)) { j.status = "paused"; continue; }
        if (j.status === "pending") { j.status = "running"; j.total = body.length; j.bytes = body.length >> 1; }
        else if (j.status === "running") {
          if (o.failFirst && failed++ === 0) { j.status = "failed"; j.reason = 404; continue; }
          j.status = "done"; j.bytes = body.length;
        }
      }
      return [...jobs.values()].map((j) => ({ ...j }));
    },
    enqueue: async (url, name, title, text, metered) => {
      log.push(`enqueue ${url} ${metered ? "metered" : "wifi"} ${title} / ${text} (${name})`);
      jobs.set(next, { id: next, url, status: "pending", reason: 0, bytes: 0, total: -1, metered });
      return next++;
    },
    remove: async (id) => { log.push(`remove ${id}`); jobs.delete(id); },
    claim: async (id, to) => {
      const j = jobs.get(id)!;
      fs.files.set(to.replace("file:///doc/", ""), bodies[j.url]!);
      jobs.delete(id);
      log.push(`claim ${id}`);
    },
    sha256: async (uri) => sha256Hex(fs.files.get(uri.replace("file:///doc/", ""))!),
  };
  return { dm, jobs, log };
}

const BODY = new TextEncoder().encode("the whole basemap, pretend");
const URL = "https://r2.test/basemap.pmtiles";
const want = { bytes: BODY.length, sha256: sha256Hex(BODY) };
const never: PartFill = async () => { throw new Error("the fallback was not expected"); };

describe("background (T298): the system's downloader carries the big files", () => {
  it("queues every big file at once (with the notification's words), then claims and verifies each", async () => {
    vi.spyOn(console, "info").mockImplementation(() => {});
    const fs = memFs(), { dm, log } = fakeDm(fs, { [URL]: BODY });
    const bg = background(dm, fs, never, { pollMs: 0 });
    await bg.queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "release/basemap.pmtiles" }], true);
    expect(log).toEqual([`enqueue ${URL} metered Downloading BC fishing rules / Base map · 0.0 MB (basemap.pmtiles-${BODY.length})`]);
    const seen: number[] = [];
    await partDownload({ ...fs, hash: (p) => dm.sha256(fs.uri(p)) }, bg.fill)(URL, "release/basemap.pmtiles", want, (n) => seen.push(n));
    expect(fs.files.get("release/basemap.pmtiles")).toEqual(BODY);
    expect(fs.files.has("release/basemap.pmtiles.part")).toBe(false);
    expect(seen.at(-1)).toBe(BODY.length);
    expect(log.slice(1)).toEqual(["claim 1"]);                     // the queued job was used: no second enqueue
  });

  it("a relaunch keeps the job the closed app left (it went on downloading), and a file in place is not queued", async () => {
    const fs = memFs(), { dm, log } = fakeDm(fs, { [URL]: BODY });
    await background(dm, fs, never).queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], true);
    await background(dm, fs, never).queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], true);
    expect(log.filter((l) => l.startsWith("enqueue"))).toHaveLength(1);
    fs.files.set("c", BODY);
    await background(dm, fs, never).queue([{ url: "https://r2.test/c", file: "tiles", bytes: BODY.length, path: "c" }], true);
    expect(log.filter((l) => l.startsWith("enqueue"))).toHaveLength(1);
  });

  it("Wait for Wi-Fi queues the jobs Wi-Fi only; Download now queues them again for mobile data", async () => {
    const fs = memFs(), { dm, log } = fakeDm(fs, { [URL]: BODY }, { hold: (j) => !j.metered });
    const bg = background(dm, fs, never);
    await bg.queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], false);
    await dm.jobs();
    expect((await dm.jobs())[0]!.status).toBe("paused");          // held for an unmetered network
    await bg.queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], true);
    expect(log.map((l) => l.split(" ").slice(0, 3).join(" "))).toEqual(
      [`enqueue ${URL} wifi`, "remove 1", `enqueue ${URL} metered`]);
  });

  it("a failed job is queued again (and the file still verified)", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const fs = memFs(), { dm, log } = fakeDm(fs, { [URL]: BODY }, { failFirst: true });
    const bg = background(dm, fs, never, { pollMs: 0 });
    await partDownload({ ...fs, hash: (p) => dm.sha256(fs.uri(p)) }, bg.fill)(URL, "f", want);
    expect(fs.files.get("f")).toEqual(BODY);
    expect(log.filter((l) => l.startsWith("enqueue"))).toHaveLength(2);
    expect(log).toContain("remove 1");
  });

  it("bytes that are not the manifest's are refused (sha256), never renamed into place", async () => {
    const fs = memFs(), { dm } = fakeDm(fs, { [URL]: new TextEncoder().encode("the whole basemap, PRETEND") });
    const bg = background(dm, fs, never, { pollMs: 0 });
    await expect(partDownload({ ...fs, hash: (p) => dm.sha256(fs.uri(p)) }, bg.fill)(URL, "f", want)).rejects.toThrow(/sha256/);
    expect(fs.files.has("f")).toBe(false);
  });

  it("a .part an older app left, with no job: resumed the old way (the fallback), not thrown away", async () => {
    const fs = memFs(), { dm, log } = fakeDm(fs, { [URL]: BODY });
    fs.files.set("f.part", BODY.slice(0, 4));
    const fallback: PartFill = async (_u, part) => { fs.files.set(part, BODY); };
    const bg = background(dm, fs, fallback);
    await bg.queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "f" }], true);
    await partDownload({ ...fs, hash: (p) => dm.sha256(fs.uri(p)) }, bg.fill)(URL, "f", want);
    expect(fs.files.get("f")).toEqual(BODY);
    expect(log).toEqual([]);
  });
});
