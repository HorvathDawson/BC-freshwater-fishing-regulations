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

/**
 * A downloader whose jobs advance one step per `jobs()` poll: pending → running (half) → done (whole). `still()` true:
 * no job moves on this poll (a download caught mid-way, as a reopened app finds it).
 */
function fakeDm(fs: ReturnType<typeof memFs>, bodies: Record<string, Uint8Array>,
                o: { failFirst?: boolean; hold?: (j: BgJob) => boolean; still?: () => boolean } = {}) {
  const jobs = new Map<number, BgJob>();
  let next = 1, failed = 0;
  const log: string[] = [];
  const dm: BackgroundNative = {
    jobs: async () => {
      for (const j of jobs.values()) {
        const body = bodies[j.url]!;
        if (o.still?.()) continue;
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

  it("(T756) a reopen on Wi-Fi after Wait for Wi-Fi keeps the RUNNING Wi-Fi-only jobs: nothing is removed, no byte thrown away", async () => {
    let still = false;
    const fs = memFs(), { dm, log, jobs } = fakeDm(fs, { [URL]: BODY }, { still: () => still });
    await background(dm, fs, never).queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], false);
    await dm.jobs();                                               // Wi-Fi is back: the job runs, half of it down
    still = true;                                                  // …and is found so by the reopened app
    expect([...jobs.values()].map((j) => [j.status, j.metered, j.bytes])).toEqual([["running", false, BODY.length >> 1]]);
    // the app is opened again, now on Wi-Fi: the mobile-data question answers "go"
    const again = background(dm, fs, never, { pollMs: 0 });
    await again.queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], true);
    expect(log.map((l) => l.split(" ").slice(0, 3).join(" "))).toEqual([`enqueue ${URL} wifi`]);
    still = false;
    vi.spyOn(console, "info").mockImplementation(() => {});
    await partDownload({ ...fs, hash: (p) => dm.sha256(fs.uri(p)) }, again.fill)(URL, "b", want);
    expect(fs.files.get("b")).toEqual(BODY);
    expect(log.map((l) => l.split(" ")[0])).toEqual(["enqueue", "claim"]);       // the one job, claimed
  });

  it("(T756) the other way is still queued again, and the log says what was thrown away: jobs that may use mobile data, then Wi-Fi only asked", async () => {
    const fs = memFs(), { dm, log } = fakeDm(fs, { [URL]: BODY });
    await background(dm, fs, never).queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], true);
    const said: string[] = [];
    vi.spyOn(console, "info").mockImplementation((m: string) => { said.push(m); });
    // the second queue's own poll finds it running, on whatever network
    await background(dm, fs, never).queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], false);
    expect(log.map((l) => l.split(" ").slice(0, 3).join(" "))).toEqual(
      [`enqueue ${URL} metered`, "remove 1", `enqueue ${URL} wifi`]);
    expect(said).toEqual([`[download] ${URL}: queued again from zero (was running, ${BODY.length >> 1} of ${BODY.length} bytes): it was queued to use mobile data and Wi-Fi only is now asked`]);
  });

  it("(T756) a Wi-Fi-only job PAUSED with bytes down is kept when mobile data is allowed; one with nothing down is queued again", async () => {
    const one = async (bytes: number) => {
      let still = false;
      const fs = memFs(), { dm, log, jobs } = fakeDm(fs, { [URL]: BODY }, { still: () => still });
      await background(dm, fs, never).queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], false);
      still = true;                                                // the job as the reopened app finds it: between tries
      for (const j of jobs.values()) { j.status = "paused"; j.bytes = bytes; }
      vi.spyOn(console, "info").mockImplementation(() => {});
      await background(dm, fs, never).queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], true);
      return log.map((l) => l.split(" ").slice(0, 3).join(" "));
    };
    expect(await one(BODY.length >> 1)).toEqual([`enqueue ${URL} wifi`]);
    expect(await one(0)).toEqual([`enqueue ${URL} wifi`, "remove 1", `enqueue ${URL} metered`]);
  });

  it("(T756) a kept Wi-Fi-only job that then waits while mobile data is allowed is queued again, and the log says so", async () => {
    let wifi = true, still = false;
    const fs = memFs(), { dm, log } = fakeDm(fs, { [URL]: BODY }, { hold: (j) => !j.metered && !wifi, still: () => still });
    await background(dm, fs, never).queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], false);
    await dm.jobs();                                               // running on Wi-Fi
    still = true;
    const again = background(dm, fs, never, { pollMs: 0, stuckMs: 0 });
    await again.queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], true);   // kept (running)
    still = false; wifi = false;                                   // Wi-Fi drops: the kept job waits for it
    const said: string[] = [];
    vi.spyOn(console, "info").mockImplementation((m: string) => { said.push(m); });
    await partDownload({ ...fs, hash: (p) => dm.sha256(fs.uri(p)) }, again.fill)(URL, "b", want);
    expect(fs.files.get("b")).toEqual(BODY);
    expect(log.map((l) => l.split(" ").slice(0, 3).join(" "))).toEqual(
      [`enqueue ${URL} wifi`, "remove 1", `enqueue ${URL} metered`, "claim 2"]);
    expect(said.filter((m) => m.includes("queued again"))).toEqual([
      `[download] ${URL}: a Wi-Fi-only download paused (reason 0) at ${BODY.length >> 1} of ${BODY.length} bytes while mobile data is allowed; queued again to use it`]);
  });

  it("(T756) a short pause of a kept Wi-Fi-only job is waited out (not queued again before stuckMs)", async () => {
    let held = 0, still = false;                                   // paused for two polls, then moving again
    const fs = memFs(), { dm, log, jobs } = fakeDm(fs, { [URL]: BODY }, { hold: () => held-- > 0, still: () => still });
    await background(dm, fs, never).queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], false);
    await dm.jobs();
    still = true;
    const again = background(dm, fs, never, { pollMs: 0, stuckMs: 60_000 });
    await again.queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], true);
    still = false; held = 2;
    // the fake's paused job is set moving again by hand once the hold is over (it only advances pending / running)
    const wake = setInterval(() => { if (held <= 0) for (const j of jobs.values()) if (j.status === "paused") j.status = "running"; }, 0);
    vi.spyOn(console, "info").mockImplementation(() => {});
    await partDownload({ ...fs, hash: (p) => dm.sha256(fs.uri(p)) }, again.fill)(URL, "b", want);
    clearInterval(wake);
    expect(log.map((l) => l.split(" ")[0])).toEqual(["enqueue", "claim"]);
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

  it("a finished job whose file is gone (the app's data cleared) is queued again, never claimed", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const fs = memFs(), { dm, jobs, log } = fakeDm(fs, { [URL]: BODY });
    jobs.set(99, { id: 99, url: URL, status: "done", reason: 0, bytes: BODY.length, total: BODY.length, metered: true, present: false });
    const bg = background(dm, fs, never, { pollMs: 0 });
    await bg.queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "f" }], true);
    expect(log.slice(0, 2).map((l) => l.split(" ").slice(0, 2).join(" "))).toEqual(["remove 99", `enqueue ${URL}`]);
    // and the install that reaches such a job: queued again, downloaded, verified
    const X = new Uint8Array([1, 2, 3]);
    const fx = fakeDm(fs, { "https://r2.test/x": X });
    fx.jobs.set(98, { id: 98, url: "https://r2.test/x", status: "done", reason: 0, bytes: 3, total: 3, metered: true, present: false });
    await partDownload({ ...fs, hash: (p) => fx.dm.sha256(fs.uri(p)) }, background(fx.dm, fs, never, { pollMs: 0 }).fill)(
      "https://r2.test/x", "x", { bytes: 3, sha256: sha256Hex(X) });
    expect(fs.files.get("x")).toEqual(X);
    expect(fx.log[0]).toBe("remove 98");
  }, 15_000);

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
