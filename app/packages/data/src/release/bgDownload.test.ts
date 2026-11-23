/**
 * (T298) The first install's big files through the platform's downloader (Android's DownloadManager), over a fake one:
 * queued at once, kept across a relaunch (the app was closed), re-queued under the other mobile-data choice, a failed
 * job tried again, the finished file claimed onto the `.part` and verified (sha256) before it is renamed into place.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { gzipSync } from "fflate";
import { INSTALL_WORDS, now as clockNow, setFailureSink } from "@app/core";
import { sha256Hex } from "../sha256";
import { background, type BackgroundNative, type BgJob } from "./bgDownload";
import { partDownload, type PartFill, type PartFs } from "./install";

function memFs(): PartFs & { files: Map<string, Uint8Array>; uri(p: string): string; read(p: string): Promise<Uint8Array> } {
  const files = new Map<string, Uint8Array>();
  return {
    files,
    uri: (p) => `file:///doc/${p}`,
    read: async (p) => { const b = files.get(p); if (!b) throw new Error(`no ${p}`); return b; },
    size: async (p) => files.get(p)?.length ?? 0,
    append: async () => { throw new Error("not used"); },
    chunks: async (p, each) => { each(files.get(p)!); },
    rename: async (a, b) => { files.set(b, files.get(a)!); files.delete(a); },
    remove: async (p) => { files.delete(p); },
  };
}

/**
 * A downloader whose jobs advance one step per `jobs()` poll: pending → running (half) → done (whole). `still()` true:
 * no job moves on this poll (a download caught mid-way, as a reopened app finds it). `hold(j)` true: the job is paused
 * (the hook may set its reason and bytes); once it is false the job runs again. `tick`: each poll moves the clock on.
 */
function fakeDm(fs: ReturnType<typeof memFs>, bodies: Record<string, Uint8Array>,
                o: { failFirst?: boolean; hold?: (j: BgJob) => boolean; still?: () => boolean; tick?: number } = {}) {
  const jobs = new Map<number, BgJob>();
  let next = 1, failed = 0;
  const log: string[] = [];
  const dm: BackgroundNative = {
    jobs: async () => {
      if (o.tick) vi.setSystemTime(clockNow() + o.tick);           // (T760) each poll is `tick` ms later (a fake clock)
      for (const j of jobs.values()) {
        const body = bodies[j.url]!;
        if (o.still?.()) continue;
        if (o.hold?.(j)) { j.status = "paused"; continue; }
        if (j.status === "paused") { j.status = "running"; continue; }   // a hold that ended: it runs again
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
    const again = background(dm, fs, never, { pollMs: 0, wifiWaitMs: 0 });
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

  it("(T756, iPhone) a kept Wi-Fi-only job still RUNNING but held back for Wi-Fi (reason 3) while mobile data is allowed is queued again to use it", async () => {
    // the iPhone module reports a task with bytes down as running, and its network path's reason: it never says paused
    const fs = memFs();
    let wait = false;
    const { dm, log, jobs } = fakeDm(fs, { [URL]: BODY }, { still: () => wait && jobs.has(1) });
    await background(dm, fs, never).queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], false);
    await dm.jobs();                                               // running on Wi-Fi, half down
    wait = true;                                                   // Wi-Fi gone: the task sits; the module says reason 3
    jobs.get(1)!.reason = 3;
    const again = background(dm, fs, never, { pollMs: 0, wifiWaitMs: 0 });
    await again.queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], true);   // kept (running)
    const said: string[] = [];
    vi.spyOn(console, "info").mockImplementation((m: string) => { said.push(m); });
    await partDownload({ ...fs, hash: (p) => dm.sha256(fs.uri(p)) }, again.fill)(URL, "b", want);
    expect(fs.files.get("b")).toEqual(BODY);
    expect(log.map((l) => l.split(" ").slice(0, 3).join(" "))).toEqual(
      [`enqueue ${URL} wifi`, "remove 1", `enqueue ${URL} metered`, "claim 2"]);
    expect(said.filter((m) => m.includes("queued again"))).toEqual([
      `[download] ${URL}: a Wi-Fi-only download running, waiting for a network (reason 3) at ${BODY.length >> 1} of ${BODY.length} bytes while mobile data is allowed; queued again to use it`]);
  }, 3_000);

  it("(T756) a short pause of a kept Wi-Fi-only job is waited out (not queued again before stuckMs)", async () => {
    let held = 0, still = false;                                   // paused for two polls, then moving again
    const fs = memFs(), { dm, log, jobs } = fakeDm(fs, { [URL]: BODY }, { hold: () => held-- > 0, still: () => still });
    await background(dm, fs, never).queue([{ url: URL, file: "basemap", bytes: BODY.length, path: "b" }], false);
    await dm.jobs();
    still = true;
    const again = background(dm, fs, never, { pollMs: 0, wifiWaitMs: 60_000 });
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

describe("(T722) a relaunch re-attaches a job already part way down", () => {
  it("the bar's first word is what the job already holds, not zero; nothing is queued again", async () => {
    const fs = memFs(), URL = "https://r2.test/release/basemap.pmtiles";
    const half = BODY.length >> 1;
    let polls = 0;
    const enqueue = vi.fn(async () => 99);
    // iOS's module after a relaunch: the job is restored with the bytes it had (the record kept by the delegate's
    // progress), running, then done on the next poll
    const dm: BackgroundNative = {
      // (fill asks once to find the job, then polls it)
      jobs: async () => { const on = ++polls <= 2;
        return [{ id: 7, url: URL, status: on ? "running" : "done", reason: 0, bytes: on ? half : BODY.length,
                  total: BODY.length, metered: true, present: true }]; },
      enqueue, remove: async () => {},
      claim: async (_id, to) => { fs.files.set(to.replace("file:///doc/", ""), BODY); },
      sha256: async (uri) => sha256Hex(fs.files.get(uri.replace("file:///doc/", ""))!),
    };
    const seen: number[] = [];
    const bg = background(dm, fs, async () => { throw new Error("no fallback"); }, { pollMs: 1 });
    await partDownload({ ...fs, hash: (p) => dm.sha256(fs.uri(p)) }, bg.fill)(URL, "f",
      { bytes: BODY.length, sha256: sha256Hex(BODY) }, (n) => seen.push(n));
    expect(seen[0]).toBe(half);
    expect(enqueue).not.toHaveBeenCalled();
    expect(fs.files.get("f")).toEqual(BODY);
  });
});

describe("background (T760): a system job that does not move is queued again, a few times, then the install fails", () => {
  const MIN = 60_000;
  const item = { url: URL, file: "basemap", bytes: BODY.length, path: "b" };
  /** each poll a minute later on a fake clock; the fill's own waits are real (pollMs 0) */
  const fakeClock = () => vi.useFakeTimers({ toFake: ["Date"], now: 0 });
  afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });
  const run = async (o: Parameters<typeof fakeDm>[2], opts: Parameters<typeof background>[3] = {}) => {
    fakeClock();
    const fs = memFs(), f = fakeDm(fs, { [URL]: BODY }, { tick: MIN, ...o });
    const said: string[] = [];
    vi.spyOn(console, "info").mockImplementation((m: string) => { said.push(m); });
    vi.spyOn(console, "warn").mockImplementation((m: string) => { said.push(m); });
    const bg = background(f.dm, fs, never, { pollMs: 0, ...opts });
    await bg.queue([item], true);
    const done = partDownload({ ...fs, hash: (p) => f.dm.sha256(fs.uri(p)) }, bg.fill)(URL, "b", want);
    return { ...f, fs, said, done };
  };
  const steps = (log: string[]) => log.map((l) => l.split(" ").slice(0, 2).join(" "));

  it("a job paused 'waiting to retry' with no new byte for stuckMs is removed and queued again, said whole in the log", async () => {
    // the first job sits paused (reason 1, PAUSED_WAITING_TO_RETRY) at 7 bytes; the one queued again runs
    const r = await run({ hold: (j) => j.id === 1 && ((j.reason = 1), (j.bytes = 7), true) });
    await r.done;
    expect(r.fs.files.get("b")).toEqual(BODY);
    expect(steps(r.log)).toEqual([`enqueue ${URL}`, "remove 1", `enqueue ${URL}`, "claim 2"]);
    expect(r.said.filter((m) => m.includes("queued again"))).toEqual([
      `[download] ${URL}: not moved for 600 s (paused, reason 1, 7 of ${BODY.length} bytes); removed and queued again from zero (1 of 2): the system cannot resume a removed download, those bytes are lost`]);
  });

  it("a job honestly waiting for a network (reason 2) or for Wi-Fi (reason 3) is never queued again, however long", async () => {
    for (const reason of [2, 3]) {
      let polls = 0;                                             // paused for two hours, then the network is back
      const r = await run({ hold: (j) => polls++ < 120 && ((j.reason = reason), (j.bytes = 7), true) });
      await r.done;
      expect(r.fs.files.get("b")).toEqual(BODY);
      expect(steps(r.log)).toEqual([`enqueue ${URL}`, "claim 1"]);
      vi.useRealTimers(); vi.restoreAllMocks();
    }
  });

  it("(iPhone) a Wi-Fi-only job PENDING while it waits for Wi-Fi (reason 3), Wi-Fi only asked, is never queued again, however long", async () => {
    // the iPhone's background session holds a Wi-Fi-only task on mobile data without a word: the module reports it
    // pending with the reason its network path gives. Two hours on mobile data, then Wi-Fi: the same job runs.
    fakeClock();
    const fs = memFs();
    let polls = 0;
    const f = fakeDm(fs, { [URL]: BODY }, { tick: MIN, still: () => polls++ < 120 });
    const said: string[] = [];
    vi.spyOn(console, "info").mockImplementation((m: string) => { said.push(m); });
    vi.spyOn(console, "warn").mockImplementation((m: string) => { said.push(m); });
    const bg = background(f.dm, fs, never, { pollMs: 0 });
    await bg.queue([item], false);
    for (const j of f.jobs.values()) j.reason = 3;
    await partDownload({ ...fs, hash: (p) => f.dm.sha256(fs.uri(p)) }, bg.fill)(URL, "b", want);
    expect(fs.files.get("b")).toEqual(BODY);
    expect(steps(f.log)).toEqual([`enqueue ${URL}`, "claim 1"]);
    expect(said.filter((m) => m.includes("queued again") || m.includes("fails"))).toEqual([]);
    expect(said[0]).toBe(`[download] ${URL}: pending, waiting for a network (reason 3), 0 of ${BODY.length} bytes`);
  });

  it("(iPhone) the same pending Wi-Fi-only job with no network-wait reason (the phone is on Wi-Fi) is still queued again after stuckMs", async () => {
    fakeClock();
    const fs = memFs();
    const box: { log?: string[] } = {};
    const f = fakeDm(fs, { [URL]: BODY }, { tick: MIN, still: () => (box.log ?? []).filter((l) => l.startsWith("enqueue")).length < 2 });
    box.log = f.log;
    vi.spyOn(console, "info").mockImplementation(() => {});
    const bg = background(f.dm, fs, never, { pollMs: 0 });
    await bg.queue([item], false);
    await partDownload({ ...fs, hash: (p) => f.dm.sha256(fs.uri(p)) }, bg.fill)(URL, "b", want);
    expect(steps(f.log)).toEqual([`enqueue ${URL}`, "remove 1", `enqueue ${URL}`, "claim 2"]);
  });

  it("bytes that move start the clock again: a job that crawls is never queued again", async () => {
    let polls = 0;                                               // paused between tries, a byte more every 5 minutes, 40 minutes long
    const r = await run({ hold: (j) => polls++ < 40 && ((j.reason = 1), (j.bytes = Math.floor(polls / 5)), true) });
    await r.done;
    expect(steps(r.log)).toEqual([`enqueue ${URL}`, "claim 1"]);
  });

  it("a pending job the system never starts is treated the same", async () => {
    // nothing moves until the job is queued a second time (then the fake starts it)
    const box: { log?: string[] } = {};
    const r = await run({ still: () => (box.log ?? []).filter((l) => l.startsWith("enqueue")).length < 2 });
    box.log = r.log;
    await r.done;
    expect(steps(r.log)).toEqual([`enqueue ${URL}`, "remove 1", `enqueue ${URL}`, "claim 2"]);
    expect(r.said.filter((m) => m.includes("queued again"))).toEqual([
      `[download] ${URL}: not moved for 600 s (pending, reason 0, 0 of ${BODY.length} bytes); removed and queued again from zero (1 of 2): the system cannot resume a removed download, those bytes are lost`]);
  });

  it("after stuckTries re-queues that again do not move, the install fails with the reader's sentence (never an endless wait)", async () => {
    const r = await run({ hold: (j) => ((j.reason = 1), true) });
    await expect(r.done).rejects.toThrow(new Error(INSTALL_WORDS.stuck));
    expect(INSTALL_WORDS.stuck).toBe("A file would not download after several tries. Check your connection, then tap Try again.");
    expect(steps(r.log)).toEqual([`enqueue ${URL}`, "remove 1", `enqueue ${URL}`, "remove 2", `enqueue ${URL}`]);
    expect(r.said.at(-1)).toBe(`[download] ${URL}: not moved for 600 s (paused, reason 1, 0 of ${BODY.length} bytes) after 2 times queued again; the install fails`);
  }, 10_000);

  it("both times are options: stuckMs and stuckTries", async () => {
    const r = await run({ hold: (j) => ((j.reason = 4), true) }, { stuckMs: 3 * MIN, stuckTries: 0 });
    await expect(r.done).rejects.toThrow(INSTALL_WORDS.stuck);
    expect(r.said.at(-1)).toBe(`[download] ${URL}: not moved for 180 s (paused, reason 4, 0 of ${BODY.length} bytes) after 0 times queued again; the install fails`);
  });
});

describe("background (T912): an update's data files go to the system's downloader too", () => {
  afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });
  const RAW = new TextEncoder().encode("the regulations, pretend (a sqlite file)");
  const DATA = "https://r2.test/r-2/core-abc.sqlite.gz";
  const expectRaw = { bytes: RAW.length, sha256: sha256Hex(RAW) };
  const item = { url: DATA, file: "data", bytes: RAW.length, path: "release/r-2/core-abc.sqlite", whole: true };
  const inAppLog = () => {
    const asked: string[] = [];
    return { asked, inApp: async (url: string) => { asked.push(url); return RAW; } };
  };

  it("queued with the notification's words, its job claimed beside the install path, unpacked and verified: the app downloads nothing", async () => {
    vi.spyOn(console, "info").mockImplementation(() => {});
    // the stored gzip as the host keeps it (a client asking for the bytes as they are may get these, or the decoded file)
    for (const body of [gzipSync(RAW), RAW]) {
      const fs = memFs(), { dm, log } = fakeDm(fs, { [DATA]: body });
      const bg = background(dm, fs, never, { pollMs: 0 });
      await bg.queue([item], false);
      expect(log).toEqual([`enqueue ${DATA} wifi Downloading BC fishing rules / Regulations data · 0.0 MB (core-abc.sqlite.gz-${RAW.length})`]);
      const { asked, inApp } = inAppLog();
      expect(await bg.fetch(DATA, expectRaw, inApp)).toEqual(RAW);
      expect(asked).toEqual([]);
      expect(log.slice(1)).toEqual(["claim 1"]);
      expect([...fs.files.keys()]).toEqual([]);                   // the claimed file is not left beside the install path
    }
  });

  it("a file that was not queued (a first install's data file) is downloaded in the app, as before", async () => {
    const fs = memFs(), { dm, log } = fakeDm(fs, {});
    const { asked, inApp } = inAppLog();
    expect(await background(dm, fs, never).fetch(DATA, expectRaw, inApp)).toEqual(RAW);
    expect(asked).toEqual([DATA]);
    expect(log).toEqual([]);
  });

  it("a job that failed, or brought other bytes, is given up for the app's own download, and the log says why (W46)", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "info").mockImplementation(() => {});
    const fs = memFs(), { dm, log } = fakeDm(fs, { [DATA]: RAW }, { failFirst: true });
    const bg = background(dm, fs, never, { pollMs: 0 });
    await bg.queue([item], true);
    const { asked, inApp } = inAppLog();
    expect(await bg.fetch(DATA, expectRaw, inApp)).toEqual(RAW);
    expect(asked).toEqual([DATA]);
    expect(log.slice(1)).toEqual(["remove 1"]);
    expect(String(warn.mock.calls.at(-1)?.[0])).toBe(`[download] ${DATA}: its system download failed (reason 404); downloaded in the app instead`);

    const other = memFs(), o = fakeDm(other, { [DATA]: new TextEncoder().encode("another file") });
    const bg2 = background(o.dm, other, never, { pollMs: 0 });
    await bg2.queue([item], true);
    const b = inAppLog();
    const reported: unknown[] = [];
    const unsink = setFailureSink((e) => reported.push(e));
    try { expect(await bg2.fetch(DATA, expectRaw, b.inApp)).toEqual(RAW); } finally { unsink(); }
    expect(reported.map(String)).toEqual([expect.stringMatching(/^Error: .*core-abc\.sqlite\.gz: 12 bytes, the manifest says \d+$/)]);
    expect(b.asked).toEqual([DATA]);
    expect(String(warn.mock.calls.at(-1)?.[0])).toMatch(/its system download brought other bytes \(.*the manifest says \d+\); downloaded in the app instead$/);
    expect([...other.files.keys()]).toEqual([]);
  });

  it("(T919 d) a job cut part way (paused 'waiting to retry') is waited on and RESUMED by the system, not fetched again from zero", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "info").mockImplementation(() => {});
    let polls = 0;
    // the job is cut at 7 bytes (reason 1) for three polls, then the system's retry resumes it and it finishes
    const fs = memFs(), { dm, log } = fakeDm(fs, { [DATA]: RAW }, { hold: (j) => {
      if (j.status !== "pending") { j.reason = 1; j.bytes = 7; }
      return j.status !== "pending" && polls++ < 3;
    } });
    const bg = background(dm, fs, never, { pollMs: 0 });
    await bg.queue([item], true);
    await dm.jobs();                                               // under way: running
    const a = inAppLog();
    expect(await bg.fetch(DATA, expectRaw, a.inApp)).toEqual(RAW);
    expect(a.asked).toEqual([]);                                   // the app downloaded nothing
    expect(log.slice(1)).toEqual(["claim 1"]);                     // the same job, never removed or queued again
    // waiting for a network (reason 2) is waited on the same way
    polls = 0;
    const fs2 = memFs(), o = fakeDm(fs2, { [DATA]: RAW }, { hold: (j) => { if (j.status !== "pending") j.reason = 2; return j.status !== "pending" && polls++ < 3; } });
    const bg2 = background(o.dm, fs2, never, { pollMs: 0 });
    await bg2.queue([item], true);
    const b = inAppLog();
    expect(await bg2.fetch(DATA, expectRaw, b.inApp)).toEqual(RAW);
    expect(b.asked).toEqual([]);
    expect(o.log.slice(1)).toEqual(["claim 1"]);
  });

  it("(T919 d) a cut job that has not moved again after cutWaitMs is given up for the app's own download, said whole", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "info").mockImplementation(() => {});
    const fs = memFs(), { dm, log } = fakeDm(fs, { [DATA]: RAW }, { tick: 30_000, hold: (j) => {
      if (j.status !== "pending") { j.reason = 1; j.bytes = 7; }
      return j.status !== "pending";
    } });
    const bg = background(dm, fs, never, { pollMs: 0 });
    await bg.queue([item], true);
    await dm.jobs();                                               // under way, then cut for good
    const a = inAppLog();
    expect(await bg.fetch(DATA, expectRaw, a.inApp)).toEqual(RAW);
    expect(a.asked).toEqual([DATA]);
    expect(log.slice(1)).toEqual(["remove 1"]);
    expect(String(warn.mock.calls.at(-1)?.[0])).toBe(`[download] ${DATA}: its system download stopped part way (paused, reason 1, 7 bytes) and did not resume in 90 s; downloaded in the app instead`);
  });

  it("a job the system parks with no new byte for stuckMs is given up for the app's own download, never waited on for ever", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "info").mockImplementation(() => {});
    const fs = memFs(), { dm } = fakeDm(fs, { [DATA]: RAW }, { still: () => true, tick: 60_000 });
    const bg = background(dm, fs, never, { pollMs: 0 });
    await bg.queue([item], true);
    const { asked, inApp } = inAppLog();
    expect(await bg.fetch(DATA, expectRaw, inApp)).toEqual(RAW);
    expect(asked).toEqual([DATA]);
  });
});
