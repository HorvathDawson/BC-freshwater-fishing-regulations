/**
 * (T737, 2026-11-15) THE AHEAD LANE: one file ahead on the network at a time; the reader never waits behind one (a file
 * ahead in flight is aborted for the reader's download unless the reader asked for it, and a file waiting in the lane
 * that the reader asks for starts at once as the reader's); nothing ahead starts while the reader downloads nor within
 * the grace after; an aborted file comes again by itself, its caller never seeing the abort.
 */
import { describe, expect, it } from "vitest";
import { createAheadLane, type LaneWork } from "./lane";

const tick = () => new Promise((r) => setTimeout(r, 0));
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** A work whose download the test finishes; records each start (and whether as the reader's) and each abort. */
function works() {
  const log: string[] = [];
  const open = new Map<string, () => void>();
  const work = (key: string): LaneWork<string> => ({ signal, reader }) => new Promise<string>((ok, no) => {
    log.push(`${key}${reader ? "!" : ""}`);
    signal.addEventListener("abort", () => { log.push(`${key}✗`); const e = new Error("aborted"); e.name = "AbortError"; no(e); });
    open.set(key, () => ok(key));
  });
  const finish = async (key: string) => { open.get(key)!(); open.delete(key); await tick(); };
  return { log, work, finish };
}

describe("(T737) the ahead lane", () => {
  it("one file ahead at a time, in the order asked", async () => {
    const w = works();
    const lane = createAheadLane({ graceMs: 0, joinMs: 0 });
    const got: string[] = [];
    for (const k of ["core", "regs-2", "lines"]) void lane.ahead(k, w.work(k)).then((v) => got.push(v));
    expect(w.log).toEqual(["core"]);
    expect(lane.state()).toMatchObject({ running: "core", waiting: ["regs-2", "lines"] });
    await w.finish("core");
    expect(w.log).toEqual(["core", "regs-2"]);
    await w.finish("regs-2");
    await w.finish("lines");
    expect(got).toEqual(["core", "regs-2", "lines"]);
  });

  it("a tap's download aborts the file ahead it does not need; the file comes again after the reader, same promise", async () => {
    const w = works();
    const lane = createAheadLane({ graceMs: 30, joinMs: 5 });
    let landed: string | null = null;
    void lane.ahead("regs-sid", w.work("regs-sid")).then((v) => { landed = v; });
    const end = lane.reader("water-2");                 // a named tap: the reader's own download starts at once
    await wait(10);
    expect(w.log).toEqual(["regs-sid", "regs-sid✗"]);   // aborted for it, not waited behind
    expect(lane.state()).toMatchObject({ running: null, waiting: ["regs-sid"], aborted: 1 });
    end();
    await wait(15);
    expect(w.log.length).toBe(2);                       // nothing within the grace
    await wait(30);
    expect(w.log).toEqual(["regs-sid", "regs-sid✗", "regs-sid"]);   // again, from the start, behind the reader
    await w.finish("regs-sid");
    expect(landed).toBe("regs-sid");                    // the caller never saw the abort
  });

  it("the file ahead the tap asks for is joined: never aborted", async () => {
    const w = works();
    const lane = createAheadLane({ graceMs: 0, joinMs: 5 });
    void lane.ahead("regs-sid", w.work("regs-sid"));
    const end = lane.reader("down");                    // the unnamed tap's other file
    expect(lane.promote("regs-sid")).toBe("running");   // and regs-sid: the reader's now
    await wait(10);
    expect(w.log).toEqual(["regs-sid"]);
    end();
    await w.finish("regs-sid");
    expect(lane.state()).toMatchObject({ running: null, readers: 0, aborted: 0 });
  });

  it("a file waiting in the lane that the reader asks for starts at once, as the reader's", async () => {
    const w = works();
    const lane = createAheadLane({ graceMs: 1000, joinMs: 5 });
    void lane.ahead("core", w.work("core"));
    void lane.ahead("lines", w.work("lines"));
    expect(w.log).toEqual(["core"]);
    expect(lane.promote("lines")).toBe("started");
    expect(w.log).toEqual(["core", "lines!"]);          // not behind core, not behind the grace
    expect(lane.promote("nope")).toBeNull();
  });

  it("(T767) a file promoted out of the lane is a reader download: the file ahead on the network gives way", async () => {
    const w = works();
    const lane = createAheadLane({ graceMs: 1000, joinMs: 10 });
    void lane.ahead("core", w.work("core"));
    void lane.ahead("lines", w.work("lines"));
    expect(lane.promote("lines")).toBe("started");
    await wait(50);
    expect(w.log).toEqual(["core", "lines!", "core✗"]);
    expect(lane.state()).toMatchObject({ running: null, waiting: ["core"], readers: 1, aborted: 1 });
  });

  it("nothing ahead starts while the reader downloads, nor in the grace after", async () => {
    const w = works();
    const lane = createAheadLane({ graceMs: 20, joinMs: 0 });
    const end = lane.reader("regs-sid");
    void lane.ahead("geo", w.work("geo"));
    await wait(5);
    expect(w.log).toEqual([]);
    end();
    await wait(5);
    expect(w.log).toEqual([]);
    const end2 = lane.reader("water-2");                // the tap's next step: the grace starts again after it
    end2();
    await wait(12);
    expect(w.log).toEqual([]);
    await wait(15);
    expect(w.log).toEqual(["geo"]);
  });

  it("aborted, then asked by the reader (before or after its end lands): it goes again at once as the reader's", async () => {
    for (const late of [false, true]) {
      const w = works();
      const timers: (() => void)[] = [];
      const lane = createAheadLane({ graceMs: 1000, joinMs: 0, setTimer: (f) => { timers.push(f); return 0; }, clearTimer: () => {} });
      let landed: string | null = null;
      void lane.ahead("down", w.work("down")).then((v) => { landed = v; });
      const end = lane.reader("water-2");
      timers.shift()!();                                  // the join wait is over: down is aborted
      if (late) await tick();                             // its end has landed (back in the lane)
      expect(lane.promote("down")).toBe("started");
      await tick();
      expect(w.log).toEqual(["down", "down✗", "down!"]);
      await w.finish("down");
      expect(landed).toBe("down");
      end();
      expect(lane.state().readers).toBe(0);
    }
  });
});
