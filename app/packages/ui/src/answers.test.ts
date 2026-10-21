/**
 * Answers asked once and peeked synchronously — the store under the Conditions chart's gauge switch (owner
 * 2026-10-21: "it flickers a millisecond before showing the chart").
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ANSWER_CAP, ANSWER_TTL_MS, askOnce, chartParameter, clearAnswers, peekAnswer, seriesKey } from "./answers";

beforeEach(clearAnswers);

describe("askOnce", () => {
  it("asks once: a second asker shares the first's promise, and the settled answer can be peeked", async () => {
    const src = {};
    const run = vi.fn(async () => "flow");
    const a = askOnce(src, "k", run, ANSWER_TTL_MS, 1000);
    const b = askOnce(src, "k", run, ANSWER_TTL_MS, 1001);
    expect(a).toBe(b);
    expect(peekAnswer(src, "k", ANSWER_TTL_MS, 1001)).toBeNull();      // in flight: nothing to peek yet
    await a;
    expect(run).toHaveBeenCalledTimes(1);
    expect(peekAnswer(src, "k", ANSWER_TTL_MS, 1002)).toEqual({ value: "flow" });
  });

  it("asks again once the entry is older than its reuse window", async () => {
    const src = {};
    const run = vi.fn(async () => "x");
    await askOnce(src, "k", run, ANSWER_TTL_MS, 0);
    expect(peekAnswer(src, "k", ANSWER_TTL_MS, ANSWER_TTL_MS)).toBeNull();
    await askOnce(src, "k", run, ANSWER_TTL_MS, ANSWER_TTL_MS);
    expect(run).toHaveBeenCalledTimes(2);
  });

  it("never keeps a failure: the next asker tries again", async () => {
    const src = {};
    const run = vi.fn().mockRejectedValueOnce(new Error("offline")).mockResolvedValueOnce("back");
    await expect(askOnce(src, "k", run, ANSWER_TTL_MS, 0)).rejects.toThrow("offline");
    expect(peekAnswer(src, "k", ANSWER_TTL_MS, 1)).toBeNull();
    await expect(askOnce(src, "k", run, ANSWER_TTL_MS, 1)).resolves.toBe("back");
  });

  it("keeps answers per source, and at most ANSWER_CAP of them (least recently asked go first)", async () => {
    const one = {}, two = {};
    await askOnce(one, "k", async () => 1, ANSWER_TTL_MS, 0);
    expect(peekAnswer(two, "k", ANSWER_TTL_MS, 0)).toBeNull();
    for (let i = 0; i < ANSWER_CAP; i++) await askOnce(one, `n${i}`, async () => i, ANSWER_TTL_MS, 0);
    expect(peekAnswer(one, "k", ANSWER_TTL_MS, 0)).toBeNull();
    expect(peekAnswer(one, "n0", ANSWER_TTL_MS, 0)).toEqual({ value: 0 });
  });
});

describe("the chart's question", () => {
  it("is one key for the chart, its model chips and the prefetch", () => {
    expect(seriesKey("08MH001" as never, "year", "level", "ELF")).toBe("series:08MH001:year:level:ELF");
    expect(seriesKey("08MH001" as never, "72h", undefined, undefined)).toBe("series:08MH001:72h:auto:auto");
  });
  it("charts the picked quantity where the station measures it, else the station's own", () => {
    expect(chartParameter("discharge", ["discharge", "level"])).toBe("discharge");
    expect(chartParameter("discharge", ["level"])).toBeUndefined();
    expect(chartParameter(null, ["discharge"])).toBeUndefined();
  });
});
