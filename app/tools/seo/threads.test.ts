import { describe, expect, it } from "vitest";
import { threadCounts } from "./threads";

const GB = 1024 ** 3;

describe("T917: how many threads the SEO build runs", () => {
  it("auto keeps the cores busy: render threads plus about 0.6 of a job thread each", () => {
    const c = threadCounts("auto", 34 * GB, 48 * GB, 18);
    expect(c).toMatchObject({ render: 10, jobs: 6 });
    expect(c.said).toMatch(/^10 render \+ 6 job threads \(--threads auto: 34\.0 GB free of 48\.0, 18 cores\)$/);
  });
  it("a number asks for that many render threads", () => {
    expect(threadCounts("4", 34 * GB, 48 * GB, 18)).toMatchObject({ render: 4, jobs: 3 });
  });
  it("is held to what the free memory pays for, and says so", () => {
    // 16 GB free of 48: 16 - 4.8 kept - 4.5 main = 6.7 GB → 5 render threads at 1.3 GB
    const c = threadCounts("auto", 16 * GB, 48 * GB, 18);
    expect(c.render).toBe(5);
    expect(c.said).toMatch(/held to 5 by memory/);
    expect(threadCounts("12", 16 * GB, 48 * GB, 18).render).toBe(5);
  });
  it("never runs fewer than two job threads", () => {
    expect(threadCounts("auto", 8 * GB, 8 * GB, 2)).toMatchObject({ render: 1, jobs: 2 });
  });
  it("refuses when the free memory fits no render thread, naming the free memory and the budget", () => {
    // 6 GB free of 48: 6 - 4.8 kept - 4.5 main < 1.3
    expect(() => threadCounts("auto", 6 * GB, 48 * GB, 18)).toThrow(/fits no render thread \(6\.0 GB free of 48\.0: 1\.3 GB a render thread, 4\.5 GB main, 10 % kept free\)/);
    expect(() => threadCounts("1", 6 * GB, 48 * GB, 18)).toThrow(/fits no render thread/);
  });
  it("refuses a count that is not a whole number ≥ 1", () => {
    for (const bad of ["0", "-2", "1.5", "lots"]) expect(() => threadCounts(bad, 34 * GB, 48 * GB, 18)).toThrow(/--threads/);
  });
});
