// T271: reports are deleted 180 days after they are received — 181 days old goes, 179 stays; a delete failure is reported.
import { describe, expect, it } from "vitest";
import { purgeReports, RETENTION_DAYS } from "../src/reports";
import type { R2BucketLike } from "../src/io";

class Mem implements R2BucketLike {
  m = new Map<string, string>();
  failDelete = false;
  async get(k: string) { const v = this.m.get(k); return v === undefined ? null : { text: async () => v }; }
  async put(k: string, b: string) { this.m.set(k, b); }
  async delete(k: string) { if (this.failDelete) throw new Error("r2 down"); this.m.delete(k); }
  async list(o: { prefix: string }) {
    return { objects: [...this.m.keys()].filter((k) => k.startsWith(o.prefix)).sort().map((key) => ({ key })), truncated: false };
  }
}
const NOW = new Date("2026-10-29T09:25:00Z");
const ago = (d: number) => new Date(NOW.getTime() - d * 86_400_000);
const add = (b: Mem, id: string, d: number) =>
  b.m.set(`private/reports/${ago(d).toISOString().slice(0, 10)}/${id}.json`, JSON.stringify({ id, received: ago(d).toISOString() }));

describe("report retention", () => {
  it("is 180 days", () => expect(RETENTION_DAYS).toBe(180));
  it("deletes a 181-day-old report and keeps a 179-day-old one", async () => {
    const b = new Mem();
    add(b, "aaaaaaaaaaaaaaaa", 181); add(b, "bbbbbbbbbbbbbbbb", 179); add(b, "cccccccccccccccc", 0);
    b.m.set("private/reports/limits/abc.json", "{}");
    const r = await purgeReports(b, NOW);
    expect(r).toMatchObject({ ok: true, deleted: 1, kept: 2, failed: 0 });
    expect([...b.m.keys()].some((k) => k.includes("aaaaaaaaaaaaaaaa"))).toBe(false);
    expect([...b.m.keys()].some((k) => k.includes("bbbbbbbbbbbbbbbb"))).toBe(true);
    expect(b.m.has("private/reports/limits/abc.json")).toBe(true);
  });
  it("uses the stored timestamp, not the folder, and ages an unreadable report by its folder", async () => {
    const b = new Mem();
    const day = ago(180).toISOString().slice(0, 10);       // folder day 180 days ago, received late that day: kept
    b.m.set(`private/reports/${day}/dddddddddddddddd.json`, JSON.stringify({ received: `${day}T23:59:00Z` }));
    b.m.set(`private/reports/${ago(185).toISOString().slice(0, 10)}/eeeeeeeeeeeeeeee.json`, "not json");
    const r = await purgeReports(b, NOW);
    expect(r.deleted).toBe(1);
    expect(b.m.size).toBe(1);
  });
  it("reports a failed delete", async () => {
    const b = new Mem(); add(b, "aaaaaaaaaaaaaaaa", 200); b.failDelete = true;
    const r = await purgeReports(b, NOW);
    expect(r).toMatchObject({ ok: false, deleted: 0, failed: 1 });
    expect(r.error).toMatch(/could not be deleted/);
  });
});
