// T246: the daily backup copies private/ + the feed state into daily/<day>/ (and monthly/ on the 1st), byte for byte,
// with a sha256 manifest written last; the release and the per-station feed files are not in it.
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { BACKUP_STATUS_KEY, MAX_BYTES, runBackup, type DstBucket, type SrcBucket } from "../src/backup";

type Obj = { body: Uint8Array; meta: Record<string, string> };
const src = (keys: Record<string, string | Uint8Array>, sizes: Record<string, number> = {}) => {
  const m = new Map<string, Obj>(Object.entries(keys).map(([k, v]) => [k, { body: typeof v === "string" ? new TextEncoder().encode(v) : v, meta: { contentType: k.endsWith(".json") ? "application/json" : "text/html" } }]));
  const b: SrcBucket & { m: typeof m } = {
    m,
    async list({ prefix }) {
      const objects = [...m].filter(([k]) => k.startsWith(prefix)).map(([key, o]) => ({ key, size: sizes[key] ?? o.body.byteLength }));
      return { objects, truncated: false };
    },
    async get(k) {
      const o = m.get(k);
      return o ? { arrayBuffer: async () => o.body.slice().buffer, httpMetadata: o.meta } : null;
    },
    async put(k, body, o) {
      m.set(k, { body: new TextEncoder().encode(body), meta: (o?.httpMetadata ?? {}) as any });
    },
  };
  return b;
};
const dst = () => {
  const m = new Map<string, { body: Uint8Array; meta: any; sha256?: string }>();
  const d: DstBucket & { m: typeof m } = {
    m,
    async put(k, body, o) {
      m.set(k, { body: typeof body === "string" ? new TextEncoder().encode(body) : new Uint8Array(body), meta: o?.httpMetadata, sha256: o?.sha256 });
    },
  };
  return d;
};
const sha = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

const STATE = {
  "private/feeds/status.json": '{"feeds":{}}',
  "private/feeds/gauges/runs/2026-10.jsonl": '{"a":1}\n',
  "private/feeds/dfo/raw/region2-eng.html": "<html>dfo</html>",
  "private/reports/2026-10-28/r1.json": '{"id":"r1"}',
  "feeds/dfo/index.json": '{"locs":{}}',
  "feeds/inseason/index.json": '{"rows":[]}',
  "feeds/live/index.json": '{"stations":{}}',
  "feeds/live/clim.json": '{"release":"x"}',
  "feeds/live/08MH006.json": '{"q":1}',
  "release/manifest.json": '{"data_release":"r-1"}',
  "release/r-1/core-abc.json.gz": "gz",
  "runtiming/index.json": '{"tile_version":"t"}',
  "runtiming/paint-1.json.gz": "gz",
};

describe("the daily backup (T246)", () => {
  it("copies exactly the state set into daily/<day>/, bytes and content type kept, manifest hashes match", async () => {
    const s = src(STATE);
    const d = dst();
    const r = await runBackup(s, d, new Date("2026-10-29T09:25:00Z"));
    expect(r.ok).toBe(true);
    expect(r.prefixes).toEqual(["daily/2026-10-29/"]);
    const copied = [...d.m.keys()].filter((k) => !k.endsWith("_manifest.json")).map((k) => k.slice("daily/2026-10-29/".length)).sort();
    expect(copied).toEqual([
      "feeds/dfo/index.json", "feeds/inseason/index.json", "feeds/live/clim.json", "feeds/live/index.json",
      "private/feeds/dfo/raw/region2-eng.html", "private/feeds/gauges/runs/2026-10.jsonl", "private/feeds/status.json",
      "private/reports/2026-10-28/r1.json", "release/manifest.json", "runtiming/index.json",
    ]);
    const man = JSON.parse(new TextDecoder().decode(d.m.get("daily/2026-10-29/_manifest.json")!.body));
    expect(man.format).toBe("backup/1");
    for (const k of copied) {
      const got = d.m.get(`daily/2026-10-29/${k}`)!;
      expect(new TextDecoder().decode(got.body)).toBe((STATE as any)[k]);
      expect(man.objects[k].sha256).toBe(sha(got.body));
      expect(got.sha256).toBe(man.objects[k].sha256); // R2 verifies the put against it
    }
    expect(d.m.get("daily/2026-10-29/private/feeds/dfo/raw/region2-eng.html")!.meta.contentType).toBe("text/html");
    // the per-station feed files and the release's content-named files are not in the daily copy
    expect([...d.m.keys()].some((k) => k.includes("08MH006") || k.includes("core-abc") || k.includes("paint-1"))).toBe(false);
    // the result is kept privately for /status.json
    expect(JSON.parse(new TextDecoder().decode(s.m.get(BACKUP_STATUS_KEY)!.body))).toMatchObject({ ok: true, copied: 10, day: "2026-10-29" });
  });

  it("on the 1st it also writes monthly/<YYYY-MM>/", async () => {
    const d = dst();
    const r = await runBackup(src(STATE), d, new Date("2026-11-01T09:25:00Z"));
    expect(r.prefixes).toEqual(["daily/2026-11-01/", "monthly/2026-11/"]);
    expect(d.m.has("monthly/2026-11/private/feeds/status.json")).toBe(true);
    expect(d.m.has("monthly/2026-11/_manifest.json")).toBe(true);
  });

  it("skips an object over the size cap and names it; a failing put fails the run (no manifest) and says why", async () => {
    const d = dst();
    const r = await runBackup(src(STATE, { "private/feeds/status.json": MAX_BYTES + 1 }), d, new Date("2026-10-29T09:25:00Z"));
    expect(r.skipped).toEqual([{ key: "private/feeds/status.json", why: expect.stringMatching(/bytes >/) }]);
    expect(d.m.has("daily/2026-10-29/private/feeds/status.json")).toBe(false);

    const broken: DstBucket = { put: async (k) => { if (k.includes("runs")) throw new Error("R2 down"); } };
    const s = src(STATE);
    const f = await runBackup(s, broken, new Date("2026-10-29T09:25:00Z"));
    expect(f.ok).toBe(false);
    expect(f.error).toMatch(/R2 down/);
    expect(JSON.parse(new TextDecoder().decode(s.m.get(BACKUP_STATUS_KEY)!.body)).ok).toBe(false);
  });
});
