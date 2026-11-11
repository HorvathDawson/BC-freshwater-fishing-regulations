// T404: production's feeds worker config (wrangler.prod.toml) is staging's (wrangler.toml) with production's names — the
// same bindings, crons, limits and vars — so the switch deploys the code staging ran, not a hand-kept copy that drifted.
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

type Table = Record<string, string>;
interface Conf { top: Table; vars: Table; buckets: Table[]; limits: Table[]; email: Table[]; crons: string[] }

/** the small subset of TOML these two files use: `key = "v"` / `key = v` lines, [table] and [[array]] headers, one crons list */
function read(file: string): Conf {
  const text = readFileSync(new URL(`../${file}`, import.meta.url), "utf8");
  const crons = [...(/crons = \[([\s\S]*?)\]/.exec(text)?.[1] ?? "").matchAll(/"([^"]+)"/g)].map((m) => m[1]!);
  const c: Conf = { top: {}, vars: {}, buckets: [], limits: [], email: [], crons };
  let cur: Table = c.top;
  for (const raw of text.replace(/crons = \[[\s\S]*?\]/, "").split("\n")) {
    const line = raw.replace(/\s+#.*$/, "").replace(/^#.*$/, "").trim();
    if (!line) continue;
    const h = /^\[\[?([a-z0-9_]+)\]\]?$/.exec(line);
    if (h) {
      const t: Table = {};
      if (h[1] === "vars") c.vars = t;
      else if (h[1] === "r2_buckets") c.buckets.push(t);
      else if (h[1] === "ratelimits") c.limits.push(t);
      else if (h[1] === "send_email") c.email.push(t);
      cur = t;
      continue;
    }
    const kv = /^([A-Za-z_]+)\s*=\s*(.+)$/.exec(line);
    if (kv) cur[kv[1]!] = kv[2]!.replace(/^"(.*)"$/, "$1");
  }
  return c;
}

const S = read("wrangler.toml"), P = read("wrangler.prod.toml");

describe("wrangler.prod.toml is wrangler.toml with production's names (T404)", () => {
  it("the names are production's, and the vars say them", () => {
    expect(P.top.name).toBe("bc-fishing-feeds");
    expect(P.vars).toMatchObject({ FEEDS_ENV: "production", WORKER_NAME: "bc-fishing-feeds",
      STATUS_URL: "https://feeds.canifishthis.ca/status",
      BUCKET_NAME: "bc-fishing-regulations", BACKUP_BUCKET_NAME: "bc-fishing-regulations-backup" });
    expect(P.buckets).toEqual([{ binding: "BUCKET", bucket_name: P.vars.BUCKET_NAME },
                               { binding: "BACKUP", bucket_name: P.vars.BACKUP_BUCKET_NAME }]);
    // nothing in the production file names a staging resource
    expect(readFileSync(new URL("../wrangler.prod.toml", import.meta.url), "utf8")).not.toMatch(/staging"|-staging\b/);
  });

  it("each file's vars name its own worker and buckets", () => {
    for (const c of [S, P]) {
      expect(c.vars.WORKER_NAME).toBe(c.top.name);
      expect(c.buckets.map((b) => b.bucket_name)).toEqual([c.vars.BUCKET_NAME, c.vars.BACKUP_BUCKET_NAME]);
    }
    // the app's report + crash endpoint (env.ts): staging on workers.dev, production on its custom domain (Q171)
    expect(S.vars.STATUS_URL).toBe(`https://${S.top.name}.horvath-dawson.workers.dev/status`);
    expect(S.top.workers_dev).toBe("true");
    expect(P.vars.STATUS_URL).toBe("https://feeds.canifishthis.ca/status");
    expect(P.top.workers_dev).toBe("false");
    expect(P.top.routes).toBe('[{ pattern = "feeds.canifishthis.ca", custom_domain = true }]');
    expect(S.vars.FEEDS_ENV).toBe("staging");
  });

  it("the same code, crons, bindings, limits and vars as staging", () => {
    for (const k of ["main", "compatibility_date", "preview_urls"]) expect(P.top[k]).toBe(S.top[k]);
    expect(P.crons).toEqual(S.crons);
    expect(P.crons.length).toBeGreaterThan(5);
    expect(P.buckets.map((b) => b.binding)).toEqual(S.buckets.map((b) => b.binding));
    expect(P.email).toEqual(S.email);
    const lim = (c: Conf) => c.limits.map(({ name, simple }) => ({ name, simple }));
    expect(lim(P)).toEqual(lim(S));
    expect(Object.keys(P.vars).sort()).toEqual(Object.keys(S.vars).sort());
    for (const k of ["NOTIFY_EMAIL_FROM", "NOTIFY_DIGEST"]) expect(P.vars[k]).toBe(S.vars[k]);
  });

  it("production's rate-limit counters are its own (a namespace id is account-wide)", () => {
    const ids = (c: Conf) => c.limits.map((l) => l.namespace_id);
    expect(ids(P).filter((i) => ids(S).includes(i))).toEqual([]);
  });
});

// T609 W7: no run-token file is ever committable (the rotation's old token once sat in an un-ignored .run-token.prev)
describe("token files are git-ignored (T609 W7)", () => {
  it("every .run-token* and .dev.vars is ignored; the rotate script writes no .prev", async () => {
    const { execFileSync } = await import("node:child_process");
    const dir = new URL("..", import.meta.url).pathname;
    for (const f of [".run-token", ".run-token.prod", ".run-token.prev", ".run-token.anything", ".dev.vars"])
      expect(execFileSync("git", ["check-ignore", f], { cwd: dir, encoding: "utf8" }).trim()).toBe(f);
    const sh = readFileSync(new URL("../../../../deploy/staging/rotate-run-token.sh", import.meta.url), "utf8");
    expect(sh).not.toMatch(/>\s*"\$F\.prev"/);
  });
});
