// T178: the monthly model score. PARITY: the PYTHON scorer (pipeline/gauges/feed/skill.py via
// parity/gauges/skill_harness.py) and the TS port (src/gauges/score.ts `runSkill`) on one generated fixture in the
// worker's archive layout (a month file, day files, parts listed in runs/state.json, a torn line, other months that
// must not be read). Same table and verdict, numbers to 1e-12 (libm). Skipped where the repo venv is absent.
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { dirStore, firstDiff, needPython, PY, REPO, FEEDS } from "../parity/gauges/node";
import { band, prevMonth, runSkill, skillText, verdict } from "../src/gauges/score";

const HARNESS = join(FEEDS, "parity/gauges/skill_harness.py");
const MONTH = "2026-10";
const NOW = new Date("2026-11-01T10:55:00Z"); // the cron: 02:55 on the 1st, Centre time
const W = { CLEVER: 1.0, COFFEE: 1.0 };

function rng(seed: number) {
  return () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const day = (y: number, m: number, d: number) => new Date(Date.UTC(y, m - 1, d)).toISOString().slice(0, 10);
const addD = (s: string, n: number) => new Date(Date.parse(s) + n * 86_400_000).toISOString().slice(0, 10);

/** Write the fixture: `n` stations, weekday issue days of October 2026, CLEVER (wide error), COFFEE (tighter, a
 * scale, sometimes none), ELF; some issue days with two runs per model (the later one counts). */
function fixture(dir: string, n: number, issueDays: number) {
  const r = rng(178);
  const stations = Array.from({ length: n }, (_, i) => `08X${String(i).padStart(4, "0")}`);
  const truth = (st: number, d: string) => 20 + st * 7 + 10 * Math.sin(Date.parse(d) / 86_400_000 / 5 + st);
  const feed = join(dir, "feed");
  const runs = join(dir, "priv/runs");
  mkdirSync(feed, { recursive: true });
  mkdirSync(runs, { recursive: true });
  // observed: Sep 20 – Nov 12 (a few null flows, one station without a file)
  stations.forEach((st, i) => {
    if (i === n - 1) return;
    const daily: unknown[][] = [];
    for (let d = "2026-09-20"; d <= "2026-11-12"; d = addD(d, 1)) daily.push([d, 1.2, r() < 0.03 ? null : +(truth(i, d) * (0.95 + 0.1 * r())).toFixed(3)]);
    writeFileSync(join(feed, `${st}.json`), JSON.stringify({ station: st, daily }));
  });
  // clim: a discharge envelope per pentad for all but station 1
  const clim: Record<string, any> = { release: "2025", stations: {} };
  stations.forEach((st, i) => {
    if (i === 1) return;
    const env: Record<string, number[]> = {};
    for (let p = 0; p < 73; p++) {
      const m = 20 + i * 7;
      env[String(p)] = [m * 0.6, m * 0.8, m, m * 1.25, m * 1.6];
    }
    clim.stations[st] = { discharge: env };
  });
  writeFileSync(join(feed, "clim.json"), JSON.stringify(clim));
  // issue days
  const days: string[] = [];
  for (let d = 1; d <= 31 && days.length < issueDays; d++) {
    const s = day(2026, 10, d);
    const wd = new Date(s).getUTCDay();
    if (wd !== 0 && wd !== 6) days.push(s);
  }
  const lines: Array<[string, string]> = []; // [issue day, line]
  for (const d0 of days) {
    stations.forEach((st, i) => {
      for (const [model, hh, spread, bias] of [["CLEVER", "10", 0.5, 0.15], ["COFFEE", "11", 0.2, 0.0], ["ELF", "09", 0.6, -0.2]] as const) {
        if (model === "COFFEE" && r() < 0.15) continue; // COFFEE only when rain is on
        const reps = r() < 0.2 ? 2 : 1;
        for (let k = 0; k < reps; k++) {
          const ds: Record<string, number> = {};
          for (let lead = 0; lead <= 10; lead++) {
            const dd = addD(d0, lead);
            ds[dd] = +(truth(i, dd) * 2 ** (bias + spread * (r() * 2 - 1) * (1 + lead / 10))).toFixed(4);
          }
          if (r() < 0.02) ds[addD(d0, 3)] = 0; // a zero day is never scored
          const scale = model === "COFFEE" ? (r() < 0.1 ? null : +(0.8 + 0.4 * r()).toFixed(6)) : 1.0;
          const issuedAt = `${d0}T${k ? String(Number(hh) + 3) : hh}:30:00-08:00`;
          lines.push([d0, JSON.stringify({ at: `${d0}T20:05:00Z`, days: ds, issuedAt, model, observed: 10, scale, station: st })]);
        }
      }
    });
  }
  // the layout: first third in the month file, next third as day files, the rest as parts (+ a torn line)
  const third = Math.ceil(days.length / 3);
  const state = { v: 1, last: {}, days: [] as string[], parts: [] as string[] };
  const put = (key: string, body: string) => {
    mkdirSync(dirname(join(dir, "priv", key)), { recursive: true });
    writeFileSync(join(dir, "priv", key), body);
  };
  put(`runs/${MONTH}.jsonl`, lines.filter(([d]) => days.indexOf(d) < third).map(([, l]) => l + "\n").join(""));
  for (const d of days.slice(third, 2 * third)) {
    const k = `runs/${MONTH}/${d}.jsonl`;
    put(k, lines.filter(([x]) => x === d).map(([, l]) => l + "\n").join(""));
    state.days.push(k);
  }
  for (const d of days.slice(2 * third)) {
    const ls = lines.filter(([x]) => x === d).map(([, l]) => l);
    const half = Math.ceil(ls.length / 2);
    for (const [j, chunk] of [ls.slice(0, half), ls.slice(half)].entries()) {
      const k = `runs/${MONTH}/${d}/${j ? "203500" : "200500"}.jsonl`;
      put(k, chunk.join("\n") + "\n" + (j ? '{"torn\n' : ""));
      state.parts.push(k);
    }
  }
  // other months: never read (Python's harness concatenates only MONTH too)
  put("runs/2026-09.jsonl", lines.slice(0, 50).map(([, l]) => l + "\n").join(""));
  put("runs/2026-11/2026-11-01/080500.jsonl", lines.slice(0, 50).map(([, l]) => l + "\n").join(""));
  state.parts.push("runs/2026-11/2026-11-01/080500.jsonl");
  put("runs/state.json", JSON.stringify(state));
}

function python(dir: string): any {
  return JSON.parse(execFileSync(PY, [HARNESS, "--dir", dir, "--month", MONTH, "--before", "2026-11-01"], { cwd: REPO, encoding: "utf-8", stdio: ["ignore", "pipe", "inherit"] }));
}

const dirs: string[] = [];
afterAll(() => dirs.forEach((d) => rmSync(d, { recursive: true, force: true })));
const tmp = () => {
  const d = mkdtempSync(join(tmpdir(), "skill-"));
  dirs.push(d);
  return d;
};

describe("model score parity (python vs ts)", () => {
  beforeAll(() => void needPython());   // (T556) no venv is a failure, never a skip
  it.each([
    ["a month with enough cases (verdicts move)", 14, 22],
    ["a thin month (too few)", 4, 3],
  ])("%s", async (_name, n, issueDays) => {
    const dir = tmp();
    fixture(dir, n, issueDays);
    const py = python(dir);
    const ts = await runSkill({ feed: dirStore(join(dir, "feed")), priv: dirStore(join(dir, "priv")), now: NOW, weights: W });
    expect(ts.month).toBe(MONTH);
    expect(ts.before).toBe("2026-11-01");
    const got = { runs: ts.runs, cases: ts.cases, table: ts.table, verdict: ts.verdict };
    expect(firstDiff(py, got)).toBeNull();
    expect(py.cases).toBeGreaterThan(0);
    if (issueDays >= 20) {
      // the fixture's COFFEE is tighter: at least one lead says so, and the report says nothing is applied
      expect(Object.values<any>(py.verdict).some((v) => /^COFFEE better/.test(v.why))).toBe(true);
      expect(ts.differs).toBe(true);
      expect(skillText(ts)).toContain("Nothing changes by itself");
    } else {
      expect(Object.values<any>(py.verdict).every((v) => /^too few/.test(v.why))).toBe(true);
      expect(ts.differs).toBe(false);
    }
  });
});

describe("score pieces", () => {
  it("band edges", () => {
    expect([band(0.2499), band(0.25), band(0.7499), band(0.75)]).toEqual(["low", "normal", "normal", "high"]);
  });
  it("verdict wording spells floats as Python does", () => {
    const row = { issueDays: 25, paired: { difference: { n: 300, mean: 0.0, se: 0.01 }, CLEVER: { mae: 1 }, COFFEE: { mae: 0.5 } } };
    expect(verdict(row).why).toBe("neither better: difference 0.0 within 2 x 0.01");
    expect(verdict({ ...row, paired: { ...row.paired, difference: { n: 300, mean: 0.2, se: 0.01 } } })).toEqual({ weights: { CLEVER: 0.5, COFFEE: 1 }, why: "COFFEE better: difference 0.2, se 0.01" });
  });
  it("the month before", () => {
    expect([prevMonth("2026-01"), prevMonth("2026-11")]).toEqual(["2025-12", "2026-10"]);
  });
});
