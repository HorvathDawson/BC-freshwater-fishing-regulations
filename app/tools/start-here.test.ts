// (T437, T1081) ONE START-HERE, ONE SHORT PAGE PER LIVE CHAT. `docs/agents/START-HERE.md` is a pointer of at most 30
// lines; it links every page of `docs/agents/restart/` (one short page per live chat, at most 60 lines, deleted when its
// chat ends) and nothing that is missing; the only restart pages left elsewhere are the four named in LEGACY_RESTART (their
// chats edit them where they are, until the rename commit moves them). The guard itself is `check-tickets.mjs` checkDocs;
// this test holds the same facts on the real files, in its own words, so a guard that goes quiet shows here. The
// agent-instructions file only points at AGENTS.md.
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { LEGACY_RESTART, RESTART_MAX, START_HERE_MAX } from "./check-tickets.mjs";

const REPO = new URL("../../", import.meta.url).pathname;
const PLAN = join(REPO, "docs");
const AGENTS = join(PLAN, "agents");
const lines = (text: string) => text.replace(/\n+$/, "").split("\n").length;

describe("one start-here, one page per live chat (T437, T1081)", () => {
  const start = readFileSync(join(AGENTS, "START-HERE.md"), "utf8");
  const pages = readdirSync(join(AGENTS, "restart")).filter((f) => !f.startsWith("."));

  it("START-HERE.md is a pointer: at most 30 lines", () => {
    expect(lines(start)).toBeLessThanOrEqual(START_HERE_MAX);
    expect(START_HERE_MAX).toBe(30);
  });

  it("START-HERE.md links every restart page, and every page it links exists", () => {
    expect(pages.length).toBeGreaterThan(0);
    const linked = [...start.matchAll(/docs\/agents\/restart\/([\w.-]+\.md)/g)].map((m) => m[1]!);
    expect([...new Set(linked)].sort()).toEqual([...pages].sort());
    for (const n of linked) expect([n, existsSync(join(AGENTS, "restart", n))]).toEqual([n, true]);
  });

  it("each restart page is at most 60 lines", () => {
    expect(RESTART_MAX).toBe(60);
    for (const n of pages) expect([n, lines(readFileSync(join(AGENTS, "restart", n), "utf8")) <= 60]).toEqual([n, true]);
  });

  it("no restart page is left outside restart/ except the four named ones; NEXT-SESSION pages are gone", () => {
    const found = readdirSync(PLAN, { recursive: true }).map(String).filter((p) => /^(RESTART|NEXT-SESSION)[^/]*\.md$/.test(p.split("/").pop()!))
      .map((p) => `docs/${p}`).filter((p) => !p.startsWith("docs/agents/restart/"));
    expect(found.filter((p) => !LEGACY_RESTART.includes(p))).toEqual([]);
    expect(LEGACY_RESTART).toHaveLength(4);
    expect(readdirSync(PLAN).filter((f) => /^NEXT-SESSION.*\.md$/.test(f))).toEqual([]);
  });

  it("the coding-agent instructions file only points at AGENTS.md", () => {
    const s = readFileSync(join(REPO, ".github/copilot-instructions.md"), "utf8");
    expect(s).toContain("AGENTS.md");
    expect(s.split("\n").length).toBeLessThan(12);
  });
});
