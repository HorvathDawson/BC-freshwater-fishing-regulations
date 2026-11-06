// (T437) ONE START-HERE. Eight restart pages had piled up in handoff/PLAN (each "replaces" an older one) and a tracked
// `.github/copilot-instructions.md` of generic boilerplate contradicted AGENTS.md. Now `handoff/PLAN/START-HERE.md`
// names the one current restart page (it must exist), at most two restart pages are kept (the current one and the one
// it is replacing), and the agent-instructions file only points at AGENTS.md.
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const REPO = new URL("../../", import.meta.url).pathname;
const PLAN = join(REPO, "handoff/PLAN");

describe("one start-here (T437)", () => {
  it("START-HERE.md names a restart page that exists", () => {
    const names = [...readFileSync(join(PLAN, "START-HERE.md"), "utf8").matchAll(/handoff\/PLAN\/(NEXT-SESSION[\w.-]*\.md)/g)].map((m) => m[1]!);
    expect(names.length).toBeGreaterThan(0);
    for (const n of names) expect([n, existsSync(join(PLAN, n))]).toEqual([n, true]);
  });

  it("at most two restart pages are kept (the current one and the one it replaces)", () => {
    expect(readdirSync(PLAN).filter((f) => /^NEXT-SESSION.*\.md$/.test(f)).length).toBeLessThanOrEqual(2);
  });

  it("the coding-agent instructions file only points at AGENTS.md", () => {
    const s = readFileSync(join(REPO, ".github/copilot-instructions.md"), "utf8");
    expect(s).toContain("AGENTS.md");
    expect(s.split("\n").length).toBeLessThan(12);
  });
});
