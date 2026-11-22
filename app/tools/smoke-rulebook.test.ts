// T736 — the smoke's rule-book step against a fake page: no browser, no staging.
import { describe, expect, it } from "vitest";
// @ts-expect-error plain .mjs tool
import { waitForRuleBook, ASKED_FIRST, OFFER_Q, PDF_Q } from "./smoke-rulebook.mjs";

/** a page that shows the size prompt at `offerAt` ms; the PDF opens only after "Download it" (or at `openAt` when no prompt) */
function page({ offerAt = null, openAt = null, pressWorks = true }: { offerAt?: number | null; openAt?: number | null; pressWorks?: boolean }) {
  let clock = 0, downloading = false;
  const b = {
    evaluate: async (expr: string) => {
      if (expr === OFFER_Q) return offerAt !== null && clock >= offerAt && !downloading;
      if (expr === PDF_Q) return downloading || (openAt !== null && clock >= openAt);
      throw new Error(`unexpected ${expr}`);
    },
  };
  const presses: string[] = [];
  return {
    b, presses,
    press: async (_: unknown, label: string) => { presses.push(label); if (pressWorks) downloading = true; return pressWorks; },
    sleep: async (ms: number) => { clock += ms; },
  };
}

describe("(T736) the rule-book step", () => {
  it("a fast line: the PDF opens, no prompt, nothing pressed, nothing said", async () => {
    const p = page({ openAt: 1000 });
    expect(await waitForRuleBook(p.b, p)).toBe("");
    expect(p.presses).toEqual([]);
  });
  it("a slow-connection prompt (the 2026-11-14 failure): pressed like a user would, and said", async () => {
    const p = page({ offerAt: 500 });
    expect(await waitForRuleBook(p.b, p)).toBe(ASKED_FIRST);
    expect(p.presses).toEqual(["Download it"]);
  });
  it("a prompt that cannot be pressed is retried, then fails naming the slow path", async () => {
    const p = page({ offerAt: 0, pressWorks: false });
    await expect(waitForRuleBook(p.b, { ...p, ms: 2000 })).rejects.toThrow(/slow-connection path.*could not be pressed/);
    expect(p.presses.length).toBeGreaterThan(1);
  });
  it("no prompt and no PDF is a real failure, and says there was no prompt", async () => {
    const p = page({});
    await expect(waitForRuleBook(p.b, { ...p, ms: 2000 })).rejects.toThrow(/no slow-connection prompt was showing/);
  });
});
