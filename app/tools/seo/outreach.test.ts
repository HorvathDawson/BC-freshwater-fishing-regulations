/**
 * T197: the outreach kit is drafts only. Every template says the app is a free non-profit and asks for feedback, never for
 * links, shares or votes; the folder holds documents only (no script that could send or post anything).
 */
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const DIR = join(dirname(fileURLToPath(import.meta.url)), "../../../handoff/PLAN/seo/outreach");
const TEMPLATES = ["club-email.md", "ffsbc-email.md", "forum-post.md", "reddit-post.md", "shop-counter-card.md"];
/** the file with its hard line wraps joined (a section break "---" stays on its own line) */
const read = (f: string) => readFileSync(join(DIR, f), "utf8").replace(/[ \t]*\n(?!---|\n)/g, " ");

describe("outreach kit (T197)", () => {
  it("holds documents only: the five templates, a README and a tracker; nothing runnable", () => {
    expect(readdirSync(DIR).sort()).toEqual(["README.md", "club-email.md", "ffsbc-email.md", "forum-post.md", "reddit-post.md", "shop-counter-card.md", "tracker.csv"]);
  });

  for (const f of TEMPLATES) {
    it(`${f}: free and non-profit, asks for feedback, and not a legal source`, () => {
      const t = read(f);
      expect(t).toMatch(/\bfree\b/i);
      expect(t).toMatch(/non-profit/i);
      expect(t).toMatch(/feedback|tell .* if anything is wrong|Spot a mistake/i);
      expect(t).toMatch(/not a legal/i);
    });

    it(`${f}: never asks for a link, a share, a vote or a review`, () => {
      const body = read(f).split("\n---\n")[0]!.split("Notes for the sender")[0]!;
      expect(body).not.toMatch(/\b(please|kindly|could you|would you)\b[^.?\n]{0,40}\b(link|share|upvote|vote|like|retweet|review|post about|mention)\b/i);
      expect(body).not.toMatch(/\bbacklink|link to (us|the site)|upvote/i);
    });
  }

  it("the README states the rules: no scripts, no auto-posting, no link schemes", () => {
    const r = read("README.md");
    expect(r).toMatch(/No scripts, no repeated posting, no sockpuppets, no incentivised votes/);
    expect(r).toMatch(/owner sends every message by hand/);
  });
});
