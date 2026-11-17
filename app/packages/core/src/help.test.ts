/**
 * (T248) The first-run walkthrough's words and the Help page's content: generated from the legend tokens, so a status, a
 * gear mark, a salmon status, a run state or a sync state added without a help sentence fails here.
 */
import { describe, expect, it } from "vitest";
import { GEAR_LEGEND } from "./method";
import { RUN_STATES } from "./runTiming";
import { SALMON_MAP_STATUSES } from "./salmon";
import { WATER_STATUSES } from "./status";
import { GUIDE_WATER, HELP_WORDS, SYNC_KINDS, WALKTHROUGH_VERSION, guideResumeAt, guideSteps, guideText, helpVM,
         walkthroughSeen, withGuideAt, withSeen, WALKTHROUGH_WORDS, type GuideStepWords } from "./help";

describe("the walkthrough (T293)", () => {
  it("(T824) ten steps in the owner's order, every step with words; DO steps are the reader's taps", () => {
    // (T824, owner 2026-11-16: "41 is way to many cut it down to like 8 or 10 items") was 41 on the phone, 29 on the web
    const app = guideSteps("app");
    expect(app.map((s) => s.key)).toEqual(["welcome", "search", "pick", "answer", "day", "conditions", "reading", "leave",
                                           "modes", "done"]);
    expect(app.filter((s) => s.kind === "do").map((s) => s.key)).toEqual(["search", "pick", "day", "conditions", "leave", "modes"]);
    for (const s of app) { expect(s.title.length, s.key).toBeGreaterThan(3); expect(s.body.length, s.key).toBeGreaterThan(20); }
    expect(new Set(app.map((s) => s.key)).size).toBe(app.length);
  });
  it("(T824) the first card says each step HIGHLIGHTS what to tap; no card says ring", () => {
    expect(guideText(guideSteps("app")[0]!.body, { name: GUIDE_WATER.name, letters: GUIDE_WATER.letters }))
      .toBe("We'll look up the Vedder River together. Each step highlights what to tap: tap it, or tap Show me.");
    const all = [...guideSteps("app"), ...guideSteps("web")].flatMap((s) => [s.title, s.body]);
    for (const t of [...all, ...(Object.values(WALKTHROUGH_WORDS) as unknown[]).filter((v): v is string => typeof v === "string")])
      expect(t, t).not.toMatch(/\bring(s|ed)?\b/i);
  });
  it("(T824) one short sentence a card (the first says how the walk works)", () => {
    for (const s of [...guideSteps("app"), ...guideSteps("web")]) {
      const sentences = s.body.split(/(?<=[.!?])\s+/);
      expect(sentences.length, s.key).toBeLessThanOrEqual(s.key === "welcome" ? 2 : 1);
      expect(s.body.split(/\s+/).length, s.body).toBeLessThanOrEqual(20);
    }
  });
  it("the web's walk is the same walk (T302: spots are the phone app's): its last card names Help alone", () => {
    const web = guideSteps("web"), app = guideSteps("app");
    expect(web.map((s) => s.key)).toEqual(app.map((s) => s.key));
    expect(web.filter((s) => s !== app.find((a) => a.key === s.key)).map((s) => s.key)).toEqual(["done"]);
    expect(web.at(-1)!.body).not.toMatch(/Spots/);
    expect(app.at(-1)!.body).toMatch(/Spots/);
  });
  it("names the chosen water, or any water when the release has none by that name", () => {
    const welcome = guideSteps("app")[0]!.body, search = guideSteps("app")[1]!.body;
    expect(guideText(welcome, { name: GUIDE_WATER.name, letters: GUIDE_WATER.letters })).toContain("the Vedder River");
    expect(guideText(search, { name: GUIDE_WATER.name, letters: GUIDE_WATER.letters })).toContain("“ved”");
    expect(guideText(welcome, null)).toContain("a water near you");
    expect(guideText(welcome, null)).not.toMatch(/\{|\}/);
  });
  it("(owner 2026-10-30) everyday words: no app-internal terms, short sentences", () => {
    // (T466) a control named by its own label ("Show regulations for this part") is the app's word, not the walk's
    const unnamed = (s: GuideStepWords, t: string) => (s.names ?? []).reduce((x, n) => x.split(n).join(""), t);
    const all = [...guideSteps("app"), ...guideSteps("web")].flatMap((s) => [s.title, unnamed(s, s.body)]);
    for (const t of all) {
      expect(t, t).not.toMatch(/\b(paint|section|part|parts|verdict|feed|donor|release|synopsis|percentile|horizon|basin)\b/i);
      for (const sentence of t.split(/(?<=[.:!?])\s+/)) expect(sentence.split(/\s+/).length, sentence).toBeLessThanOrEqual(26);
    }
  });
  it("seen only when the stored text is the current version", () => {
    expect([walkthroughSeen(null), walkthroughSeen(""), walkthroughSeen("walkthrough-2"), walkthroughSeen(WALKTHROUGH_VERSION),
            walkthroughSeen(` ${WALKTHROUGH_VERSION}\n`)]).toEqual([false, false, false, true, true]);
  });
  it("keeps where an unfinished walk was left, one line beside the others, and forgets it", () => {
    const s = withGuideAt(withSeen(null, WALKTHROUGH_VERSION), "answer");
    expect([s, guideResumeAt(s)]).toEqual([`${WALKTHROUGH_VERSION}\nguide-at:answer`, "answer"]);
    const moved = withGuideAt(s, "book");
    expect([guideResumeAt(moved), moved.split("\n").length]).toEqual(["book", 2]);
    expect([guideResumeAt(withGuideAt(moved, null)), withGuideAt(moved, null)]).toEqual([null, WALKTHROUGH_VERSION]);
  });
});

describe("the Help page (T248)", () => {
  const rows = helpVM().flatMap((s) => s.rows);
  const keys = rows.map((r) => r.key);

  it("covers every water status, gear mark, salmon status, run state and sync state — no hand-listed set", () => {
    const want = [
      ...WATER_STATUSES.map((s) => `status-${s}`),
      ...GEAR_LEGEND.map((m) => `gear-${m}`),
      ...SALMON_MAP_STATUSES.map((s) => `salmon-${s}`),
      ...RUN_STATES.map((s) => `run-${s}`), "run-none",
      ...SYNC_KINDS.map((k) => `sync-${k}`),
      "tidal", "not-fishing", "bell",
    ];
    for (const k of want) expect(keys, k).toContain(k);
    expect(new Set(keys).size).toBe(keys.length);
  });
  it("the owner's list: closed, bait ban, fly only, no access, tidal area are all there, in words", () => {
    const said = (k: string) => rows.find((r) => r.key === k)!.name;
    expect([said("status-closed"), said("gear-bait"), said("gear-fly"), said("status-no_access"), said("tidal")])
      .toEqual(["Closed", "Bait ban", "Fly fishing only", "No public access", "Tidal water"]);
  });
  it("every row has a name and a sentence; no swatch carries a hex colour (tokens only)", () => {
    for (const r of rows) { expect(r.name.length, r.key).toBeGreaterThan(1); expect(r.text.length, r.key).toBeGreaterThan(10); }
    expect(JSON.stringify(helpVM())).not.toMatch(/#[0-9a-fA-F]{3,8}\b/);
  });
  it("the sync rows carry the icon's own label, kind for kind", () => {
    const sync = rows.filter((r) => r.swatch.kind === "sync").map((r) => r.swatch.kind === "sync" ? r.swatch.sync.kind : null);
    expect(sync).toEqual([...SYNC_KINDS]);
    expect(Object.keys(HELP_WORDS.sync).sort()).toEqual([...SYNC_KINDS].sort());
  });
  it("no access is drawn dotted and the rest solid, as the map draws them", () => {
    const dotted = rows.filter((r) => r.swatch.kind === "status" && r.swatch.dotted).map((r) => r.key);
    expect(dotted).toEqual(["status-no_access"]);
  });
});
