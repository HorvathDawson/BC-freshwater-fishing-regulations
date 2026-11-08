/**
 * (T248) The first-run walkthrough's words and the Help page's content: generated from the legend tokens, so a status, a
 * gear mark, a salmon status, a run state or a sync state added without a help sentence fails here.
 */
import { describe, expect, it } from "vitest";
import { GEAR_LEGEND } from "./method";
import { RUN_STATES } from "./runTiming";
import { SALMON_MAP_STATUSES } from "./salmon";
import { WATER_STATUSES } from "./status";
import { GUIDE_WATER, HELP_WORDS, SYNC_KINDS, TAB_TOURS, WALKTHROUGH_VERSION, guideResumeAt, guideSteps, guideText, helpVM,
         walkthroughSeen, withGuideAt, withSeen, WALKTHROUGH_WORDS, type GuideStepWords } from "./help";

describe("the walkthrough (T293)", () => {
  it("one walk in the owner's order, every step with words; DO steps are the reader's taps", () => {
    const app = guideSteps("app");
    expect(app.filter((s) => s.kind === "do").map((s) => s.key))
      .toEqual(["search", "pick", "map", "regs", "day", "trout", "salmon", "conditions", "forecast", "quantity", "span",
                "partsmap", "upstream", "closedregs", "leave", "layers", "ground", "obstacles",
                // (T466) the real creation of a spot, a tap at a time, then the spot on the map and in Spots
                "spot", "add", "pin", "drop", "water", "when", "name", "save", "show", "open"]);
    for (const s of app) { expect(s.title.length, s.key).toBeGreaterThan(3); expect(s.body.length, s.key).toBeGreaterThan(20); }
    expect(new Set(app.map((s) => s.key)).size).toBe(app.length);
  });
  it("the web's walk has no spot steps (T302: spots are the phone app's) and its last step says no works-offline", () => {
    const web = guideSteps("web"), app = guideSteps("app");
    const spotKeys = ["spot", "add", "pin", "drop", "water", "when", "name", "save", "show", "open", "days", "reasons"];
    expect(web.map((s) => s.key)).toEqual(app.map((s) => s.key).filter((k) => !spotKeys.includes(k)));
    expect(web.filter((s) => s !== app.find((a) => a.key === s.key)).map((s) => s.key)).toEqual(["done"]);
    expect(web.at(-1)!.body).not.toMatch(/offline|no signal|on this phone/);
    expect(app.at(-1)!.body).toMatch(/no signal/);
  });
  it("names the chosen water, or any water when the release has none by that name", () => {
    const welcome = guideSteps("app")[0]!.body, search = guideSteps("app")[1]!.body;
    expect(guideText(welcome, { name: GUIDE_WATER.name, letters: GUIDE_WATER.letters })).toContain("the Vedder River");
    expect(guideText(search, { name: GUIDE_WATER.name, letters: GUIDE_WATER.letters })).toContain("“ved”");
    expect(guideText(welcome, null)).toContain("a water near you");
    expect(guideText(welcome, null)).not.toMatch(/\{|\}/);
  });
  it("(T466) the closed example's stretch is named, or said generally while the release is read; none: the step says so", () => {
    const up = guideSteps("app").find((s) => s.key === "upstream")!.body;
    expect(guideText(up, null, { part: "Slesse Creek → Reserve" })).toContain("until you reach “Slesse Creek → Reserve”.");
    expect(guideText(up, null)).toContain(`until you reach “${WALKTHROUGH_WORDS.closedPartAny}”.`);
    expect(guideText(WALKTHROUGH_WORDS.closedNone.body, { name: GUIDE_WATER.name, letters: GUIDE_WATER.letters }))
      .toBe("This copy of the rules has no stretch near the Vedder River that is closed all year, so this bit of the tour is skipped.");
    // the closed phase is every step about the example, and none of them depends on a season
    expect(guideSteps("web").filter((s) => s.closed).map((s) => s.key)).toEqual(["partsmap", "upstream", "closedregs", "closed"]);
    expect(guideSteps("web").find((s) => s.key === "salmon")!.title).not.toMatch(/closed/i);
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
  it("(T294-proof) the spot steps say a day you fished, not one visit", () => {
    const days = guideSteps("app").find((s) => s.key === "days")!.body;
    expect(days).toContain("a day you fished here");
  });
  it("seen only when the stored text is the current version", () => {
    expect([walkthroughSeen(null), walkthroughSeen(""), walkthroughSeen("walkthrough-2"), walkthroughSeen(WALKTHROUGH_VERSION),
            walkthroughSeen(` ${WALKTHROUGH_VERSION}\n`)]).toEqual([false, false, false, true, true]);
  });
  it("keeps where an unfinished walk was left, one line beside the others, and forgets it", () => {
    const s = withGuideAt(withSeen(null, TAB_TOURS.runs), "answer");
    expect([s, guideResumeAt(s)]).toEqual([`${TAB_TOURS.runs}\nguide-at:answer`, "answer"]);
    const moved = withGuideAt(s, "book");
    expect([guideResumeAt(moved), moved.split("\n").length]).toEqual(["book", 2]);
    expect([guideResumeAt(withGuideAt(moved, null)), withGuideAt(moved, null)]).toEqual([null, TAB_TOURS.runs]);
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
