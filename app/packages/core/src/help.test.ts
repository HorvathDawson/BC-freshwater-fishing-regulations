/**
 * (T248) The first-run walkthrough's words and the Help page's content: generated from the legend tokens, so a status, a
 * gear mark, a salmon status, a run state or a sync state added without a help sentence fails here.
 */
import { describe, expect, it } from "vitest";
import { GEAR_LEGEND } from "./method";
import { RUN_STATES } from "./runTiming";
import { SALMON_MAP_STATUSES } from "./salmon";
import { WATER_STATUSES } from "./status";
import { GUIDE_WATER, HELP_MAP_KEY, HELP_WORDS, SYNC_KINDS, WALKTHROUGH_VERSION, guideResumeAt, guideSteps, guideText, helpVM,
         walkthroughSeen, withGuideAt, withSeen, WALKTHROUGH_WORDS, type GuideStepWords } from "./help";

describe("the walkthrough (T293)", () => {
  it("(T842) the owner's six steps in his order, every step with words; DO steps are the reader's taps", () => {
    // (T824) was 41 on the phone, then 10; (T842, owner 2026-11-17, his pick "6 steps") six: the welcome folded into the
    // search, the reading into the conditions, and back to the map / Trout or Salmon / Spots and Help said on the last card
    const app = guideSteps("app");
    expect(app.map((s) => s.key)).toEqual(["search", "pick", "answer", "day", "conditions", "done"]);
    expect(app.map((s) => s.title)).toEqual(["Search for it", "Open its page", "Open or closed", "Pick another day",
                                             "How is the river?", "You're all set"]);
    expect(app.filter((s) => s.kind === "do").map((s) => s.key)).toEqual(["search", "pick", "day", "conditions"]);
    // the last card is words only: nothing to tap, nothing highlighted
    expect([app.at(-1)!.kind, app.at(-1)!.targets]).toEqual(["show", []]);
    for (const k of ["Map", "Trout", "Salmon", "Spots", "Help", "More"]) expect(app.at(-1)!.body).toContain(k);
    // (T842) the pick step closes the keyboard (it hid the Vedder River row)
    expect(app.filter((s) => s.keyboard === "closed").map((s) => s.key)).toEqual(["pick"]);
    for (const s of app) { expect(s.title.length, s.key).toBeGreaterThan(3); expect(s.body.length, s.key).toBeGreaterThan(20); }
    expect(new Set(app.map((s) => s.key)).size).toBe(app.length);
  });
  it("(T824, T874) the first card says how the walk goes (Next does what is highlighted); no card says ring", () => {
    expect(guideText(guideSteps("app")[0]!.body, { name: GUIDE_WATER.name, letters: GUIDE_WATER.letters }))
      .toBe("I'll walk you through the Vedder River: Next searches for it, or tap the search bar and type “ved” yourself. "
        + "On every step, Next does what is highlighted.");
    const all = [...guideSteps("app"), ...guideSteps("web")].flatMap((s) => [s.title, s.body]);
    for (const t of [...all, ...(Object.values(WALKTHROUGH_WORDS) as unknown[]).filter((v): v is string => typeof v === "string")])
      expect(t, t).not.toMatch(/\bring(s|ed)?\b/i);
  });
  it("(T824) one short sentence a card (T842: the first also says how the walk works, the last says where things are)", () => {
    for (const s of [...guideSteps("app"), ...guideSteps("web")]) {
      const sentences = s.body.split(/(?<=[.!?])\s+/);
      expect(sentences.length, s.key).toBeLessThanOrEqual(["search", "done"].includes(s.key) ? 2 : 1);
      for (const t of sentences) expect(t.split(/\s+/).length, t).toBeLessThanOrEqual(20);
    }
  });
  it("(T874, owner: \"every next should be show me logic\") each DO step's words say what Next does; no word says Show me", () => {
    const water = { name: GUIDE_WATER.name, letters: GUIDE_WATER.letters };
    const said = Object.fromEntries(guideSteps("app").filter((s) => s.kind === "do").map((s) => [s.key, guideText(s.body, water)]));
    expect(said).toEqual({
      search: "I'll walk you through the Vedder River: Next searches for it, or tap the search bar and type “ved” yourself. "
        + "On every step, Next does what is highlighted.",
      pick: "Next opens its page, or tap Vedder River in the list yourself.",
      day: "Next picks tomorrow, or tap the date and pick the day you plan to go.",
      conditions: "Next opens Conditions, how high the river runs; its dot says low, normal or high for this date.",
    });
    const all = [...guideSteps("app"), ...guideSteps("web")].flatMap((s) => [s.title, s.body]);
    for (const t of [...all, ...(Object.values(WALKTHROUGH_WORDS) as unknown[]).filter((v): v is string => typeof v === "string")])
      expect(t, t).not.toMatch(/show me/i);
    expect(Object.keys(WALKTHROUGH_WORDS)).not.toContain("showMe");
  });
  it("the web's walk is the same walk (T302: spots are the phone app's): its last card names Help alone", () => {
    const web = guideSteps("web"), app = guideSteps("app");
    expect(web.map((s) => s.key)).toEqual(app.map((s) => s.key));
    expect(web.filter((s) => s !== app.find((a) => a.key === s.key)).map((s) => s.key)).toEqual(["done"]);
    expect(web.at(-1)!.body).not.toMatch(/Spots/);
    expect(app.at(-1)!.body).toMatch(/Spots/);
  });
  it("names the chosen water, or any water when the release has none by that name", () => {
    const welcome = guideSteps("app")[0]!.body, search = welcome;
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
    expect([s, guideResumeAt(s)]).toEqual([`${WALKTHROUGH_VERSION}\nguide-at:${WALKTHROUGH_VERSION}/answer`, "answer"]);
    const moved = withGuideAt(s, "book");
    expect([guideResumeAt(moved), moved.split("\n").length]).toEqual(["book", 2]);
    expect([guideResumeAt(withGuideAt(moved, null)), withGuideAt(moved, null)]).toEqual([null, WALKTHROUGH_VERSION]);
  });
  it("(T842) a step kept by an older walk is not resumed (the 10-step walk's \"answer\" was its step 4): it starts at step 1", () => {
    expect(guideResumeAt("walkthrough-5\nguide-at:answer")).toBeNull();
    expect(guideResumeAt("guide-at:walkthrough-6/answer")).toBeNull();
    // and a new resume line replaces the old one
    expect(withGuideAt("walkthrough-5\nguide-at:answer", "pick")).toBe(`walkthrough-5\nguide-at:${WALKTHROUGH_VERSION}/pick`);
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
  it("every row has a name; no swatch carries a hex colour (tokens only)", () => {
    for (const r of rows) expect(r.name.length, r.key).toBeGreaterThan(1);
    expect(JSON.stringify(helpVM())).not.toMatch(/#[0-9a-fA-F]{3,8}\b/);
  });
  it("(T873, owner 2026-11-18: \"way way to verbose\") a page read in ten seconds: no intro, few short notes, short facts", () => {
    // a note only where the name alone would mislead or leave out what to do: these, word for word
    expect(Object.fromEntries(rows.filter((r) => r.note).map((r) => [r.key, r.note]))).toEqual({
      "status-base": "Only the region's rules apply.",
      "status-no_access": "Regulations still apply; check access first.",
      tidal: "DFO's tidal rules apply here.",
      "salmon-check": "To be determined by DFO, or a DFO row the app couldn't read.",
      "run-none": "Not the same as no fish.",
      "sync-stale": "Tap it, or restart the app.",
      "sync-error": "Tap the red mark to retry.",
      bell: "Changes not on the map yet.",
      "fresh-stale": "Refresh before relying on it.",
      "fresh-unknown": "No date recorded.",
      "fresh-dated": "Map base data: age doesn't matter.",
    });
    expect(HELP_WORDS.facts.app).toEqual(["Regulations, the map and your spots work with no signal.",
                                          "Gauges and weather need a connection.", "Big updates wait for Wi-Fi."]);
    for (const t of [...HELP_WORDS.facts.app, ...HELP_WORDS.facts.web]) expect(t.split(/\s+/).length, t).toBeLessThanOrEqual(12);
    // the map's key leads the page; every section title is short
    expect(helpVM().slice(0, HELP_MAP_KEY.length).map((s) => s.key)).toEqual([...HELP_MAP_KEY]);
    for (const s of helpVM()) expect(s.title.split(/\s+/).length, s.title).toBeLessThanOrEqual(5);
    // (T860) one speaker: the app is "I", never "we"
    const all = JSON.stringify([HELP_WORDS, helpVM().map((s) => s.rows.map((r) => [r.name, r.note]))]);
    expect(all).not.toMatch(/\b(we|we're|we've|our|us)\b/i);
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
