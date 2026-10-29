/**
 * (T248) The first-run walkthrough's words and the Help page's content: generated from the legend tokens, so a status, a
 * gear mark, a salmon status, a run state or a sync state added without a help sentence fails here.
 */
import { describe, expect, it } from "vitest";
import { GEAR_LEGEND } from "./method";
import { RUN_STATES } from "./runTiming";
import { SALMON_MAP_STATUSES } from "./salmon";
import { WATER_STATUSES } from "./status";
import { HELP_WORDS, SYNC_KINDS, WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, helpVM, walkthroughSeen } from "./help";

describe("the walkthrough (T248)", () => {
  it("is 3 or 4 cards, each with a title and words, in the order the owner listed", () => {
    expect(WALKTHROUGH_WORDS.cards.map((c) => c.key)).toEqual(["modes", "days", "tap", "offline"]);
    for (const c of WALKTHROUGH_WORDS.cards) { expect(c.title.length).toBeGreaterThan(3); expect(c.body.length).toBeGreaterThan(40); }
  });
  it("names the three map modes by the words the app already uses", () => {
    const b = WALKTHROUGH_WORDS.cards[0]!.body;
    expect(["Standard", "Salmon", "Run timing"].every((w) => b.includes(w))).toBe(true);
  });
  it("seen only when the stored text is the current version", () => {
    expect([walkthroughSeen(null), walkthroughSeen(""), walkthroughSeen("walkthrough-0"), walkthroughSeen(WALKTHROUGH_VERSION),
            walkthroughSeen(` ${WALKTHROUGH_VERSION}\n`)]).toEqual([false, false, false, true, true]);
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
      "tidal", "bell",
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
