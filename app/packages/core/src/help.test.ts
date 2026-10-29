/**
 * (T248) The first-run walkthrough's words and the Help page's content: generated from the legend tokens, so a status, a
 * gear mark, a salmon status, a run state or a sync state added without a help sentence fails here.
 */
import { describe, expect, it } from "vitest";
import { GEAR_LEGEND } from "./method";
import { RUN_STATES } from "./runTiming";
import { SALMON_MAP_STATUSES } from "./salmon";
import { WATER_STATUSES } from "./status";
import { HELP_WORDS, SYNC_KINDS, WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, helpVM, walkthroughCards, walkthroughSeen } from "./help";

describe("the walkthrough (T248)", () => {
  it("(T257) is six steps, each pointing at a control, in the order the owner listed", () => {
    expect(WALKTHROUGH_WORDS.cards.map((c) => c.key)).toEqual(["days", "modes", "map", "conditions", "runs", "sync"]);
    expect(WALKTHROUGH_WORDS.cards.map((c) => c.targets))
      .toEqual([["days"], ["modes"], ["map"], ["tab:conditions"], ["tab:runs"], ["sync", "bell"]]);
    for (const c of WALKTHROUGH_WORDS.cards) { expect(c.title.length).toBeGreaterThan(3); expect(c.body.length).toBeGreaterThan(40); }
  });
  it("(owner 2026-10-29) the web's tour says what its refresh button does there; the app's keeps works-offline", () => {
    const web = walkthroughCards("web"), app = walkthroughCards("app");
    expect(web.map((c) => [c.key, c.targets])).toEqual(app.map((c) => [c.key, c.targets]));
    const sync = (l: typeof web) => l.find((c) => c.key === "sync")!;
    expect(sync(web).body).toContain("checks for new gauge data and regulations");
    expect(sync(web).body).not.toMatch(/offline|Wi-Fi|on your device/);
    expect(sync(app).title).toBe("Works offline");
    expect(web.filter((c, i) => c !== app[i]).map((c) => c.key)).toEqual(["sync"]);
  });
  it("names the map's modes by the words the app already uses (Run timing is its own tab, T255)", () => {
    const b = WALKTHROUGH_WORDS.cards.find((c) => c.key === "modes")!.body;
    expect(["Trout & more", "Salmon"].every((w) => b.includes(w))).toBe(true);
  });
  it("seen only when the stored text is the current version", () => {
    expect([walkthroughSeen(null), walkthroughSeen(""), walkthroughSeen("walkthrough-1"), walkthroughSeen(WALKTHROUGH_VERSION),
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
