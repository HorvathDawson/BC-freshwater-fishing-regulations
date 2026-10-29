// T225 (owner 2026-10-28): the email template — one layout for every kind, HTML + plain text, each item's text ONCE and
// in full, one "What to do" per email, the env tag + status link + why in the footer. Snapshots pin every kind.
import { describe, expect, it } from "vitest";
import { compose, type FeedEvent } from "../src/events";
import { sampleDfoEvents, wrap, type RenderCtx } from "../src/email";
import { gaugeEvents, healthEvents } from "../src/index";

const ctx: RenderCtx = { env: "staging", statusUrl: "https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/status" };
const at = "2026-10-28T12:00:00Z";
const count = (hay: string, needle: string) => hay.split(needle).length - 1;
const htmlEsc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

/** the plain text keeps to 78 columns except a command or URL line (never wrapped) */
function checkWidth(text: string) {
  for (const l of text.split("\n")) if (l.length > 78) expect(l, "a long line must be a command or URL").toMatch(/^\s{7}\S+ |https?:\/\//);
}

const inseason: FeedEvent = {
  key: "inseason:abc", kind: "inseason", title: "x", text: "x", at,
  data: {
    source: "https://www2.gov.bc.ca/gov/content/sports-culture/recreation/fishing-hunting/fishing/fishing-regulations",
    added: [{ section: "Region 4 - Kootenay", water: "Elk River", change: "No fishing September 1 to October 31, including tributaries.", effective: "October 28, 2026" }],
    changed: [{
      before: { section: "Region 1 - Vancouver Island (includes Haida Gwaii: WMU 6-12, 6-13)", water: "Cowichan River", change: "No fishing", effective: "July 4, 2025 to August 31, 2025" },
      after: { section: "Region 1 - Vancouver Island (includes Haida Gwaii: WMU 6-12, 6-13)", water: "Cowichan River", change: "No fishing upstream of Skutz Falls", effective: "July 4, 2025 to September 15, 2025" },
    }],
    removed: [{ section: "Region 7B - Peace", water: "Peace River", change: "No fishing between Peace Canyon Dam and Hwy 29 Bridge", effective: "May 2, 2025" }],
  },
};
const hydat = gaugeEvents([{ kind: "hydat", since: "2026-10-28T09:00:00Z", current: "20250115", latest: "20261015", action: "scripts/update-hydat.sh" }], at);
const roster = gaugeEvents([{ kind: "roster", since: at, current: "data/source/bc_hydrometric_stations.json", latest: "+07FA008 +08DA013 -08NM050", action: "rm data/source/bc_hydrometric_stations.json && (cd data && PYTHONPATH=.. ../.venv/bin/python fetch_data.py --layers hydrometric_stations)" }], at);
const health = healthEvents([{ job: "gauges", from: "ok", to: "failing", text: "Gauge feed FAILED 2 run(s) in a row: Error: no station answered (asked 438)" }], at);
const report: FeedEvent = { key: "report:0123456789abcdef", kind: "report", title: "Wrong rule: Elk River — the closure dates are a year out", text: "The closure dates are a year out.\n\nWhere: Elk River · map regs", at };

describe("the email template", () => {
  it("DFO (the owner's first email, redone): every place once, in full; one queue command; no truncation", () => {
    const [m] = compose(sampleDfoEvents(), false, ctx);
    expect(m!.subject).toBe("[staging] DFO: 2 new places to place (Region 1)");
    for (const e of sampleDfoEvents()) {
      expect(count(m!.text.replace(/\n\s+/g, " "), e.data.place)).toBe(1);
      expect(count(m!.html!, htmlEsc(e.data.place))).toBe(1);
    }
    expect(count(m!.text, "queue --regions")).toBe(1);
    expect(count(m!.html!, "queue --regions")).toBe(1);
    expect(m!.text).not.toMatch(/…/);
    expect(m!.text).toContain("Review app → DFO → Extents: Nanaimo River (2 places)");
    checkWidth(m!.text);
    expect(m!.text).toMatchSnapshot("dfo.txt");
    expect(m!.html).toMatchSnapshot("dfo.html");
  });

  it("in-season: added / changed (before → after) / removed, per region", () => {
    const [m] = compose([inseason], false, ctx);
    expect(m!.subject).toBe("[staging] In-season: 1 added, 1 changed, 1 removed (Regions 1, 4, 7B)");
    expect(m!.text).toContain("was: No fishing");
    expect(m!.html).toContain("<s>No fishing</s>");
    checkWidth(m!.text);
    expect(m!.text).toMatchSnapshot("inseason.txt");
    expect(m!.html).toMatchSnapshot("inseason.html");
  });

  it("feed health, HYDAT, roster drift, a problem report", () => {
    const all = [...health, ...hydat, ...roster, report];
    const subjects = all.map((e) => compose([e], false, ctx)[0]!.subject);
    expect(subjects).toEqual([
      "[staging] Gauge feed failing",
      "[staging] New HYDAT 20261015 — run the HYDAT action",
      "[staging] Roster drift: 2 joined, 1 left",
      "[staging] 1 problem report from the app",
    ]);
    for (const e of all) {
      const [m] = compose([e], false, ctx);
      checkWidth(m!.text);
      expect(m!.text).toContain("Environment: STAGING");
      expect(m!.text).toMatchSnapshot(`${e.kind}.txt`);
      expect(m!.html).toMatchSnapshot(`${e.kind}.html`);
    }
  });

  it("the daily digest: one section per kind, each with its own what-to-do", () => {
    const [m] = compose([...health, ...hydat, ...roster], true, ctx);
    expect(m!.subject).toBe("[staging] Daily digest: Gauge feed failing; New HYDAT 20261015 — run the HYDAT action; Roster drift: 2 joined, 1 left");
    expect(count(m!.text, "What to do:")).toBe(3);
    checkWidth(m!.text);
    expect(m!.text).toMatchSnapshot("digest.txt");
    expect(m!.html).toMatchSnapshot("digest.html");
  });

  it("wrap: hanging indent, words never split", () => {
    expect(wrap("aaa bbb ccc ddd", 9, "* ", "  ")).toEqual(["* aaa bbb", "  ccc ddd"]);
  });
});
