// T321: the DFO wording registry in the Worker (src/dfo/wording.ts) — the same file and the same reading as the Python
// (pipeline/regs/dfo_salmon/wording.py), and the URGENT owner email for a clause DFO never printed before.
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { compose } from "../src/events";
import { type RenderCtx } from "../src/email";
import { dfoEvents } from "../src/index";
import { runDfo } from "../src/dfo";
import { clauses, REGISTRY, rowWords, template, unknown } from "../src/dfo/wording";
import { toRules } from "../src/dfo/typed";
import { MemStore, REPO } from "./dfo.scenario";

const PY = join(REPO, ".venv/bin/python");
const FIX = join(REPO, "app/workers/feeds/test/fixtures/dfo");
const page = (slug: string) => readFileSync(join(FIX, `pages/region${slug}-eng.html`), "utf-8");
const NEW = "fish from shore on Sundays";
const MUTANTS = [
  `4 per day, only 2 over 50 cm, ${NEW}`,
  "4 per day, one hour before sunrise to one hour after sunset — FN0853",
  "4 coho per day in the aggregate, must be hatchery marked, only 2 of which may be greater than 40 cm — FN1281",
  "4 per day, only 1 over 65 cm (See exception below).",
  "2 per day, only 1 65cm or less and 1 over 65 cm.",
  "Hook restriction, single barbless hook less than 15 mm",
  "",
];

describe.skipIf(!existsSync(PY))("the wording registry: TS == Python", () => {
  it("every saved cell (tracked fixtures) and the mutants: clauses, templates, unknown, the card's row", () => {
    const script = `
import json, sys
from pipeline.regs.dfo_salmon import wording as W
pages = [p for p in W.saved_pages() if "fixtures" in str(p[0])]
cells = sorted({(r.species, r.dates, r.limits_gear) for _, r in W.saved_rows(pages)})
cells += [("Coho", "Sep 1 to Oct 31", c) for c in json.loads(sys.argv[1])]
out = [{"rec": [s, d, c], "clauses": W.clauses(c), "templates": [W.template(x) for x in W.clauses(c)],
        "unknown": W.unknown(c), "words": W.row_words({"species": s, "dates": d, "limits_gear": c}),
        "n": len(W.registry())} for s, d, c in cells]
print(json.dumps(out))`;
    const py = JSON.parse(execFileSync(PY, ["-c", script, JSON.stringify(MUTANTS)], { cwd: REPO, encoding: "utf-8", maxBuffer: 1 << 26 }));
    expect(py.length).toBeGreaterThan(200);
    for (const x of py) {
      const [species, dates, cell] = x.rec;
      expect(clauses(cell)).toEqual(x.clauses);
      expect(clauses(cell).map(template)).toEqual(x.templates);
      expect(unknown(cell)).toEqual(x.unknown);
      expect(rowWords({ species, dates, limits_gear: cell })).toEqual(x.words);
      expect(REGISTRY.size).toBe(x.n);
    }
  });
});

describe("new wording in the Worker", () => {
  it("an unseen clause flags the row: review_reason, no number", () => {
    const rules = toRules({ species: "Coho", dates: "Sep 1 to Oct 31", limits_gear: `4 per day, only 2 over 50 cm, ${NEW}` }, "x", 0, true);
    expect(rules[0]!.review_reason).toBe(`new wording: '${NEW}'`);
    const solar = toRules({ species: "Pink", dates: "Aug 22 to Sep 30", limits_gear: MUTANTS[1]! }, "x", 0, true);
    expect(solar[0]!.when?.hours).toEqual({ start: { solar: "sunrise", offset_min: -60 }, end: { solar: "sunset", offset_min: 60 } });
    expect(solar[0]!.review_reason).toBeUndefined();
  });

  it("a live run: the card's rows carry it, the owner is emailed URGENTLY once per template, never again", async () => {
    const entries = async (slug: string) => readFileSync(join(FIX, `entries/region-${slug}.json`), "utf-8");
    const bad = page("8").replace("<td>4 per day, only 2 over 50&nbsp;cm.</td>", `<td>4 per day, only 2 over 50&nbsp;cm, ${NEW}.</td>`);
    const priv = new MemStore();
    const run = (now: string) =>
      runDfo({ fetch: async () => new Response("ok"), notifyUrl: "http://n", feed: new MemStore(), priv, entries, now, regions: ["8"], fetcher: async () => [bad, null] });
    const got = await run("2026-10-31T12:00:00Z");
    const idx = got.index as any; // eslint-disable-line @typescript-eslint/no-explicit-any
    const flagged = Object.entries(idx.locs).filter(([, c]: [string, any]) => c.x?.length); // eslint-disable-line @typescript-eslint/no-explicit-any
    expect(flagged.length).toBeGreaterThan(0);
    for (const [, c] of flagged as [string, any][]) expect(c.x.map((r: any) => r.k)).toEqual(["new"]); // eslint-disable-line @typescript-eslint/no-explicit-any
    const w = got.notices.filter((n) => n.key.startsWith("dfo-wording:"));
    expect(w.map((n) => n.key)).toEqual([`dfo-wording:${template(NEW)}`]);
    expect(w[0]!.data).toMatchObject({ region: "8", kind: "wording", clause: `${NEW}.`, template: template(NEW) });

    const ctx: RenderCtx = { env: "staging", statusUrl: null };
    const [m] = compose(dfoEvents(w, "2026-10-31T12:00:00Z"), false, ctx);
    expect(m!.subject).toContain("1 new wording in a limits cell");
    const flat = m!.text.replace(/\s+/g, " ");
    expect(flat).toContain(`New wording: "${NEW}."`);
    expect(flat).toContain("data/curated/dfo_salmon/wording.json");
    expect(flat).toContain(`"${template(NEW)}"`);

    expect(got.sent.sent).toContain(`dfo-wording:${template(NEW)}`);
    const again = await run("2026-11-01T00:00:00Z");
    expect(again.sent.sent.filter((k) => k.startsWith("dfo-wording:"))).toEqual([]); // told once
  });
});
