/**
 * (T1081) The size guard (`tools/check-size.mjs`). Each budget is held on a made-up lean tree: it passes as built, and
 * one change over a budget makes exactly that budget's message appear. The guard also reads the real repo once (below) so
 * a budget table that today's tree already breaks shows here, and the CLI's `--measure` output is held to what it fixes.
 */
import { describe, expect, it } from "vitest";
import { execFileSync } from "node:child_process";
import { AGENTS_DIR, BUDGETS, DOCS_DIR, LOOKUP, checkRepo, checkSize, keptProblems, lineCount, packageDirs } from "./check-size.mjs";
import { TICKETS_DIR } from "./ledger.mjs";

type F = { path: string; lines?: number; bytes?: number };
const repoRoot = new URL("../..", import.meta.url).pathname;
const md = (path: string, lines = 5): F => ({ path, lines });
const max = (k: keyof typeof BUDGETS) => BUDGETS[k][0];

/** A lean tree: every required page, one package of each kind with its README, one open ticket, one picture. */
const lean = (): F[] => [
  md("README.md", 10), md("AGENTS.md", 100),
  md(`${AGENTS_DIR}/START-HERE.md`, 20), md(`${AGENTS_DIR}/STATUS.md`, 30), md(`${DOCS_DIR}/app-integration/decisions.md`, 200),
  md(`${AGENTS_DIR}/restart/chat.md`, 30),
  md("app/conformance/README.md"), md("app/tools/README.md"), { path: "app/packages/core/package.json" }, md("app/packages/core/README.md", 30),
  { path: "app/apps/mobile/package.json" }, md("app/apps/mobile/README.md"), { path: "app/workers/feeds/package.json" }, md("app/workers/feeds/README.md"),
  { path: "pipeline/atlas/__init__.py" }, md("pipeline/atlas/README.md", 40), { path: "pipeline/atlas/waters/x.py" },
  md(`${TICKETS_DIR}/T9-small.md`, 100), md(`${TICKETS_DIR}/README.md`, 90), { path: `${TICKETS_DIR}/closed.tsv` },
  { path: `${DOCS_DIR}/shots/a.png`, bytes: 1000 },
];
type Maps = { readmeLong?: Record<string, number>; ticketOver?: Record<string, number>; markdownLong?: Record<string, number>;
              pageLong?: Record<string, number>; workingPages?: Record<string, string>; readingBudget?: number };
/** the guard on a made-up tree, with no exception maps unless the test gives its own (the real ones name real files) */
const run = (files: F[], extra: { closedIds?: string[]; kept?: Record<string, string> } & Maps = {}) =>
  checkSize({ files, closedIds: extra.closedIds ?? [], kept: extra.kept ?? {}, readmeLong: extra.readmeLong ?? {}, ticketOver: extra.ticketOver ?? {},
             markdownLong: extra.markdownLong ?? {}, pageLong: extra.pageLong ?? {}, workingPages: extra.workingPages ?? {},
             readingBudget: extra.readingBudget ?? 1e9 });
const grow = (files: F[], path: string, lines: number) => files.map((f) => (f.path === path ? { ...f, lines } : f));
const without = (files: F[], path: string) => files.filter((f) => f.path !== path);
/** the message names the file, its size, the budget and the way out */
const says = (problems: string[], path: string, size: number | string, budget: number | string) => {
  const hit = problems.filter((p) => p.startsWith(`${path}:`));
  expect(hit).toHaveLength(1);
  expect(hit[0]).toContain(`${size} `);
  expect(hit[0]).toContain(`budget ${budget}`);
  expect(hit[0]).toContain("cut it, or raise the budget in the same PR with a reason");
};

describe("the made-up lean tree", () => {
  it("passes", () => expect(run(lean())).toEqual([]));
  it("must name files: an empty list throws rather than passing", () => expect(() => run([])).toThrow(/never/));
  it("counts lines like the other guards (the trailing newline is no line)", () => {
    expect(lineCount("a\nb\n")).toBe(2);
    expect(lineCount("a\nb")).toBe(2);
  });
});

describe("required reading", () => {
  const cases: [string, string, keyof typeof BUDGETS][] = [
    ["README.md", "README.md", "readme"], ["START-HERE.md", `${AGENTS_DIR}/START-HERE.md`, "startHere"], ["AGENTS.md", "AGENTS.md", "agents"],
    ["STATUS.md", `${AGENTS_DIR}/STATUS.md`, "status"], ["the decisions page", `${DOCS_DIR}/app-integration/decisions.md`, "decisions"],
    ["a restart page", `${AGENTS_DIR}/restart/chat.md`, "restart"],
  ];
  for (const [label, path, key] of cases) {
    it(`${label}: at the budget passes, one line over fails`, () => {
      expect(run(grow(lean(), path, max(key)))).toEqual([]);
      says(run(grow(lean(), path, max(key) + 1)), path, max(key) + 1, max(key));
    });
  }
  it("the budgets are the owner's numbers", () => {
    expect([max("readme"), max("startHere"), max("agents"), max("status"), max("restart"), max("decisions")]).toEqual([80, 30, 450, 60, 60, 650]);
  });
  it("a required page that is not tracked fails (a moved page does not slip the budget)", () => {
    expect(run(without(lean(), "AGENTS.md")).join("\n")).toContain("AGENTS.md: not tracked");
  });
});

describe("READMEs under app/ and pipeline/", () => {
  it(`${max("subReadme")} lines pass, one more fails`, () => {
    expect(run(grow(lean(), "app/packages/core/README.md", max("subReadme")))).toEqual([]);
    says(run(grow(lean(), "app/packages/core/README.md", max("subReadme") + 1)), "app/packages/core/README.md", max("subReadme") + 1, max("subReadme"));
  });
  it("a listed exception holds its own budget, and the list is only what it must be", () => {
    const path = "pipeline/atlas/waters/long/README.md", own = 60, readmeLong = { [path]: own };
    expect(run([...lean(), md(path, own)], { readmeLong })).toEqual([]);
    says(run([...lean(), md(path, own + 1)], { readmeLong }), path, own + 1, own);
    // an entry whose README is back within the general budget, or gone, fails: take it off
    expect(run([...lean(), md(path, max("subReadme"))], { readmeLong }).join("\n")).toContain(`README_LONG lists ${path}, now ${max("subReadme")} lines`);
    expect(run(lean(), { readmeLong }).join("\n")).toContain(`README_LONG lists ${path}, which is not tracked`);
    expect(run([...lean(), md(path, 30)], { readmeLong: { [path]: 30 } }).join("\n")).toContain("take it off the list");
  });
  it("a README outside app/ and pipeline/ is not this budget's", () => {
    expect(run([...lean(), md("deploy/README.md", max("page"))])).toEqual([]);
  });
});

describe("every package has a README", () => {
  it("lists the app packages, conformance, tools and each pipeline package", () => {
    expect(packageDirs(lean().map((f) => f.path))).toEqual(["app/apps/mobile", "app/conformance", "app/packages/core", "app/tools", "app/workers/feeds", "pipeline/atlas"]);
  });
  for (const dir of ["app/apps/mobile", "app/packages/core", "app/workers/feeds", "app/conformance", "app/tools", "pipeline/atlas"])
    it(`${dir} without a README fails`, () => {
      expect(run(without(lean(), `${dir}/README.md`)).join("\n")).toContain(`${dir}: no README.md`);
    });
  it("a new app package or pipeline package without a README fails", () => {
    expect(run([...lean(), { path: "app/packages/newpkg/package.json" }]).join("\n")).toContain("app/packages/newpkg: no README.md");
    expect(run([...lean(), { path: "pipeline/newstage/__init__.py" }]).join("\n")).toContain("pipeline/newstage: no README.md");
  });
  it("a pipeline folder that is not a package (no __init__.py) needs none", () => {
    expect(run([...lean(), { path: "pipeline/docs/notes.txt" }])).toEqual([]);
  });
});

describe("open tickets", () => {
  const t = (n: string) => `${TICKETS_DIR}/${n}`;
  it(`${max("ticket")} lines pass, one more fails`, () => {
    expect(run([...lean(), md(t("T10-a.md"), max("ticket"))])).toEqual([]);
    says(run([...lean(), md(t("T10-a.md"), max("ticket") + 1)]), t("T10-a.md"), max("ticket") + 1, max("ticket"));
  });
  it("README.md and other non-ticket pages in the folder are not tickets", () => {
    expect(run(grow(lean(), t("README.md"), 400)).filter((p) => p.includes("README.md: 400"))).toEqual([]);
  });
  it("a grandfathered ticket holds its own size and can only shrink; the map keeps no stale entry", () => {
    const name = "T77-long.md", own = 200, ticketOver = { [name]: own };
    expect(run([...lean(), md(t(name), own)], { ticketOver }).filter((p) => p.startsWith(t(name)) || p.includes(`lists ${name}`))).toEqual([]);
    says(run([...lean(), md(t(name), own + 1)], { ticketOver }), t(name), own + 1, own);
    expect(run([...lean(), md(t(name), max("ticket"))], { ticketOver }).join("\n")).toContain(`TICKET_OVER lists ${name}, now ${max("ticket")} lines`);
    expect(run([...lean(), md(t(name), own - 1)], { ticketOver }).join("\n")).toContain(`TICKET_OVER holds ${name} at ${own} but it is ${own - 1} lines`);
    expect(run(lean(), { ticketOver }).join("\n")).toContain(`TICKET_OVER lists ${name}, which is not an open ticket file`);
  });
});

describe("any markdown file", () => {
  it(`${max("anyMarkdown")} lines pass, one more fails (a page in the tickets folder: outside it the page budget is the lower one)`, () => {
    const notes = `${TICKETS_DIR}/notes.md`;
    expect(run([...lean(), md(notes, max("anyMarkdown"))])).toEqual([]);
    says(run([...lean(), md(notes, max("anyMarkdown") + 1)]), notes, max("anyMarkdown") + 1, max("anyMarkdown"));
  });
  it("a listed file holds its own budget; a stale or lowered entry fails", () => {
    const path = "data/ref/API.md", own = 900, markdownLong = { [path]: own };
    says(run([...lean(), md(path, own + 1)], { markdownLong }), path, own + 1, own);
    expect(run([...lean(), md(path, own)], { markdownLong })).toEqual([]);
    expect(run([...lean(), md(path, max("anyMarkdown"))], { markdownLong }).join("\n")).toContain(`MARKDOWN_LONG lists ${path}, now ${max("anyMarkdown")} lines`);
    expect(run(lean(), { markdownLong }).join("\n")).toContain(`MARKDOWN_LONG lists ${path}, which is not tracked`);
  });
});

describe("the page standards outside the tickets folder (the second cut)", () => {
  const page = `${DOCS_DIR}/runbooks/thing.md`;
  it(`a page of ${max("page")} lines passes, one more fails, in the docs folder or anywhere else`, () => {
    expect(run([...lean(), md(page, max("page")), md("deploy/NOTES.md", max("page"))])).toEqual([]);
    says(run([...lean(), md(page, max("page") + 1)]), page, max("page") + 1, max("page"));
    says(run([...lean(), md("deploy/NOTES.md", max("page") + 1)]), "deploy/NOTES.md", max("page") + 1, max("page"));
  });
  it("a listed page holds its size and only shrinks; a stale entry fails", () => {
    const pageLong = { [page]: 300 };
    expect(run([...lean(), md(page, 300)], { pageLong })).toEqual([]);
    says(run([...lean(), md(page, 301)], { pageLong }), page, 301, 300);
    expect(run([...lean(), md(page, 200)], { pageLong }).join("\n")).toContain(`PAGE_LONG holds ${page} at 300 but it is 200 lines`);
    expect(run([...lean(), md(page, max("page"))], { pageLong }).join("\n")).toContain(`PAGE_LONG lists ${page}, now ${max("page")} lines`);
    expect(run(lean(), { pageLong }).join("\n")).toContain(`PAGE_LONG lists ${page}, which is not tracked`);
  });
  it("tickets, look-up files and pages with their own budget are not held to the page budget", () => {
    // every LOOKUP entry (a file, or a folder ending in "/"), read from the guard's own table
    expect(LOOKUP.length).toBeGreaterThan(3);
    expect(run([...lean(), ...LOOKUP.map((l: string) => md(l.endsWith("/") ? `${l}x.md` : l, 600))])).toEqual([]);
    expect(run(grow(lean(), "AGENTS.md", max("agents")))).toEqual([]);
  });
  it("a page named for a day fails, in the docs folder only", () => {
    expect(run([...lean(), md(`${DOCS_DIR}/research/review-2026-11-15.md`)]).join("\n")).toContain("a page named for a day is history");
    expect(run([...lean(), md(`${DOCS_DIR}/research/review.md`), md(`${AGENTS_DIR}/ROADMAP-after-0.9.0.md`)])).toEqual([]);
    expect(run([...lean(), md(`${TICKETS_DIR}/T9-files/notes-2026-11-15.md`)])).toEqual([]);
    expect(run([...lean(), md(`${DOCS_DIR}/research/2026-11-15/notes.md`)]).join("\n")).toContain("a page named for a day is history");
  });
  it("a ticket's page outside its ticket's folder fails unless listed; in the folder it is the ticket's (held to the page budget)", () => {
    const loose = `${DOCS_DIR}/app-integration/T9-findings.md`, inFolder = `${DOCS_DIR}/app-integration/T9-shots/before-after.md`;
    expect(run([...lean(), md(loose)]).join("\n")).toContain(`${loose}: a ticket's page outside its ticket's folder`);
    expect(run([...lean(), md(loose)], { workingPages: { [loose]: "the owner reads it" } })).toEqual([]);
    expect(run(lean(), { workingPages: { [loose]: "gone" } }).join("\n")).toContain(`WORKING_PAGES lists ${loose}, which is not tracked`);
    expect(run([...lean(), md(inFolder), md(`${DOCS_DIR}/app-integration/T9-files/pasted-2026-11-25.md`)])).toEqual([]);
    says(run([...lean(), md(inFolder, max("page") + 1)]), inFolder, max("page") + 1, max("page"));
    expect(run([...lean(), { path: `${DOCS_DIR}/app-integration/T9-shots/a.png`, bytes: 10 }])).toEqual([]);
    // a folder keyed to a ticket but not named as a ticket's folder outlives the ticket
    expect(run([...lean(), md(`${DOCS_DIR}/research/t9-hard-cases/NOTES.md`)]).join("\n")).toContain("a ticket's page outside its ticket's folder");
  });
  it("the reading total: every markdown line outside tickets, look-up files and restart pages, at the budget passes, one over fails", () => {
    const total = lean().filter((f) => f.path.endsWith(".md") && !f.path.startsWith(`${TICKETS_DIR}/`) && !f.path.startsWith(`${AGENTS_DIR}/restart/`)).reduce((n, f) => n + (f.lines ?? 0), 0);
    expect(run([...lean(), md("CHANGELOG.md", 500)], { readingBudget: total })).toEqual([]);
    says(run([...lean(), md("deploy/NOTES.md", 1)], { readingBudget: total }), "markdown outside the tickets folder", total + 1, total);
  });
  it("(T1144) a restart page is outside the reading total and still held by its own cap", () => {
    const total = lean().filter((f) => f.path.endsWith(".md") && !f.path.startsWith(`${TICKETS_DIR}/`) && !f.path.startsWith(`${AGENTS_DIR}/restart/`)).reduce((n, f) => n + (f.lines ?? 0), 0);
    const page = `${AGENTS_DIR}/restart/another-chat.md`;
    // a full restart page at a reading total with no room left: passes (mutation: count it -> "markdown outside the tickets folder")
    expect(run([...lean(), md(page, max("restart"))], { readingBudget: total })).toEqual([]);
    // one line over its own cap still fails, by its own rule
    says(run([...lean(), md(page, max("restart") + 1)], { readingBudget: total }), page, max("restart") + 1, max("restart"));
    // and any other page still counts
    says(run([...lean(), md(`${DOCS_DIR}/NOTES.md`, 1)], { readingBudget: total }), "markdown outside the tickets folder", total + 1, total);
  });
  it("the real budget is set (never the 0 it is written with before a measure)", () => expect(max("readingLines")).toBeGreaterThan(1000));
});

describe("the docs tree's totals", () => {
  /** `total` markdown lines in files of 600 (under the one-file budget), in the tickets folder (outside it a page is 150 at most) */
  const many = (_n: number, total: number): F[] => Array.from({ length: Math.ceil(total / 600) }, (_, i) => md(`${TICKETS_DIR}/pages/p${i}.md`, Math.min(600, total - i * 600)));
  it("markdown lines: at the budget passes, one over fails, and it counts docs + tickets once", () => {
    const base = lean();
    const have = base.filter((f) => [DOCS_DIR, TICKETS_DIR].some((d) => f.path.startsWith(`${d}/`))).reduce((n, f) => n + (f.lines ?? 0), 0);
    const files = [...base, ...many(1, max("treeLines") - have)];
    expect(run(files)).toEqual([]);
    const bad = run([...base, ...many(1, max("treeLines") - have + 1)]);
    expect(bad.some((p) => p.includes(`${max("treeLines")} (`) || p.includes(`budget ${max("treeLines")}`))).toBe(true);
    expect(bad.join("\n")).toContain("tracked markdown lines");
  });
  it("tracked files: one over the count fails", () => {
    const base = lean();
    const have = base.filter((f) => [DOCS_DIR, TICKETS_DIR].some((d) => f.path.startsWith(`${d}/`))).length;
    const pad = (n: number): F[] => Array.from({ length: n }, (_, i) => ({ path: `${DOCS_DIR}/pad/f${i}.json` }));
    expect(run([...base, ...pad(max("treeFiles") - have)])).toEqual([]);
    expect(run([...base, ...pad(max("treeFiles") - have + 1)]).join("\n")).toContain("tracked files, budget");
  });
  it("picture bytes: one byte over fails; a picture outside the tree does not count", () => {
    const base = lean().filter((f) => !f.path.endsWith(".png"));
    expect(run([...base, { path: `${DOCS_DIR}/shots/a.png`, bytes: max("treePictureBytes") }])).toEqual([]);
    expect(run([...base, { path: `${DOCS_DIR}/shots/a.png`, bytes: max("treePictureBytes") + 1 }]).join("\n")).toContain("bytes of pictures, budget");
    expect(run([...base, { path: "app/apps/mobile/a.png", bytes: max("treePictureBytes") * 2 }])).toEqual([]);
    for (const ext of ["jpg", "jpeg", "webp", "gif"])
      expect(run([...base, { path: `${TICKETS_DIR}/s/a.${ext}`, bytes: max("treePictureBytes") + 1 }]).join("\n")).toContain("bytes of pictures");
  });
});

describe("closed tickets' picture folders", () => {
  const dir = (n: string) => `${TICKETS_DIR}/${n}`;
  const pic = (n: string): F => ({ path: `${dir(n)}/a.txt` });
  it("a folder of a closed ticket fails; one of an open ticket, or an unrelated folder, passes", () => {
    const bad = run([...lean(), pic("T50-shots")], { closedIds: ["T50"] });
    expect(bad.join("\n")).toContain(`${dir("T50-shots")}: its ticket T50 is closed`);
    expect(run([...lean(), pic("T50-shots")], { closedIds: ["T51"] })).toEqual([]);
    expect(run([...lean(), pic("T50-shots")], { closedIds: [] })).toEqual([]);
  });
  it("a folder under the docs tree outside the tickets folder is caught too", () => {
    expect(run([...lean(), { path: `${DOCS_DIR}/app-integration/T60-files/x.json` }], { closedIds: ["T60"] }).join("\n")).toContain("T60-files: its ticket T60 is closed");
  });
  it("KEPT_ASSETS exempts it (keyed by the path under the docs folder, as ledger.mjs keys it)", () => {
    const key = dir("T50-files").startsWith(`${DOCS_DIR}/`) ? dir("T50-files").slice(DOCS_DIR.length + 1) : dir("T50-files");
    expect(run([...lean(), pic("T50-files")], { closedIds: ["T50"], kept: { [key]: "a test reads it" } })).toEqual([]);
  });
  it("a range folder (T5-7-shots) is closed only when every ticket in it is", () => {
    expect(run([...lean(), pic("T5-7-shots")], { closedIds: ["T5", "T6"] })).toEqual([]);
    expect(run([...lean(), pic("T5-7-shots")], { closedIds: ["T5", "T6", "T7"] }).join("\n")).toContain("T5-7-shots: its ticket");
  });
});

describe("KEPT_ASSETS cannot rot (keptProblems)", () => {
  const kept = { "app-integration/T9-shots": "why" };
  const readers = { "app-integration/T9-shots": ["docs/a.md"] };
  const paths = [`${DOCS_DIR}/app-integration/T9-shots/x.png`, "docs/a.md"];
  const read = (t: string) => () => t;
  it("passes when the asset is tracked and its reader names it", () => {
    expect(keptProblems({ paths, kept, readers, read: read("see T9-shots/x.png") })).toEqual([]);
  });
  it("mutation: the asset gone, the reader gone, the citation gone, no reader, a stray reader entry each fail", () => {
    expect(keptProblems({ paths: ["docs/a.md"], kept, readers, read: read("T9-shots") }).join()).toContain("which is not tracked");
    expect(keptProblems({ paths: [paths[0]], kept, readers, read: read("T9-shots") }).join()).toContain("names docs/a.md");
    expect(keptProblems({ paths, kept, readers, read: read("nothing here") }).join()).toContain("no longer names T9-shots");
    expect(keptProblems({ paths, kept, readers: {}, read: read("T9-shots") }).join()).toContain("no KEPT_READERS entry");
    expect(keptProblems({ paths, kept: {}, readers, read: read("T9-shots") }).join()).toContain("not in KEPT_ASSETS");
  });
});

describe("the real repo", () => {
  it("the real exception maps hold nothing the real tree no longer needs, and every entry is reported by name", () => {
    const problems = checkRepo(repoRoot);
    // the only failures allowed here are leftover picture folders of closed tickets that the cleanup deletes; every budget passes
    expect(problems.filter((p) => !/: its ticket .* is closed \(closed\.tsv\)/.test(p))).toEqual([]);
  });
  it("is fast: one git ls-files and the markdown files", () => {
    const t0 = Date.now();
    checkRepo(repoRoot);
    expect(Date.now() - t0).toBeLessThan(5000);
  });
  it("--measure prints the three totals and each exception map", () => {
    const out = execFileSync("node", ["tools/check-size.mjs", "--measure"], { cwd: new URL("..", import.meta.url).pathname, encoding: "utf8" });
    for (const k of ["treeLines:", "treeFiles:", "treePictureBytes:", "README_LONG = {", "TICKET_OVER = {", "MARKDOWN_LONG = {"]) expect(out).toContain(k);
  });
});
