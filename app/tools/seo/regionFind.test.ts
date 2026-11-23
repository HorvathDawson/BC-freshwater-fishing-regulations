// @vitest-environment jsdom
/**
 * (T1010) A region page's water search: the page's own markup (regionLead) run through the SHIPPED script (findScript).
 * Hidden until the script runs; loads the names once; ranks matches; folds accents; finds a renamed water by its former
 * name; a stale names file (404 after a release) says "reload it" and is not reported; any other failed load is said, reported
 * once as the region page's own app/route, and never retried.
 */
import { beforeEach, describe, expect, it } from "vitest";
import { findScript } from "./gauges";
import { regionLead } from "./regionMap";

const CFG = { crash: "https://crash.test/crash", env: "test", release: "r-test" };
type Names = [string, string, string[]?][];
// the names file's order: alphabetical
const NAMES: Names = [["Adams River", "/water/adams-river"], ["Akimukoo Creek", "/water/akimukoo-creek"], ["Akinkoom Creek", "/water/akinkoom-creek"],
  ["East Branch Kootenay River", "/water/east-branch-kootenay-river"], ["Koos Creek", "/water/koos-creek"], ["Kootenay Lake", "/water/kootenay-lake"],
  ["Tŝilhqox Biny", "/water/tsilhqox-biny", ["Chilko Lake"]]];
let posted: { url: string; body: { name: string; message: string; app: string; route: string } }[] = [];
let fetched: string[] = [];
const tick = () => new Promise((r) => setTimeout(r, 0));
type Reply = { ok: boolean; status: number; json(): Promise<unknown> };
function mount(fetcher: (url: string) => Promise<Reply>) {
  document.body.innerHTML = regionLead("Region 4", { svg: "<svg></svg>", waters: 3 }, `<a class="app" href="/">Open</a>`, "/assets/region-4.abc.json", "/x.js", CFG).replace(/<script[^>]*><\/script>/, "");
  const src = findScript().replace("window.fetch.bind(window)", "globalThis.__f");
  expect(src).toContain("globalThis.__f");
  (globalThis as { __f?: unknown }).__f = (url: string, init?: { body?: string }) => {
    if (url === CFG.crash) { posted.push({ url, body: JSON.parse(init!.body!) }); return Promise.resolve(new Response(null, { status: 204 })); }
    fetched.push(url);
    return fetcher(url);
  };
  (globalThis as { fetch?: unknown }).fetch = (globalThis as { __f?: unknown }).__f;
  new Function(src)();
}
const ok = (n: Names = NAMES) => async () => ({ ok: true, status: 200, json: async () => n });
const type = async (v: string) => { const q = document.querySelector("input")!; q.value = v; q.dispatchEvent(new Event("input")); await tick(); await tick(); };
const links = () => [...document.querySelectorAll(".sugg a")].map((a) => [a.textContent, a.getAttribute("href")]);
describe("(T1010) the region page's water search", () => {
  beforeEach(() => { document.body.innerHTML = ""; posted = []; fetched = []; });
  it("is hidden until the script runs; matches are ranked links to the water", async () => {
    document.body.innerHTML = regionLead("Region 4", { svg: "<svg></svg>", waters: 3 }, "<a>x</a>", "/assets/n.json", "/x.js", CFG);
    expect(document.querySelector("form")!.hidden).toBe(true);
    mount(ok());
    expect(document.querySelector("form")!.hidden).toBe(false);
    await type("koo");
    expect(fetched).toEqual(["/assets/region-4.abc.json"]);
    expect(links()).toEqual([
      ["Koos Creek", "/water/koos-creek"], ["Kootenay Lake", "/water/kootenay-lake"],          // start with it
      ["East Branch Kootenay River", "/water/east-branch-kootenay-river"],                    // a word starts with it
      ["Akimukoo Creek", "/water/akimukoo-creek"], ["Akinkoom Creek", "/water/akinkoom-creek"]]); // it is inside a word
    // a separate, visually hidden live status says how many; the list is not itself live
    expect(document.querySelector(".sr")!.textContent).toBe("5 waters found.");
    expect(document.querySelector(".sugg")!.hasAttribute("aria-live")).toBe(false);
    await type("zzz");
    expect(links().length).toBe(0);
    expect(document.querySelector(".sr")!.textContent).toBe("No water found.");
    expect(fetched.length).toBe(1);
  });
  it("shows Loading while the names come", async () => {
    let done!: (r: Reply) => void;
    mount(() => new Promise<Reply>((r) => { done = r; }));
    await type("koo");
    expect(document.querySelector(".said")!.textContent).toMatch(/Loading the names/);
    done({ ok: true, status: 200, json: async () => NAMES }); await tick(); await tick();
    expect(links().length).toBeGreaterThan(0);
  });
  it("folds accents and finds a renamed water by its former name, linking to the current water", async () => {
    mount(ok());
    await type("tsilhqox");
    expect(links()).toEqual([["Tŝilhqox Biny", "/water/tsilhqox-biny"]]);
    await type("Chilko Lake");
    expect(links()).toEqual([["Tŝilhqox Biny (formerly Chilko Lake)", "/water/tsilhqox-biny"]]);
  });
  it("a stale names file (404 after a release) says to reload, is not reported, and is asked for once", async () => {
    mount(async () => ({ ok: false, status: 404, json: async () => [] }));
    await type("k"); await type("ko"); await type("koo");
    expect(document.querySelector(".said")!.textContent).toBe("This page is out of date: reload it.");
    expect(document.querySelector(".said a")!.textContent).toBe("reload it");
    expect(fetched.length).toBe(1);
    expect(posted.length).toBe(0);
  });
  it("any other failed load is said and reported ONCE as the region page's own app and route, never retried", async () => {
    mount(async () => ({ ok: false, status: 500, json: async () => [] }));
    await type("k"); await type("ko"); await type("koo");
    expect(document.querySelector(".said")!.textContent).toMatch(/could not be loaded.*500/);
    expect(fetched.length).toBe(1);
    expect(posted.length).toBe(1);
    expect(posted[0]!.body.message).toMatch(/names file 500/);
    expect([posted[0]!.body.app, posted[0]!.body.route]).toEqual(["region-page", "other"]);
  });
});
