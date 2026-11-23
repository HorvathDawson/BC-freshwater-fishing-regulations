// @vitest-environment jsdom
/**
 * (T1010) A region page's water search: the page's own markup (regionLead) run through the SHIPPED script (FIND_SCRIPT).
 * Hidden until the script runs; loads the names on first use; lists matches as links; a failed load is said on the page.
 */
import { beforeEach, describe, expect, it } from "vitest";
import { findScript } from "./gauges";
import { regionLead } from "./regionMap";

const CFG = { crash: "https://crash.test/crash", env: "test", release: "r-test" };
// the names file's order: alphabetical
const NAMES: [string, string][] = [["Adams River", "/water/adams-river"], ["Akimukoo Creek", "/water/akimukoo-creek"], ["Akinkoom Creek", "/water/akinkoom-creek"],
  ["East Branch Kootenay River", "/water/east-branch-kootenay-river"], ["Koos Creek", "/water/koos-creek"], ["Kootenay Lake", "/water/kootenay-lake"]];
let posted: { url: string; body: { name: string; message: string } }[] = [];
const tick = () => new Promise((r) => setTimeout(r, 0));
function mount(fetcher: (url: string) => Promise<{ ok: boolean; status: number; json(): Promise<unknown> }>) {
  document.body.innerHTML = regionLead("Region 4", { svg: "<svg></svg>", waters: 3 }, `<a class="app" href="/">Open</a>`, "/assets/region-4.abc.json", "/x.js", CFG).replace(/<script[^>]*><\/script>/, "");
  const src = findScript().replace("window.fetch.bind(window)", "globalThis.__f");
  expect(src).toContain("globalThis.__f");
  (globalThis as { __f?: unknown }).__f = (url: string, init?: { body?: string }) => {
    if (url === CFG.crash) { posted.push({ url, body: JSON.parse(init!.body!) }); return Promise.resolve(new Response(null, { status: 204 })); }
    return fetcher(url);
  };
  (globalThis as { fetch?: unknown }).fetch = (globalThis as { __f?: unknown }).__f;
  new Function(src)();
}
const type = (v: string) => { const q = document.querySelector("input")!; q.value = v; q.dispatchEvent(new Event("input")); };
describe("(T1010) the region page's water search", () => {
  beforeEach(() => { document.body.innerHTML = ""; posted = []; });
  it("is hidden until the script runs, then shown; matches are links to the water", async () => {
    document.body.innerHTML = regionLead("Region 4", { svg: "<svg></svg>", waters: 3 }, "<a>x</a>", "/assets/n.json", "/x.js", CFG);
    expect(document.querySelector("form")!.hidden).toBe(true);
    let asked = "";
    mount(async (u) => { asked = u; return { ok: true, status: 200, json: async () => NAMES }; });
    expect(document.querySelector("form")!.hidden).toBe(false);
    type("koo"); await tick(); await tick();
    expect(asked).toBe("/assets/region-4.abc.json");
    expect([...document.querySelectorAll(".sugg a")].map((a) => [a.textContent, a.getAttribute("href")])).toEqual([
      ["Koos Creek", "/water/koos-creek"], ["Kootenay Lake", "/water/kootenay-lake"],          // start with it
      ["East Branch Kootenay River", "/water/east-branch-kootenay-river"],                    // a word starts with it
      ["Akimukoo Creek", "/water/akimukoo-creek"], ["Akinkoom Creek", "/water/akinkoom-creek"]]); // it is inside a word
    type("zzz");
    expect(document.querySelectorAll(".sugg a").length).toBe(0);
    expect(document.querySelector(".said")!.textContent).toMatch(/No water/);
  });
  it("a names file that cannot be loaded is said on the page, never silent", async () => {
    mount(async () => ({ ok: false, status: 404, json: async () => [] }));
    type("koo"); await tick(); await tick();
    expect(document.querySelector(".said")!.textContent).toMatch(/could not be loaded.*404/);
    await tick();
    // shown AND reported (the repo rule): the crash record names the failure
    expect(posted.length).toBe(1);
    expect(posted[0]!.url).toBe(CFG.crash);
    expect(posted[0]!.body.message).toMatch(/names file 404/);
  });
});
