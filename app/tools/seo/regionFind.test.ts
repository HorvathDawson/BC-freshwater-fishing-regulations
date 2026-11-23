// @vitest-environment jsdom
/**
 * (T1010) A region page's water search: the page's own markup (regionLead) run through the SHIPPED script (FIND_SCRIPT).
 * Hidden until the script runs; loads the names on first use; lists matches as links; a failed load is said on the page.
 */
import { beforeEach, describe, expect, it } from "vitest";
import { FIND_SCRIPT, regionLead } from "./regionMap";

const NAMES: [string, string][] = [["Koos Creek", "/water/koos-creek"], ["Kootenay Lake", "/water/kootenay-lake"], ["Adams River", "/water/adams-river"]];
const tick = () => new Promise((r) => setTimeout(r, 0));
function mount(fetcher: (url: string) => Promise<{ ok: boolean; status: number; json(): Promise<unknown> }>) {
  document.body.innerHTML = regionLead("Region 4", { svg: "<svg></svg>", waters: 3 }, `<a class="app" href="/">Open</a>`, "/assets/region-4.abc.json", "/x.js").replace(/<script[^>]*><\/script>/, "");
  (globalThis as { fetch?: unknown }).fetch = fetcher;
  new Function(FIND_SCRIPT)();
}
const type = (v: string) => { const q = document.querySelector("input")!; q.value = v; q.dispatchEvent(new Event("input")); };
describe("(T1010) the region page's water search", () => {
  beforeEach(() => { document.body.innerHTML = ""; });
  it("is hidden until the script runs, then shown; matches are links to the water", async () => {
    document.body.innerHTML = regionLead("Region 4", { svg: "<svg></svg>", waters: 3 }, "<a>x</a>", "/assets/n.json", "/x.js");
    expect(document.querySelector("form")!.hidden).toBe(true);
    let asked = "";
    mount(async (u) => { asked = u; return { ok: true, status: 200, json: async () => NAMES }; });
    expect(document.querySelector("form")!.hidden).toBe(false);
    type("koo"); await tick(); await tick();
    expect(asked).toBe("/assets/region-4.abc.json");
    expect([...document.querySelectorAll(".sugg a")].map((a) => [a.textContent, a.getAttribute("href")])).toEqual([["Koos Creek", "/water/koos-creek"], ["Kootenay Lake", "/water/kootenay-lake"]]);
    type("zzz");
    expect(document.querySelectorAll(".sugg a").length).toBe(0);
    expect(document.querySelector(".said")!.textContent).toMatch(/No water/);
  });
  it("a names file that cannot be loaded is said on the page, never silent", async () => {
    mount(async () => ({ ok: false, status: 404, json: async () => [] }));
    type("koo"); await tick(); await tick();
    expect(document.querySelector(".said")!.textContent).toMatch(/could not be loaded.*404/);
  });
});
