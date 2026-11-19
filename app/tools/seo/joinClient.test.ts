// @vitest-environment jsdom
/**
 * (T911) The site's Join the test card: the page's own markup (openInAppControl) run through the SHIPPED script
 * (joinScript): opens, Email me is the mailto, Copy email says Copied, a refused clipboard is said and the address shown,
 * and ✕ / Escape / a click outside close it with focus back on the link.
 */
import { beforeEach, describe, expect, it } from "vitest";
import { WEB_SPOTS_WORDS } from "@app/core";
import { joinScript, openInAppControl } from "./gauges";
import { joinPopup } from "./joinClient";

const C = "support@canifishthis.ca";
const P = WEB_SPOTS_WORDS.join.popup;

/** jsdom has no <dialog> methods: a minimal showModal / close, as the browser's */
function polyfill() {
  const D = HTMLDialogElement.prototype as unknown as Record<string, unknown>;
  D.showModal = function (this: HTMLDialogElement) { this.setAttribute("open", ""); };
  D.close = function (this: HTMLDialogElement) { this.removeAttribute("open"); this.dispatchEvent(new Event("close")); };
}
let clip: { writeText: (t: string) => Promise<void> } | undefined;
function mount(html = openInAppControl(null, null, C)) {
  document.body.innerHTML = `<div class="opens">${html}</div>`;
  const src = joinScript().replace("window.navigator.clipboard", "globalThis.__clip");
  (globalThis as { __clip?: unknown }).__clip = clip;
  new Function(src)();
}
beforeEach(() => { polyfill(); clip = undefined; });

describe("the site's Join the test card (T911)", () => {
  it("the shipped script is joinPopup's own source, started on load", () => {
    expect(joinScript()).toContain("function joinPopup(");
    document.body.innerHTML = "<p>no join links here</p>";
    expect(() => joinPopup(document, () => undefined)).not.toThrow();
  });
  it("each Join link is still the mailto, beside a closed dialog with the line, Email me and Copy email", () => {
    const html = openInAppControl(null, null, C);
    expect(html).toContain(`<a class="app" href="mailto:${C}?subject=Join%20the%20Android%20test&amp;body=`);
    expect(html).toContain(`data-join="join-android"`);
    expect(html).toContain(P.android); expect(html).toContain(P.iphone);
    expect(html).toContain(`>${P.email}</a>`); expect(html).toContain(`>${P.copy}</button>`);
    expect(html).toContain(`data-copy="${C}"`);
    expect(html).not.toMatch(/<script/);
  });
  it("no contact: no buttons and no dialogs", () => {
    expect(openInAppControl(null, null, null)).not.toContain("dialog");
  });
  it("a click opens the card instead of the mail, with Email me on the same mailto", () => {
    mount();
    const link = document.querySelector<HTMLAnchorElement>('a[data-join="join-android"]')!;
    const d = document.getElementById("join-android") as HTMLDialogElement;
    const ev = new MouseEvent("click", { bubbles: true, cancelable: true });
    link.dispatchEvent(ev);
    expect(ev.defaultPrevented).toBe(true);
    expect(d.hasAttribute("open")).toBe(true);
    expect(d.querySelector<HTMLAnchorElement>(".acts a")!.getAttribute("href")).toBe(link.getAttribute("href"));
  });
  it("Copy email copies the address and says Copied", async () => {
    const got: string[] = [];
    clip = { writeText: async (t) => { got.push(t); } };
    mount();
    const d = document.getElementById("join-android")!;
    d.querySelector<HTMLElement>("[data-copy]")!.click();
    await new Promise((r) => setTimeout(r, 0));
    expect(got).toEqual([C]);
    expect(d.querySelector("[data-said]")!.textContent).toBe(P.copied);
    expect(d.querySelector<HTMLElement>("[data-addr]")!.hidden).toBe(true);
  });
  it("a refused clipboard says so and shows the address; no clipboard at all is the same", async () => {
    clip = { writeText: () => Promise.reject(new Error("denied")) };
    mount();
    let d = document.getElementById("join-android")!;
    d.querySelector<HTMLElement>("[data-copy]")!.click();
    await new Promise((r) => setTimeout(r, 0));
    expect(d.querySelector("[data-said]")!.textContent).toBe(P.refused);
    expect(d.querySelector<HTMLElement>("[data-addr]")!.hidden).toBe(false);
    expect(d.querySelector("[data-addr]")!.textContent).toBe(C);
    clip = undefined; mount();
    d = document.getElementById("join-iphone")!;
    d.querySelector<HTMLElement>("[data-copy]")!.click();
    expect(d.querySelector("[data-said]")!.textContent).toBe(P.refused);
    expect(d.querySelector<HTMLElement>("[data-addr]")!.hidden).toBe(false);
  });
  it("closes by the ✕, Escape and a click outside, and focus goes back to the link", () => {
    mount();
    const link = document.querySelector<HTMLElement>('a[data-join="join-android"]')!;
    const d = document.getElementById("join-android") as HTMLDialogElement;
    const open = () => { link.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true })); expect(d.hasAttribute("open")).toBe(true); };
    for (const how of ["x", "escape", "outside"]) {
      open();
      if (how === "x") d.querySelector<HTMLElement>("[data-close]")!.click();
      else if (how === "escape") d.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
      else d.dispatchEvent(new MouseEvent("click", { bubbles: true }));       // target is the dialog itself: the backdrop
      expect(d.hasAttribute("open"), how).toBe(false);
      expect(document.activeElement, how).toBe(link);
    }
  });
});
