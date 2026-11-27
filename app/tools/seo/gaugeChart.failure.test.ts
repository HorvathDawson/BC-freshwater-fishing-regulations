// @vitest-environment jsdom
/**
 * (T1039 re-review, W46) THE GAUGE PAGE'S REACT ROOTS SAY THEIR OWN FAILURES: a render error in the app's chart or in the
 * "Get the app" block is said where it was drawn and reported (never a silently emptied box, React 19's default), and a page
 * whose config parsed but lacks a place for either is shown AND reported. The two components are made to throw here.
 */
import "../render-setup";
import "./globals";
import { afterEach, describe, expect, it, vi } from "vitest";
import { waitFor } from "@testing-library/react";
import { setFailureSink } from "@app/core";

vi.mock("../../packages/ui-native/src/StationChart", () => ({ StationChart: () => { throw new Error("the chart's render broke"); } }));
vi.mock("../../packages/ui-native/src/GetAppBlock", () => ({ GetAppBlock: () => { throw new Error("the block's render broke"); } }));

const { CHART_FAILED, mountGaugeChart } = await import("./gaugeChart");

const CRASH = "https://c/crash";
function page(body = `<div id="gauge-chart"></div><div id="gauge-get-app"></div>`) {
  const doc = document.implementation.createHTMLDocument("g");
  doc.body.innerHTML = body + `<script type="application/json" id="gauge-config">${JSON.stringify({
    station: "08MH001", facts: "https://d/f.json", live: "https://d/feeds/live", clim: [], fonts: null, weather: null, weatherAttribution: "",
    sky: {}, crash: CRASH, env: "staging", release: "r1", contact: null, place: null, source: { name: "Environment Canada", url: "https://wateroffice.ec.gc.ca/" } })}</script>`;
  const sent: string[] = [];
  const fetchImpl = (async (u: string, init?: RequestInit) => {
    if (u === CRASH) { sent.push(JSON.parse(String(init?.body)).message); return new Response("{}", { status: 201 }); }
    return new Response("not here", { status: 404 });
  }) as typeof fetch;
  return { doc, sent, fetchImpl };
}

afterEach(() => { setFailureSink(null); });

describe("the gauge page's roots say their own failures", () => {
  it("a render error in the chart and in the Get the app block: each box says so, each is reported", async () => {
    const p = page();
    mountGaugeChart(p.doc, p.fetchImpl, Date.now);
    await waitFor(() => expect(p.sent).toEqual(expect.arrayContaining(["the chart's render broke", "the block's render broke"])));
    expect(p.doc.getElementById("gauge-chart")!.textContent).toBe(CHART_FAILED);
    expect(p.doc.getElementById("gauge-get-app")!.textContent).toBe(CHART_FAILED);
  });

  it("a config that parsed but no place for Get the app: shown where the chart goes AND reported", async () => {
    const p = page(`<div id="gauge-chart"></div>`);
    expect(() => mountGaugeChart(p.doc, p.fetchImpl, Date.now)).toThrow(/no #gauge-get-app/);
    expect(p.doc.getElementById("gauge-chart")!.textContent).toBe(`${CHART_FAILED} Environment Canada`);       // (T1194) with the source's link
    await waitFor(() => expect(p.sent).toEqual(["gauge chart: no #gauge-get-app on this page"]));
  });
});
