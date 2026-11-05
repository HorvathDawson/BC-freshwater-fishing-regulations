/**
 * (T237) "Send report", mounted: the reason, the optional email (validated), the rows left on — and the payload that
 * reaches the sender is pinned whole (no field of a switched-off row; T352: every field sent is LISTED before Send — the
 * section handle under "Where you were", the map's layer, forecast offset, zoom and centre, the install id). Sent → the confirmation; offline → "saved, will be sent"; refused → said, the form kept. No sender → no
 * Send button (email / share only). About links the form.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { ReportPayload } from "@app/core";
import type { ReportSendResult } from "@app/data";
import { ERROR_LOG, useMenu, type ReportSender, type Route } from "@app/ui";
import { LIGHT } from "../theme";
import { MenuHost, type MenuInfo } from "./MenuHost";

afterEach(() => { cleanup(); vi.restoreAllMocks(); ERROR_LOG.clear(); });

const ROUTE = { mode: "water", stack: [{ item: "356308001", view: "regs", part: 2, tapSid: 812345 }] } as unknown as Route;

const MAP = { layer: "regulations · map", horizon: 3 as const, zoom: 9.437, lat: 49.12345, lon: -121.95678 };

function Host({ info, route = ROUTE, open = "report", map = null }: { info: MenuInfo; route?: Route; open?: "report" | "about";
                                                                    map?: typeof MAP | null }) {
  const menu = useMenu();
  return (
    <>
      <button data-testid="open" onClick={() => (open === "report" ? menu.report({ screen: "water · regulations",
        place: "Vedder River", water: "356308001", part: 2, section: 812345, day: "Today, Oct 28" }) : menu.show("about"))} />
      <MenuHost menu={menu} palette={LIGHT} loader={null} info={info} tiles="10cbcb72e9f907cf.edc12ab96087" route={route}
                day={null} mode="salmon" map={map} />
    </>
  );
}

const settle = () => act(() => {
  for (const el of Array.from(document.body.querySelectorAll("*")))
    for (const t of ["animationend", "webkitAnimationEnd"]) el.dispatchEvent(new Event(t, { bubbles: true }));
});
const click = (id: string) => { act(() => { fireEvent.click(screen.getByTestId(id)); }); settle(); };
const type = (id: string, v: string) => act(() => { fireEvent.change(screen.getByTestId(id), { target: { value: v } }); });
const flush = () => act(async () => { await new Promise((r) => setTimeout(r, 0)); });

function sender(result: ReportSendResult) {
  const sent: ReportPayload[] = [];
  const s: ReportSender = { send: async (p) => { sent.push(p); return result; } };
  return { s, sent };
}

describe("Send report (T237)", () => {
  it("sends the reason, the words, the email and exactly the rows left on; then thanks the reader", async () => {
    const { s, sent } = sender({ kind: "sent" });
    render(<Host map={MAP} info={{ appBuild: "0.1.0", contact: null, report: s, install: "0a1b2c3d4e5f6789",
                         env: { about: { env: "staging", built: "staging", origin: "x", switchable: false }, pending: null,
                                pick: () => {} } }} />);
    click("open");
    // (T352) everything that will be sent is listed, ABOVE the Send button: the section handle, the map, the install id
    expect(screen.getByTestId("report-diag-where").textContent).toMatch(/Section: 812345/);
    expect(screen.getByTestId("report-diag-where").textContent).toMatch(/Map colours: Salmon/);
    expect(screen.getByTestId("report-diag-map").textContent)
      .toBe("Map viewLayer: regulations · mapForecast: +3 daysZoom: 9.4Map centre: 49.123, -121.957");
    expect(screen.getByTestId("report-diag-install").textContent).toBe("Install id0a1b2c3d4e5f6789 (random, made on this device)");
    const sendBtn = screen.getByLabelText("Send report");
    for (const k of ["where", "map", "install"])
      expect(screen.getByTestId(`report-diag-${k}`).compareDocumentPosition(sendBtn) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(screen.getByTestId("report-privacy").textContent).toMatch(/only to reply/);
    act(() => { fireEvent.click(screen.getByLabelText("Wrong place on the map")); });
    type("report-what", "The closure ends at the bridge, not the forks.");
    expect(screen.queryByTestId("report-contact")).toBeNull();          // no email: no "contact me"
    type("report-email", "not an email");
    expect(screen.queryByTestId("report-contact")).toBeNull();          // a malformed one: none either
    expect(screen.getByTestId("report-email-bad").textContent).toBe("That does not look like an email address.");
    act(() => { fireEvent.click(screen.getByLabelText("Send report")); });   // disabled while the email is malformed
    expect(sent).toEqual([]);
    type("report-email", "angler@example.org");
    expect(screen.getByTestId("report-contact").getAttribute("aria-checked")).toBe("false");   // off by default
    act(() => { fireEvent.click(screen.getByTestId("report-contact")); });
    expect(screen.getByTestId("report-contact").getAttribute("aria-checked")).toBe("true");
    act(() => { fireEvent.click(screen.getByLabelText("Platform")); });      // switched off: not sent
    act(() => { fireEvent.click(screen.getByLabelText("Send report")); });
    await flush();
    expect(sent).toHaveLength(1);
    const p = sent[0]!;
    expect(p.id).toMatch(/^[0-9a-f]{16}$/);
    expect(Number.isNaN(Date.parse(p.created))).toBe(false);
    expect({ ...p, id: "x", created: "y" }).toEqual({
      format: "report/2", id: "x", created: "y", reason: "place", text: "The closure ends at the bridge, not the forks.",
      email: "angler@example.org", contact: true,
      context: { screen: "water · regulations", place: "Vedder River", water: "356308001", part: 2, section: 812345,
                 day: "Today, Oct 28", mode: "salmon", app: "0.1.0", env: "staging",
                 layer: "regulations · map", horizon: "3", zoom: 9.4, lat: 49.123, lon: -121.957, install: "0a1b2c3d4e5f6789",
                 tiles: "10cbcb72e9f907cf.edc12ab96087" },
    });
    expect(screen.getByTestId("report-done-text").textContent).toBe("Thank you — your report was sent.");
    click("open");
  });

  it("(T407) App bug and Feature request are reasons of their own; the question and its example follow the choice", async () => {
    const { s, sent } = sender({ kind: "sent" });
    render(<Host info={{ appBuild: null, contact: null, report: s }} />);
    click("open");
    expect(screen.getByLabelText("What's wrong?")).toBeTruthy();
    act(() => { fireEvent.click(screen.getByLabelText("Feature request")); });
    expect(screen.getByLabelText("What would you like?")).toBeTruthy();
    act(() => { fireEvent.click(screen.getByLabelText("App bug")); });
    expect(screen.getByLabelText("What happened?")).toBeTruthy();
    type("report-what", "The map went blank.");
    act(() => { fireEvent.click(screen.getByLabelText("Send report")); });
    await flush();
    expect(sent.map((p) => p.reason)).toEqual(["bug"]);
  });

  it("switching Where you were off sends no water, part, section or day", async () => {
    const { s, sent } = sender({ kind: "sent" });
    render(<Host info={{ appBuild: null, contact: null, report: s }} />);
    click("open");
    type("report-what", "x");
    act(() => { fireEvent.click(screen.getByLabelText("Where you were")); });
    act(() => { fireEvent.click(screen.getByLabelText("Send report")); });
    await flush();
    expect(sent[0]!.context).toEqual({ platform: "web", tiles: "10cbcb72e9f907cf.edc12ab96087" });
  });

  it("offline: saved on the device, to be sent later", async () => {
    const { s } = sender({ kind: "queued", offline: true });
    render(<Host info={{ appBuild: null, contact: null, report: s }} />);
    click("open");
    type("report-what", "x");
    act(() => { fireEvent.click(screen.getByLabelText("Send report")); });
    await flush();
    expect(screen.getByTestId("report-done-text").textContent)
      .toBe("You are offline. Your report is saved on this device and will be sent when you are back online.");
    act(() => { fireEvent.click(screen.getByLabelText("Report something else")); });
    expect((screen.getByTestId("report-what") as HTMLTextAreaElement).value).toBe("");
  });

  it("refused: says why and keeps the form", async () => {
    const { s } = sender({ kind: "refused", error: "report too large" });
    render(<Host info={{ appBuild: null, contact: null, report: s }} />);
    click("open");
    type("report-what", "x");
    act(() => { fireEvent.click(screen.getByLabelText("Send report")); });
    await flush();
    expect(screen.getByTestId("report-refused").textContent).toBe("The report was not accepted: report too large");
    expect(screen.getByTestId("report-what")).toBeTruthy();
  });

  it("no sender: no Send button, no reason or email — email / share only", () => {
    render(<Host info={{ appBuild: null, contact: null }} />);
    click("open");
    expect(screen.queryByLabelText("Send report")).toBeNull();
    expect(screen.queryByTestId("report-email")).toBeNull();
    expect(screen.queryByTestId("report-reasons")).toBeNull();
    expect(screen.getByLabelText("Share…")).toBeTruthy();
  });

  it("About links Report a problem", () => {
    render(<Host info={{ appBuild: null, contact: null }} open="about" />);
    click("open");
    click("about-report");
    expect(screen.getByTestId("menu-page-report")).toBeTruthy();
  });
});
