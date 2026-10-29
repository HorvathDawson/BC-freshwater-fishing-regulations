/**
 * (T237) The report outbox over a mocked endpoint: sent at once; offline (the device's word, a thrown fetch), a 5xx and a
 * 429 queue it; a 4xx refuses it for good; flush sends the queue oldest first and stops at the first that cannot go; the
 * queue survives a restart; and the update service flushes it on a check and when the network comes back.
 */
import { describe, expect, it } from "vitest";
import type { ReportPayload } from "@app/core";
import { createUpdateService } from "../update/service";
import { createReportOutbox, reportId } from "./outbox";

const URL_ = "https://feeds.example/report";
const p = (id: string): ReportPayload =>
  ({ format: "report/1", id, created: "2026-10-28T20:00:00.000Z", reason: "rule", text: `t${id}`, email: null, context: {} });

function endpoint() {
  const got: { id: string }[] = [];
  let mode: "ok" | "offline" | number = "ok";
  const fetchImpl = (async (_u: RequestInfo | URL, i?: RequestInit) => {
    if (mode === "offline") throw new TypeError("Network request failed");
    if (typeof mode === "number") return Response.json({ error: `nope ${mode}` }, { status: mode });
    got.push(JSON.parse(String(i!.body)));
    return Response.json({ ok: true }, { status: 201 });
  }) as typeof fetch;
  return { got, fetch: fetchImpl, set: (m: typeof mode) => { mode = m; } };
}
const mem = () => { const s = { text: null as string | null, read: () => s.text, write: (t: string) => { s.text = t; } }; return s; };

describe("the report outbox (T237)", () => {
  it("sends at once when it can", async () => {
    const e = endpoint();
    const box = createReportOutbox({ url: URL_, fetch: e.fetch, store: mem() });
    expect(await box.send(p("a"))).toEqual({ kind: "sent" });
    expect(e.got.map((x) => x.id)).toEqual(["a"]);
    expect(box.pending()).toBe(0);
  });

  it("queues offline (no request when the device says so), on a thrown fetch, a 5xx and a 429; sends them in order later", async () => {
    const e = endpoint();
    let online = false;
    const store = mem();
    const box = createReportOutbox({ url: URL_, fetch: e.fetch, store, online: () => online });
    expect(await box.send(p("a"))).toEqual({ kind: "queued", offline: true });
    expect(e.got).toEqual([]);
    online = true;
    e.set("offline");
    expect((await box.send(p("b"))).kind).toBe("queued");
    e.set(503);
    expect((await box.send(p("c"))).kind).toBe("queued");
    e.set(429);
    expect((await box.send(p("d"))).kind).toBe("queued");
    expect(box.pending()).toBe(4);
    // a restart keeps them
    const again = createReportOutbox({ url: URL_, fetch: e.fetch, store, online: () => online });
    expect(again.pending()).toBe(4);
    e.set("ok");
    expect(await again.flush()).toBe(4);
    expect(e.got.map((x) => x.id)).toEqual(["a", "b", "c", "d"]);
    expect(again.pending()).toBe(0);
    expect(JSON.parse(store.text!)).toEqual([]);
  });

  it("a 4xx refuses the report for good, saying why; it is never queued", async () => {
    const e = endpoint();
    e.set(400);
    const box = createReportOutbox({ url: URL_, fetch: e.fetch, store: mem() });
    expect(await box.send(p("a"))).toEqual({ kind: "refused", error: "nope 400" });
    expect(box.pending()).toBe(0);
  });

  it("a new report waits behind the queue, and goes with it", async () => {
    const e = endpoint();
    e.set("offline");
    const box = createReportOutbox({ url: URL_, fetch: e.fetch, store: mem() });
    await box.send(p("a"));
    e.set("ok");
    expect(await box.send(p("b"))).toEqual({ kind: "sent" });
    expect(e.got.map((x) => x.id)).toEqual(["a", "b"]);
  });

  it("makes 16-hex ids", () => {
    expect(reportId()).toMatch(/^[0-9a-f]{16}$/);
  });

  it("the update service flushes it on a check and the moment the network comes back (T231's retry)", async () => {
    const e = endpoint();
    let online = false;
    let tell: ((o: boolean) => void) | null = null;
    const box = createReportOutbox({ url: URL_, fetch: e.fetch, store: mem(), online: () => online });
    await box.send(p("a"));
    const svc = createUpdateService({
      feeds: [], outbox: box, fetch: e.fetch,
      timers: { set: () => 0, clear: () => {} },
      network: { online: () => online, subscribe: (on) => { tell = on; return () => {}; } },
    });
    svc.start();
    await svc.check({});
    expect(box.pending()).toBe(1);            // offline: nothing tried
    online = true;
    tell!(true);
    await box.flush();
    expect(e.got.map((x) => x.id)).toEqual(["a"]);
    expect(box.pending()).toBe(0);
    await box.send(p("b"));                   // sent at once
    e.set("offline");
    await box.send(p("c"));
    e.set("ok");
    await svc.check({});
    await box.flush();
    expect(e.got.map((x) => x.id)).toEqual(["a", "b", "c"]);
    svc.stop();
  });
});
