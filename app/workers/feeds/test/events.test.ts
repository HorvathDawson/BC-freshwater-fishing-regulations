// T225 follow-up (owner 2026-10-28): the owner's notifications — one outbox, deduped, email first (Cloudflare
// send_email), webhook fallback, a daily digest option, a dry run — and the in-season changes check it reports.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { compose, deliver, mime, NOTIFY_STATE, readState, type FeedEvent, type NotifyConfig } from "../src/events";
import { dfoEvents, gaugeEvents, healthEvents } from "../src/index";
import { buildFeed, diff, effectiveDates, parseChange, parseInSeason, regionOf, runInSeason, type InSeasonFeed } from "../src/inseason";
import { MemStore } from "./dfo.scenario";

/** a test event with the `data` its kind's template renders */
const ev = (key: string, kind: FeedEvent["kind"] = "dfo", title = key): FeedEvent => ({
  key, kind, title, text: `about ${key}`, at: "2026-10-28T12:00:00Z",
  data:
    kind === "dfo" ? { region: "6", kind: "new", water: "Skeena River", place: `place ${key}`, affects: "1 rule held" }
    : kind === "hydat" ? { latest: key.split(":")[1], current: "20250115", since: "2026-10-28T00:00:00Z" }
    : kind === "health" ? { job: "dfo", from: /OK again/.test(title) ? "stale" : "ok", to: /OK again/.test(title) ? "ok" : "stale", detail: title }
    : kind === "inseason" ? { added: [{ section: "Region 4 - Kootenay", water: "Elk River", change: `No fishing (${key})`, effective: "October 1, 2026" }], changed: [], removed: [], source: "https://example/inseason" }
    : undefined,
});
const T = (iso: string) => new Date(iso);
const okFetch = async () => new Response("ok");

function mailbox() {
  const sent: { from: string; to: string; raw: string }[] = [];
  const cfg: NotifyConfig = { emailTo: "owner@example.com", emailFrom: "feeds@example.com", sendEmail: async (from, to, raw) => void sent.push({ from, to, raw }) };
  return { sent, cfg };
}
const unb64 = (b: string) => new TextDecoder().decode(Uint8Array.from(atob(b.replace(/\r\n/g, "")), (c) => c.charCodeAt(0)));
/** the text/plain part (multipart or not) */
const bodyOf = (raw: string) => {
  const m = /Content-Type: text\/plain; charset=utf-8\r\nContent-Transfer-Encoding: base64\r\n\r\n([A-Za-z0-9+/=\r\n]+?)(?:\r\n--|$)/.exec(raw);
  return unb64(m![1]!);
};

describe("the outbox", () => {
  it("emails once per event: HYDAT on its own, DFO places as one list; a repeat is not told again", async () => {
    const store = new MemStore();
    const { sent, cfg } = mailbox();
    const events = [ev("hydat:20261015", "hydat", "New HYDAT 20261015 — run the HYDAT action"), ev("dfo-new:6:a"), ev("dfo-new:6:b")];
    const r = await deliver(events, { store, config: cfg, fetch: okFetch, now: T("2026-10-28T12:00:00Z") });
    expect(r.sentVia).toEqual(["email", "email"]);
    const subj = (raw: string) => {
      const v = /Subject: (.*)/.exec(raw)![1]!;
      const m = /^=\?UTF-8\?B\?(.*)\?=$/.exec(v);
      return m ? new TextDecoder().decode(Uint8Array.from(atob(m[1]!), (c) => c.charCodeAt(0))) : v;
    };
    expect(sent.map((s) => subj(s.raw))).toEqual(["[staging] DFO: 2 new places to place (Region 6)", "[staging] New HYDAT 20261015 — run the HYDAT action"]);
    expect(sent[0]!.raw).toMatch(/\r\nMessage-ID: <[^>]+@example\.com>\r\n/);
    expect(bodyOf(sent[0]!.raw)).toContain("place dfo-new:6:a");
    expect(sent[0]!.raw).toMatch(/Content-Type: multipart\/alternative/);
    const again = await deliver([...events, ev("dfo-new:6:c")], { store, config: cfg, fetch: okFetch, now: T("2026-10-28T13:00:00Z") });
    expect(again.messages.map((m) => m.keys)).toEqual([["dfo-new:6:c"]]);
  });

  it("email fails → the webhook; both fail → kept and retried next run", async () => {
    const store = new MemStore();
    const posts: string[] = [];
    let webhookUp = true;
    const fetch = async (_u: string, i?: RequestInit) => (webhookUp ? (posts.push(String(i?.body)), new Response("ok")) : new Response("down", { status: 503 }));
    const cfg: NotifyConfig = { emailTo: "o@x", emailFrom: "f@x", sendEmail: async () => { throw new Error("not verified"); }, webhook: "https://ntfy.example/feeds" };
    expect((await deliver([ev("hydat:1", "hydat")], { store, config: cfg, fetch, now: T("2026-10-28T12:00:00Z") })).sentVia).toEqual(["webhook"]);
    webhookUp = false;
    const r = await deliver([ev("hydat:2", "hydat")], { store, config: cfg, fetch, now: T("2026-10-28T13:00:00Z") });
    expect(r.failed).toBe(1);
    expect((await readState(store)).pending.map((e) => e.key)).toEqual(["hydat:2"]);
    webhookUp = true;
    const r2 = await deliver([], { store, config: cfg, fetch, now: T("2026-10-28T14:00:00Z") });
    expect(r2.sentVia).toEqual(["webhook"]);
    expect(posts.at(-1)).toMatch(/New HYDAT 2 /);
    expect((await readState(store)).pending).toEqual([]);
  });

  it("no channel: logged and recorded (adding a channel later replays nothing)", async () => {
    const store = new MemStore();
    const lines: string[] = [];
    const r = await deliver([ev("dfo-x")], { store, config: {}, fetch: okFetch, now: T("2026-10-28T12:00:00Z"), log: (s) => lines.push(s) });
    expect(r.sentVia).toEqual(["log"]);
    expect(lines[0]).toMatch(/no channel configured/);
    expect((await readState(store)).told["dfo-x"]).toBeDefined();
  });

  it("daily digest: DFO and in-season go now (urgent); HYDAT and health wait for one email a day", async () => {
    const store = new MemStore();
    const { sent, cfg } = mailbox();
    const c = { ...cfg, digest: true, digestHourUtc: 15 };
    const r1 = await deliver([ev("dfo-a"), ev("health:gauges:ok>failing:t", "health", "Gauge feed FAILED")], { store, config: c, fetch: okFetch, now: T("2026-10-28T09:00:00Z") });
    expect(r1.messages.map((m) => m.subject)).toEqual(["[staging] DFO: 1 new place to place (Region 6)"]);
    expect(r1.queued).toBe(1);
    const r2 = await deliver([ev("inseason:h", "inseason"), ev("hydat:20261015", "hydat")], { store, config: c, fetch: okFetch, now: T("2026-10-28T12:00:00Z") });
    expect(r2.messages.map((m) => m.subject)).toEqual(["[staging] In-season: 1 added (Region 4)"]);
    const r3 = await deliver([], { store, config: c, fetch: okFetch, now: T("2026-10-28T15:05:00Z") });
    expect(r3.messages).toHaveLength(1);
    expect(r3.messages[0]!.subject).toMatch(/^\[staging\] Daily digest: /);
    expect(bodyOf(sent.at(-1)!.raw)).toMatch(/Daily digest — 2 items[\s\S]*Gauge feed FAILED[\s\S]*New HYDAT 20261015 is out/);
    // the next digest is a day later, not the next run
    await deliver([ev("hydat:x", "hydat")], { store, config: c, fetch: okFetch, now: T("2026-10-28T18:00:00Z") });
    expect(sent).toHaveLength(3);
    expect((await deliver([], { store, config: c, fetch: okFetch, now: T("2026-10-29T15:10:00Z") })).messages).toHaveLength(1);
  });

  it("no 'all fine' digest: a day with only a recovery sends nothing", async () => {
    const store = new MemStore();
    const { sent, cfg } = mailbox();
    const c = { ...cfg, digest: true, digestHourUtc: 15 };
    await deliver([{ ...ev("health:gauges:failing>ok:t", "health", "Gauge feed is OK again"), fine: true }], { store, config: c, fetch: okFetch, now: T("2026-10-28T10:00:00Z") });
    const r = await deliver([], { store, config: c, fetch: okFetch, now: T("2026-10-28T15:30:00Z") });
    expect(r.messages).toEqual([]);
    expect(sent).toHaveLength(0);
    expect((await readState(store)).pending).toEqual([]);
    // but with an issue the same day, the recovery rides along
    await deliver([{ ...ev("health:dfo:ok>stale:u", "health", "DFO STALE") }, { ...ev("health:dfo:stale>ok:v", "health", "DFO OK again"), fine: true }], { store, config: c, fetch: okFetch, now: T("2026-10-29T16:00:00Z") });
    expect(sent).toHaveLength(1);
    expect(bodyOf(sent[0]!.raw)).toMatch(/DFO STALE[\s\S]*DFO OK again/);
  });

  it("dry run: shows the messages, sends nothing, records nothing", async () => {
    const store = new MemStore();
    const { sent, cfg } = mailbox();
    const r = await deliver([ev("hydat:9", "hydat")], { store, config: cfg, fetch: okFetch, now: T("2026-10-28T12:00:00Z"), dryRun: true });
    expect(r.dryRun).toBe(true);
    expect(r.messages).toHaveLength(1);
    expect(sent).toHaveLength(0);
    expect(store.m.has(NOTIFY_STATE)).toBe(false);
  });

  it("mime: UTF-8 subject encoded, body base64 in 76-column lines", () => {
    const raw = mime("f@example.com", "o@example.com", { subject: "Région — ok", text: "x".repeat(200), keys: [] }, T("2026-10-28T12:00:00Z"));
    expect(raw).toMatch(/^From: BC fishing feeds <f@example\.com>\r\nTo: o@example\.com\r\nSubject: =\?UTF-8\?B\?/);
    expect(raw.split("\r\n\r\n")[1]!.split("\r\n").every((l) => l.length <= 76)).toBe(true);
    expect(bodyOf(raw)).toBe("x".repeat(200));
  });
});

describe("what each job tells", () => {
  it("HYDAT: the action to run; roster drift; DFO with the review link; health", () => {
    const [h, r] = gaugeEvents(
      [
        { kind: "hydat", since: "2026-10-28T00:00:00Z", current: "20250115", latest: "20261015", action: "scripts/update-hydat.sh" },
        { kind: "roster", since: "x", current: "data/source/bc_hydrometric_stations.json", latest: "+2/-1", action: "python -m …" },
      ],
      "t",
    );
    expect(h).toMatchObject({ key: "hydat:20261015", kind: "hydat", title: "New HYDAT 20261015 — run the HYDAT action" });
    expect(h!.text).toMatch(/feeds — HYDAT → climatology → R2/);
    expect(r).toMatchObject({ key: "roster:+2/-1", kind: "roster" });
    const [d] = dfoEvents([{ key: "dfo-new:6:abc", text: "DFO Region 6: new location on Skeena River: 'x' — 2 rules held unplaced. Place it in DFO review after: …" }], "t");
    expect(d!.text).toContain("First: python -m pipeline.regs.dfo_salmon.live queue --regions 6");
    expect(d!.text).toContain("Then: Review app → DFO → Extents: Skeena River — x");
    expect(d!.text).not.toMatch(/https?:/);
    expect(healthEvents([{ job: "dfo", from: "ok", to: "stale", text: "DFO salmon feed is STALE" }], "t")[0]!.key).toBe("health:dfo:ok>stale:t");
  });
});

describe("in-season changes (main's update-in-season.yml, ported)", () => {
  const page = readFileSync(join(__dirname, "fixtures/inseason/page-2026-10-28.html"), "utf-8");
  it("reads every table row of the recorded page", () => {
    const rows = parseInSeason(page);
    expect(rows).toHaveLength(32);
    expect(rows[0]).toEqual({ section: "Region 1 - Vancouver Island (includes Haida Gwaii: WMU 6-12, 6-13)", water: "Cowichan River", change: "No fishing", effective: "July 4, 2025 to August 31, 2025" });
  });
  it("a blocked / redesigned page is an error, never an empty snapshot", () => {
    expect(() => parseInSeason("<html><body>Access Denied</body></html>")).toThrow(/no accordion-container/);
  });
  it("first run is the baseline; then only a changed page tells, with the diff", async () => {
    const priv = new MemStore();
    const a = await runInSeason({ fetch: okFetch, priv, html: page, now: T("2026-10-28T12:00:00Z") });
    expect(a).toMatchObject({ first: true, events: [] });
    expect((await runInSeason({ fetch: okFetch, priv, html: page, now: T("2026-10-28T18:00:00Z") })).events).toEqual([]);
    const edited = page
      .replace("July 4, 2025 to August 31, 2025", "July 4, 2025 to September 15, 2025") // changed
      .replace(/<tr>(?:(?!<\/tr>)[\s\S])*?Heber Creek(?:(?!<\/tr>)[\s\S])*?April 1, 2026[\s\S]*?<\/tr>/, ""); // removed
    const c = await runInSeason({ fetch: okFetch, priv, html: edited, now: T("2026-10-29T00:00:00Z") });
    expect(c.diff!.changed.map((x) => x.after.effective)).toEqual(["July 4, 2025 to September 15, 2025"]);
    expect(c.events).toHaveLength(1);
    expect(c.events[0]!.text).toMatch(/CHANGED \(1\)[\s\S]*Cowichan River — No fishing \(effective July 4, 2025 to September 15, 2025\)\n\s+was: No fishing \(effective July 4, 2025 to August 31, 2025\)/);
    expect([...priv.m.keys()].filter((k) => k.startsWith("history/"))).toHaveLength(2);
  });
  it("diff pairs a reworded row as changed", () => {
    const r = { section: "R1", water: "A", change: "No fishing", effective: "x" };
    expect(diff([r], [{ ...r, change: "Catch and release" }])).toMatchObject({ added: [], removed: [], changed: [{ before: r }] });
    expect(compose([]).length).toBe(0);
  });
});

describe("the published in-season feed (feeds/inseason/index.json, inseason-feed/1)", () => {
  const page = readFileSync(join(__dirname, "fixtures/inseason/page-2026-10-28.html"), "utf-8");
  it("every row publishes, unmatched rows flagged; ids stable; dates and confident fields parsed", async () => {
    const priv = new MemStore();
    const feed = new MemStore();
    const r = await runInSeason({ fetch: okFetch, priv, feed, html: page, now: T("2026-10-28T12:00:00Z") });
    expect(r.feed).toMatchObject({ rows: 32, unmatched: 32, new: 0, changed: 0, removed: 0 });
    const doc: InSeasonFeed = JSON.parse(feed.m.get("index.json")!);
    expect(doc).toMatchObject({ format: "inseason-feed/1", matching: { method: null, matched: 0, unmatched: 32 } });
    expect(new Set(doc.rows.map((x) => x.id)).size).toBe(32); // Heber Creek's two rows get two ids
    const cow = doc.rows[0]!;
    expect(cow).toMatchObject({ region: "1", water: "Cowichan River", waters: [], matched: false, from: "2025-07-04", to: "2025-08-31", change: "No fishing", parsed: { kinds: ["closure"] }, status: "unchanged" });
    expect(doc.rows.find((x) => x.change.startsWith("Rescind"))!.parsed!.kinds).toEqual(["rescind"]);
    expect(doc.rows.find((x) => x.change.startsWith("Clarification"))!.parsed!.kinds).toEqual(["clarification", "licence"]);
    expect(doc.rows.find((x) => x.change === "No fishing July 30 to September 15.")!.parsed).toEqual({ kinds: ["closure"], window: { from_month: 7, from_day: 30, to_month: 9, to_day: 15 } });
    // an unchanged page: the feed object is not rewritten (only the 24 h heartbeat)
    const puts: string[] = [];
    const watched = Object.assign(Object.create(feed), { put: async (k: string, b: string) => (puts.push(k), feed.put(k, b)) });
    await runInSeason({ fetch: okFetch, priv, feed: watched, html: page, now: T("2026-10-28T18:00:00Z") });
    expect(puts).toEqual([]);
    // a changed page: statuses, previous text, removed rows; ids survive the rewording
    const edited = page.replace("July 4, 2025 to August 31, 2025", "July 4, 2025 to September 15, 2025").replace(/<tr>(?:(?!<\/tr>)[\s\S])*?Peace Canyon Dam[\s\S]*?<\/tr>/, "");
    await runInSeason({ fetch: okFetch, priv, feed: watched, html: edited, now: T("2026-10-29T00:00:00Z") });
    const d2: InSeasonFeed = JSON.parse(feed.m.get("index.json")!);
    expect(d2.rows[0]).toMatchObject({ id: cow.id, status: "changed", previous: { effective: "July 4, 2025 to August 31, 2025" }, to: "2025-09-15", firstSeen: "2026-10-28T12:00:00.000Z", changedAt: "2026-10-29T00:00:00.000Z" });
    expect(d2.removed).toHaveLength(1);
    expect(d2.removed[0]).toMatchObject({ region: "7B", change: "No fishing between Peace Canyon Dam and Hwy 29 Bridge", removedAt: "2026-10-29T00:00:00.000Z" });
    expect(d2.rows.filter((x) => x.status === "unchanged")).toHaveLength(30);
  });
  it("helpers", () => {
    expect(regionOf("Region 7B - Peace")).toBe("7B");
    expect(regionOf("Provincial level information")).toBe("provincial");
    expect(effectiveDates("Sept 16, 2025")).toEqual({ from: "2025-09-16", to: null });
    expect(effectiveDates("whenever")).toEqual({ from: null, to: null });
    expect(parseChange("Trout/char daily quota = 1 (none under 30 cm) and bait ban")).toMatchObject({ kinds: ["limit", "bait"], dailyQuota: 1, species: ["trout", "char"] });
    expect(parseChange("Something new")).toBeUndefined();
    const f = buildFeed([{ section: "Region 2 - Lower Mainland", water: "A", change: "No fishing", effective: "" }], null, "2026-10-28T00:00:00Z", "h", true);
    expect(f.rows[0]!.status).toBe("unchanged"); // the first feed has no "new"
  });
});
