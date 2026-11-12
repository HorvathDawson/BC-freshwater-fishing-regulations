// T225 follow-up (owner 2026-10-28): the owner's notifications — one outbox, deduped, email first (Cloudflare
// send_email), webhook fallback, a daily digest option, a dry run — and the in-season changes check it reports.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { compose, deliver, mime, NOTIFY_STATE, readState, type FeedEvent, type NotifyConfig } from "../src/events";
import { dfoEvents, gaugeEvents, healthEvents } from "../src/index";
import { buildFeed, diff, effectiveDates, undatedEvent, matchedWaters, parseChange, parseInSeason, regionOf, rowId, runInSeason, type InSeasonFeed } from "../src/inseason";
import { repoData } from "../../../conformance/src/repoData";
import { MemStore } from "./dfo.scenario";
import { INSEASON_LINKS_FORMAT } from "../../../packages/core/src/inseason";

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

  // T366 (owner 2026-11-03): DFO's rule-row changes on known places ride the digest; places to review stay urgent
  it("daily digest: a DFO rule-row change (urgent: false) waits for the digest; a DFO place goes now", async () => {
    const { rowsNotice } = await import("../src/dfo/rows");
    const store = new MemStore();
    const { sent, cfg } = mailbox();
    const c = { ...cfg, digest: true, digestHourUtc: 15 };
    const rows = rowsNotice("2", "ab".repeat(32), "2026-11-02", [{
      location_id: "2:fraser-river:mainstem", place: "Fraser River: mainstem waters downstream of the CPR Bridge at Mission",
      old: ["Chum | Oct 1 to Oct 31 | No fishing for salmon"], new: ["Chum | Nov 3 to Nov 30 | 2 per day — FN1179"], fns: ["FN1179"],
      read: [{ row: "Chum | Nov 3 to Nov 30 | 2 per day — FN1179", read: ["retention_limit · chum · Nov 3–Nov 30 · take 2"] }],
    }]);
    const place = { key: "dfo-drift:2:ffff", text: "DFO Region 2: new wording on Fraser River, likely 2:fraser-river:x (0.90) — 1 rule held unplaced." };
    const evs = dfoEvents([rows, place], "2026-11-03T09:00:00Z");
    expect(evs.map((e) => [e.key, e.urgent])).toEqual([[rows.key, false], ["dfo-drift:2:ffff", true]]);
    const r1 = await deliver(evs, { store, config: c, fetch: okFetch, now: T("2026-11-03T09:00:00Z") });
    expect(r1.messages).toHaveLength(1);
    expect(r1.messages[0]!.keys).toEqual(["dfo-drift:2:ffff"]);
    expect(r1.queued).toBe(1);
    const r2 = await deliver([], { store, config: c, fetch: okFetch, now: T("2026-11-03T15:05:00Z") });
    expect(r2.messages.map((m) => m.keys)).toEqual([[rows.key]]);
    expect(r2.messages[0]!.subject).toBe("[staging] Daily digest: DFO: rule rows changed on 1 place (Region 2)");
    const body = bodyOf(sent.at(-1)!.raw);
    expect(body).toContain("Fraser River: mainstem waters downstream of the CPR Bridge at Mission");
    expect(body).toMatch(/Was[\s\S]*Chum \| Oct 1 to Oct 31 \| No fishing for salmon/);
    expect(body).toMatch(/Now[\s\S]*Chum \| Nov 3 to Nov 30 \| 2 per day — FN1179/);
    expect(body).toContain("read as: retention_limit · chum · Nov 3–Nov 30 · take 2");
    expect(body).toContain("nothing to re-review");
    expect(body).toContain("python -m pipeline.regs.dfo_salmon.live run --regions 2 --dry-run");
    // without the digest setting every DFO notice is emailed at once, the row change included
    const s2 = new MemStore();
    const inst = await deliver(dfoEvents([rows], "2026-11-03T09:00:00Z"), { store: s2, config: cfg, fetch: okFetch, now: T("2026-11-03T09:00:00Z") });
    expect(inst.messages.map((m) => m.keys)).toEqual([[rows.key]]);
  });

  it("gauge failures never email alone, even flagged urgent; the digest carries them", async () => {
    const store = new MemStore();
    const { sent, cfg } = mailbox();
    const c = { ...cfg, digest: true, digestHourUtc: 15 };
    const g = (key: string) => ({ ...ev(key, "health", "Gauge feed FAILED"), urgent: true, data: { job: "gauges", from: "ok", to: "failing", detail: "x" } });
    const r1 = await deliver([g("health:gauges:ok>failing:a")], { store, config: c, fetch: okFetch, now: T("2026-10-28T09:00:00Z") });
    expect(r1.messages).toEqual([]);
    expect(sent).toHaveLength(0);
    const r2 = await deliver([], { store, config: c, fetch: okFetch, now: T("2026-10-28T15:05:00Z") });
    expect(r2.messages).toHaveLength(1);
    expect(r2.messages[0]!.subject).toMatch(/^\[staging\] Daily digest: /);
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
        { kind: "roster", since: "x", current: repoData("source/bc_hydrometric_stations.json"), latest: "+2/-1", action: "python -m …" },
      ],
      "t",
    );
    // (T554 item 4) the envelope in use is in the key: an older clim.json put back is a new event, told again
    expect(h).toMatchObject({ key: "hydat:20261015:20250115", kind: "hydat", title: "New HYDAT 20261015 — run the HYDAT action" });
    expect(h!.text).toMatch(/feeds — HYDAT → climatology → R2/);
    expect(r).toMatchObject({ key: "roster:+2/-1", kind: "roster" });
    const [d] = dfoEvents([{ key: "dfo-new:6:abc", text: "DFO Region 6: new location on Skeena River: 'x' — 2 rules held unplaced. Place it in DFO review after: …" }], "t");
    // (T549 item 9) the Mac reads DFO's page first: `queue` alone reads the Mac's own unplaced.json, never the worker's
    expect(d!.text).toContain("First: python -m pipeline.regs.dfo_salmon.live run --regions 6 && python -m pipeline.regs.dfo_salmon.live queue --regions 6\n");
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
    // (T609 W14) the links file is there (none bound): once a feed is published a missing one fails the run
    feed.m.set("matches.json", JSON.stringify({ format: INSEASON_LINKS_FORMAT, release: "r-1", notices: {} }));
    const r = await runInSeason({ fetch: okFetch, priv, feed, html: page, now: T("2026-10-28T12:00:00Z") });
    expect(r.feed).toMatchObject({ rows: 32, unmatched: 32, new: 0, changed: 0, removed: 0 });
    const doc: InSeasonFeed = JSON.parse(feed.m.get("index.json")!);
    expect(doc).toMatchObject({ format: "inseason-feed/1", matching: { matched: 0, unmatched: 32 } });
    expect(new Set(doc.rows.map((x) => x.id)).size).toBe(32); // Heber Creek's two rows get two ids
    const cow = doc.rows[0]!;
    expect(cow).toMatchObject({ region: "1", water: "Cowichan River", waters: [], matched: false, from: "2025-07-04", to: "2025-08-31", change: "No fishing", parsed: { kinds: ["closure"] }, status: "unchanged" });
    expect(doc.rows.find((x) => x.change.startsWith("Rescind"))!.parsed!.kinds).toEqual(["rescind"]);
    expect(doc.rows.find((x) => x.change.startsWith("Clarification"))!.parsed!.kinds).toEqual(["clarification", "licence"]);
    expect(doc.rows.find((x) => x.change === "No fishing July 30 to September 15.")!.parsed).toEqual({ kinds: ["closure"], window: { from_month: 7, from_day: 30, to_month: 9, to_day: 15 } });
    // (T549 item 7) an unchanged page: the feed is still written, so `fetchedAt` is this check; the rows stand as they were
    const puts: string[] = [];
    const watched = Object.assign(Object.create(feed), { put: async (k: string, b: string) => (puts.push(k), feed.put(k, b)) });
    await runInSeason({ fetch: okFetch, priv, feed: watched, html: page, now: T("2026-10-28T18:00:00Z") });
    expect(puts).toEqual(["index.json"]);
    const again: InSeasonFeed = JSON.parse(feed.m.get("index.json")!);
    expect(again.fetchedAt).toBe("2026-10-28T18:00:00.000Z");
    expect({ ...again, fetchedAt: doc.fetchedAt }).toEqual(doc);
    // a changed page: statuses, previous text, removed rows; ids survive the rewording
    const edited = page.replace("July 4, 2025 to August 31, 2025", "July 4, 2025 to September 15, 2025").replace(/<tr>(?:(?!<\/tr>)[\s\S])*?Peace Canyon Dam[\s\S]*?<\/tr>/, "");
    await runInSeason({ fetch: okFetch, priv, feed: watched, html: edited, now: T("2026-10-29T00:00:00Z") });
    const d2: InSeasonFeed = JSON.parse(feed.m.get("index.json")!);
    expect(d2.rows[0]).toMatchObject({ id: cow.id, status: "changed", previous: { effective: "July 4, 2025 to August 31, 2025" }, to: "2025-09-15", firstSeen: "2026-10-28T12:00:00.000Z", changedAt: "2026-10-29T00:00:00.000Z" });
    expect(d2.removed).toHaveLength(1);
    expect(d2.removed[0]).toMatchObject({ region: "7B", change: "No fishing between Peace Canyon Dam and Hwy 29 Bridge", removedAt: "2026-10-29T00:00:00.000Z" });
    expect(d2.rows.filter((x) => x.status === "unchanged")).toHaveLength(30);
  });
  it("(T520) a row's id is its content: stable under reorder, a new id for an amended text (status changed)", () => {
    const R = (water: string, change: string) => ({ section: "Region 1 - Vancouver Island", water, change, effective: "" });
    const a = buildFeed([R("Heber Creek", "No fishing"), R("Heber Creek", "Bait ban")], null, "2026-11-01T00:00:00Z", "h", true);
    // a DFO-style insert above: the existing rows keep their ids (the position never mattered)
    const b = buildFeed([R("Heber Creek", "Catch and release"), R("Heber Creek", "No fishing"), R("Heber Creek", "Bait ban")],
                        a, "2026-11-02T00:00:00Z", "h2", true);
    expect(b.rows.slice(1).map((r) => r.id)).toEqual(a.rows.map((r) => r.id));
    expect(b.rows.map((r) => r.status)).toEqual(["new", "unchanged", "unchanged"]);
    // spacing / case in the text: the same notice
    expect(rowId("1", "Heber Creek", "No  fishing")).toBe(rowId("1", "heber creek", "no fishing"));
    expect(rowId("1", "Heber Creek", "No fishing")).toBe("is-03e14f544b83");      // pinned: the curated file's ids
    // typography is not a new notice: curly quotes, dashes, NFKC
    expect(rowId("3", "Fraser River", "Rescind the \u201cNo Fishing\u201d closure \u2013 Sept\u00a01"))
      .toBe(rowId("3", "Fraser River", 'Rescind the "No Fishing" closure - Sept 1'));
    // the same words on two dates: the dates tell them apart; a lone row's id ignores its date
    const H = (eff: string) => ({ section: "Region 1 - Vancouver Island", water: "Heber Creek", change: "No fishing", effective: eff });
    const two = buildFeed([H("July 1, 2025"), H("Aug 1, 2025")], null, "2026-11-01T00:00:00Z", "h", true);
    expect(new Set(two.rows.map((r) => r.id)).size).toBe(2);
    expect(buildFeed([H("July 1, 2025")], null, "2026-11-01T00:00:00Z", "h", true).rows[0]!.id).toBe(rowId("1", "Heber Creek", "No fishing"));
    // one notice printed twice (every field): ONE row, never a failed run
    const twice = buildFeed([H("July 1, 2025"), H("July 1, 2025")], null, "2026-11-01T00:00:00Z", "h", true);
    expect(twice.rows.map((r) => r.id)).toEqual([rowId("1", "Heber Creek", "No fishing")]);
    // rows that would share an id but differ as printed (spacing): refused — never told apart by position
    expect(() => buildFeed([H("July 1, 2025"), H("July  1, 2025")], null, "2026-11-01T00:00:00Z", "h", true))
      .toThrow(/differ only in how they are printed/);
    // amended: a new id, shown as a change of the row it replaced, which leaves the page
    const c = buildFeed([R("Heber Creek", "No fishing until Dec 1"), R("Heber Creek", "Bait ban")], a, "2026-11-03T00:00:00Z", "h3", true);
    expect(c.rows[0]!.id).not.toBe(a.rows[0]!.id);
    expect(c.rows[0]).toMatchObject({ status: "changed", previous: { change: "No fishing" } });
    expect(c.removed.map((r) => r.id)).toEqual([a.rows[0]!.id]);
  });
  it("(T675) a re-key is not a change and not a removal: the row keeps its firstSeen and status, `removed` gains nothing", () => {
    const page = readFileSync(join(__dirname, "fixtures/inseason/page-2026-10-28.html"), "utf-8");
    const rows = parseInSeason(page);
    // the feed as an older id scheme published it: the same notices under other ids, first seen in October
    const now = buildFeed(rows, null, "2026-10-28T00:00:00Z", "h", true);
    const old: InSeasonFeed = { ...now, rows: now.rows.map((r, i) => ({ ...r, id: `is-old-scheme-${i}`, status: i === 0 ? "new" as const : r.status })) };
    // the page did not move; only the ids did (production's first run with the content ids)
    const a = buildFeed(rows, old, "2026-11-12T00:00:00Z", "h", false);
    expect(a.rows.map((r) => r.id)).toEqual(now.rows.map((r) => r.id));
    expect(a.removed).toEqual([]);
    expect(a.rows.map((r) => [r.status, r.previous, r.firstSeen, r.changedAt]))
      .toEqual(old.rows.map((r) => [r.status, undefined, "2026-10-28T00:00:00Z", "2026-10-28T00:00:00Z"]));
    expect(a.rows[0]!.status).toBe("new");
    // a re-key in the same reading as real edits: one row amended, one gone — only those two are said
    const gone = rows.find((r) => /Peace Canyon Dam/.test(r.change))!;
    // … and a new notice on a water whose other notice was only re-keyed: new, never "changed from" that one
    const edited = [...rows.filter((r) => r !== gone).map((r, i) => (i === 0 ? { ...r, change: `${r.change} (amended)` } : r)),
                    { ...rows[1]!, change: "Bait ban (a new notice)" }];
    const b = buildFeed(edited, old, "2026-11-12T00:00:00Z", "h2", true);
    expect([b.rows.at(-1)!.status, b.rows.at(-1)!.previous]).toEqual(["new", undefined]);
    expect(b.rows.filter((r) => r.status === "changed")).toHaveLength(1);
    expect(b.rows[0]).toMatchObject({ status: "changed", previous: { change: rows[0]!.change, effective: rows[0]!.effective } });
    expect(b.rows.filter((r) => r.status === "unchanged")).toHaveLength(30);
    expect(b.rows.slice(1, -1).every((r) => r.firstSeen === "2026-10-28T00:00:00Z")).toBe(true);
    expect(b.removed.map((r) => r.change)).toEqual([rows[0]!.change, gone.change]);
    // a sibling notice leaves, so the date drops out of the survivor's id: the same notice still
    const H = (eff: string) => ({ section: "Region 1 - Vancouver Island", water: "Heber Creek", change: "No fishing", effective: eff });
    const two = buildFeed([H("July 1, 2025"), H("Aug 1, 2025")], null, "2026-11-01T00:00:00Z", "h", true);
    const one = buildFeed([H("July 1, 2025")], two, "2026-11-05T00:00:00Z", "h2", true);
    expect(one.rows[0]!.id).not.toBe(two.rows[0]!.id);
    expect(one.rows[0]).toMatchObject({ status: "unchanged", firstSeen: "2026-11-01T00:00:00Z" });
    expect(one.rows[0]!.previous).toBeUndefined();
    expect(one.removed.map((r) => r.effective)).toEqual(["Aug 1, 2025"]);
  });
  it("(T675) the staging feed the old rule wrote is put right by the next run: no false change, no live notice in `removed`", () => {
    // staging's feeds/inseason/index.json as read 2026-11-11, after the T520 re-key of 2026-11-08: every live row
    // "changed" from its own text, and `removed` holding the same notices under their old ids
    const staging: InSeasonFeed = JSON.parse(readFileSync(join(__dirname, "fixtures/inseason/feed-staging-rekeyed-2026-11-11.json"), "utf-8"));
    const text = (r: { region: string; water: string; change: string; effective: string }) => `${r.region}|${r.water}|${r.change}|${r.effective}`;
    const liveText = new Set(staging.rows.map(text));
    expect([staging.rows.length, staging.rows.filter((r) => r.status === "changed" && r.previous && text({ ...r, ...r.previous }) === text(r)).length,
            staging.removed.length, staging.removed.filter((r) => liveText.has(text(r))).length]).toEqual([32, 32, 38, 38]);
    const rows = staging.rows.map((r) => ({ section: r.section, water: r.water, change: r.change, effective: r.effective }));
    const next = buildFeed(rows, staging, "2026-11-12T06:00:00Z", staging.pageHash, false);
    expect(next.rows.map((r) => r.id)).toEqual(staging.rows.map((r) => r.id));
    expect(next.rows.map((r) => [r.status, r.previous])).toEqual(staging.rows.map(() => ["unchanged", undefined]));
    expect(next.removed).toEqual([]);
    // what the old rule lost stays lost: the reset firstSeen is what the feed holds (nothing recorded the earlier one)
    expect(next.rows.map((r) => r.firstSeen)).toEqual(staging.rows.map((r) => r.firstSeen));
    // a notice that really left before the re-key stays in `removed`
    const real = { id: "is-000000000000", region: "3", water: "Gone Creek", change: "No fishing", effective: "", removedAt: "2026-11-01T00:00:00Z" };
    expect(buildFeed(rows, { ...staging, removed: [...staging.removed, real] }, "2026-11-12T06:00:00Z", staging.pageHash, false).removed).toEqual([real]);
  });
  it("(T520) matched / unmatched are the curated links' true counts; links of another format fail the run", async () => {
    const page = readFileSync(join(__dirname, "fixtures/inseason/page-2026-10-28.html"), "utf-8");
    const first = buildFeed(parseInSeason(page), null, "2026-10-28T00:00:00Z", "h", true);
    const links = { format: INSEASON_LINKS_FORMAT, release: "r-1", notices: { [first.rows[0]!.id]: { waters: { "gnis:12227": { all: true } } } } };
    const feed = new MemStore();
    await feed.put("matches.json", JSON.stringify(links));
    const r = await runInSeason({ fetch: okFetch, priv: new MemStore(), feed, html: page, now: T("2026-10-28T12:00:00Z") });
    expect(r.feed).toMatchObject({ rows: 32, unmatched: 31 });
    const doc: InSeasonFeed = JSON.parse(feed.m.get("index.json")!);
    expect(doc.matching).toEqual({ method: "curated", matched: 1, unmatched: 31 });
    expect(doc.rows[0]).toMatchObject({ waters: ["gnis:12227"], matched: true });
    expect(() => matchedWaters(JSON.stringify({ format: "inseason-links/1", matches: {} }))).toThrow(/not inseason-links\/3/);
    // T517's /3 is what the Mac publishes; a /2 file (before it) is refused by name, never read as a /3
    expect(() => matchedWaters(JSON.stringify({ format: "inseason-links/2", notices: {} }))).toThrow(/is inseason-links\/2, not inseason-links\/3/);
  });
  it("(T520) a page printing a row twice: the feed publishes it once, says so once, and the run succeeds", async () => {
    const page = readFileSync(join(__dirname, "fixtures/inseason/page-2026-10-28.html"), "utf-8");
    const row = /<tr>(?:(?!<\/tr>)[\s\S])*?Peace Canyon Dam[\s\S]*?<\/tr>/.exec(page)![0];
    const doubled = page.replace(row, row + row);
    const feed = new MemStore(), priv = new MemStore();
    const r = await runInSeason({ fetch: okFetch, priv, feed, html: doubled, now: T("2026-10-28T12:00:00Z") });
    expect(r.feed).toMatchObject({ rows: 32 });
    expect(r.events.filter((e) => e.key.startsWith("inseason-duplicate:"))).toHaveLength(1);
    feed.m.set("matches.json", JSON.stringify({ format: INSEASON_LINKS_FORMAT, release: "r-1", notices: {} }));   // (T609 W14)
    const again = await runInSeason({ fetch: okFetch, priv, feed, html: doubled, now: T("2026-10-28T18:00:00Z") });
    // the same duplicate keeps its key: the notifier sends it once
    expect(again.events.map((e) => e.key)).toEqual(r.events.filter((e) => e.key.startsWith("inseason-duplicate:")).map((e) => e.key));
  });
  it("helpers", () => {
    expect(regionOf("Region 7B - Peace")).toBe("7B");
    expect(regionOf("Provincial level information")).toBe("provincial");
    expect(effectiveDates("Sept 16, 2025")).toEqual({ from: "2025-09-16", to: null });
    expect(effectiveDates("whenever")).toEqual({ from: null, to: null });
    // (T548) an open end keeps its start; a start with no year takes the end's
    expect(effectiveDates("April 1, 2027 until further notice")).toEqual({ from: "2027-04-01", to: null });
    expect(effectiveDates("July 30 to September 15, 2026")).toEqual({ from: "2026-07-30", to: "2026-09-15" });
    expect(effectiveDates("July 4, 2025 to August 31, 2025")).toEqual({ from: "2025-07-04", to: "2025-08-31" });
    const row = (effective: string) => ({ section: "Region 3", water: "W", change: "No fishing", effective });
    expect(undatedEvent([row("Sept 16, 2025"), row(""), row("April 1, 2027 until further notice")], "t")).toEqual([]);
    const [u] = undatedEvent([row("whenever"), row("Sept 16, 2025")], "t");
    expect([u!.key.startsWith("inseason-undated:"), u!.title, (u!.data as { undated: unknown[] }).undated.length])
      .toEqual([true, "1 in-season row has an effective date the feed cannot read (shown as in force now)", 1]);
    expect(parseChange("Trout/char daily quota = 1 (none under 30 cm) and bait ban")).toMatchObject({ kinds: ["limit", "bait"], dailyQuota: 1, species: ["trout", "char"] });
    expect(parseChange("Something new")).toBeUndefined();
    const f = buildFeed([{ section: "Region 2 - Lower Mainland", water: "A", change: "No fishing", effective: "" }], null, "2026-10-28T00:00:00Z", "h", true);
    expect(f.rows[0]!.status).toBe("unchanged"); // the first feed has no "new"
  });
});

// T558 item 3: with no email secret every owner message was logged and recorded as told; deploys did not check for it
describe("no channel (T558 item 3)", () => {
  it("production keeps a message it cannot send; staging may log it; /status.json says which channel", async () => {
    const { channelOf } = await import("../src/events");
    const ev: FeedEvent = { key: "dfo-new:1", kind: "dfo", title: "t", text: "x", at: "2026-11-10T00:00:00Z" };
    const prod = new MemStore();
    const r = await deliver([ev], { store: prod, config: { ctx: { env: "production", statusUrl: null } }, fetch: okFetch, now: T("2026-11-10T01:00:00Z") });
    expect(r.failed).toBe(1);
    expect((await readState(prod)).pending.map((e) => e.key)).toEqual(["dfo-new:1"]);
    expect((await readState(prod)).told["dfo-new:1"]).toBeUndefined();
    const stg = new MemStore();
    await deliver([ev], { store: stg, config: { ctx: { env: "staging", statusUrl: null } }, fetch: okFetch, now: T("2026-11-10T01:00:00Z") });
    expect((await readState(stg)).told["dfo-new:1"]).toBeDefined();
    expect(channelOf({})).toBe("log");
    expect(channelOf({ webhook: "https://h" })).toBe("webhook");
    const { default: worker } = await import("../src/index");
    const bucket = { get: async () => null, put: async () => {}, delete: async () => {} };
    const j = await (await worker.fetch(new Request("https://w/status.json"), { BUCKET: bucket, FEEDS_ENV: "production" } as never)).json() as { notify: unknown };
    expect(j.notify).toEqual({ channel: "log", digest: false });
    const h = await (await worker.fetch(new Request("https://w/status"), { BUCKET: bucket } as never)).text();
    expect(h).toContain("nobody is told");
  });
  it("both deploy scripts refuse a worker without NOTIFY_EMAIL_TO", () => {
    for (const env of ["staging", "prod"]) {
      const sh = readFileSync(join(__dirname, `../../../../deploy/${env}/feeds.sh`), "utf-8");
      expect(sh).toMatch(/secret list[^\n]*\|\s*grep -q 'NOTIFY_EMAIL_TO'/);
      expect(sh.indexOf("NOTIFY_EMAIL_TO")).toBeLessThan(sh.lastIndexOf("wrangler deploy -c"));
    }
  });
});

// T609 W4: a redirect to another gov.bc.ca page of the same build would have been published as regulation changes
describe("only the in-season page is read (T609 W4)", () => {
  const page = readFileSync(join(__dirname, "fixtures/inseason/page-2026-10-28.html"), "utf-8");
  it("a table without the water / change / effective header, or under an unknown section, fails the parse", () => {
    expect(parseInSeason(page)).toHaveLength(32);
    expect(() => parseInSeason(page.replace(/Effective date/g, "Price"))).toThrow(/no water \/ change \/ effective header/);
    expect(() => parseInSeason(page.replace("Region 3 - Thompson", "Hunting news"))).toThrow(/unknown section "Hunting news/);
  });
  it("a redirected answer fails the run as parse, nothing written", async () => {
    const priv = new MemStore();
    const redirected = async () => new Response(null, { status: 301, headers: { location: "https://www2.gov.bc.ca/other" } });
    await expect(runInSeason({ fetch: redirected, priv })).rejects.toMatchObject({ cls: "parse", message: expect.stringMatching(/redirected to https:\/\/www2\.gov\.bc\.ca\/other/) });
    expect(priv.m.size).toBe(0);
  });
});

// T609 W14: a missing matches.json published every row unmatched and recorded the run ok
it("(T609 W14) once a feed is published, a missing matches.json fails the run and writes nothing", async () => {
  const page = readFileSync(join(__dirname, "fixtures/inseason/page-2026-10-28.html"), "utf-8");
  const feed = new MemStore();
  feed.m.set("index.json", JSON.stringify({ format: "inseason-feed/1" }));
  const before = feed.m.get("index.json");
  await expect(runInSeason({ fetch: okFetch, priv: new MemStore(), feed, html: page, now: T("2026-10-28T12:00:00Z") }))
    .rejects.toMatchObject({ cls: "parse", message: expect.stringMatching(/matches\.json is missing/) });
  expect(feed.m.get("index.json")).toBe(before);
});
