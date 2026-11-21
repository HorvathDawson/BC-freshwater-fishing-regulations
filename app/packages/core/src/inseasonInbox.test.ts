/**
 * (T700, T694) The in-season inbox: what enters, unread and read, "Updated" only when the feed links it, how long a
 * notice is kept, the device's saved history round trip, and the words — each pinned whole (AGENTS 33).
 */
import { describe, expect, it } from "vitest";
import { INBOX_KEEP_DAYS_AFTER_END, INBOX_MAX_ITEMS, inboxDetail, inboxMark, inboxMerge, inboxPrompted, inboxRead, inboxReadAll,
         inboxTarget, inboxText, inboxUnread, inboxVM, inboxWantsOpen, parseInbox, type InboxState } from "./inseasonInbox";
import { joinInSeason, type InSeasonFeed, type InSeasonLinks, type InSeasonRow } from "./inseason";

const SRC = "https://www2.gov.bc.ca/in-season";
const row = (o: Partial<InSeasonRow> & { id: string }): InSeasonRow =>
  ({ region: "4", water: "Kaslo River", effective: "Nov 1, 2026", from: null, to: null, change: "No fishing", status: "unchanged",
     firstSeen: "2026-11-01T10:00:00Z", changedAt: "2026-11-01T10:00:00Z", ...o });
const feed = (rows: InSeasonRow[], removed: { id: string; removedAt: string }[] = []): InSeasonFeed =>
  ({ format: "inseason-feed/1", fetchedAt: "2026-11-13T12:00:00Z", source: SRC, rows, removed });
const links = (m: Record<string, string[]>): InSeasonLinks => ({ format: "inseason-links/4", notices: Object.fromEntries(
  Object.entries(m).map(([id, ws]) => [id, { waters: Object.fromEntries(ws.map((w) => [w, { all: true as const }])) }])) }) as InSeasonLinks;
/** one feed read on `day`, folded into `s` */
const read = (s: InboxState | null, f: InSeasonFeed, l: InSeasonLinks, day: string, seen?: ReadonlySet<string>) =>
  inboxMerge(s, f, joinInSeason(f, l, day), day, seen);

describe("in-season inbox (T700)", () => {
  const A = row({ id: "is-a", water: "Kaslo River" });
  const B = row({ id: "is-b", region: "8", water: "Okanagan River", change: "Closed to all fishing" });
  const L = links({ "is-a": ["gnis:1"] });

  it("a first read starts quiet: a matched notice is read, one not on the map yet is unread (unless T230's notice showed it)", () => {
    const s = read(null, feed([A, B]), L, "2026-11-13");
    expect(s.items.map((i) => [i.id, i.read, i.waters])).toEqual([["is-a", true, ["gnis:1"]], ["is-b", false, null]]);
    expect([inboxUnread(s), inboxWantsOpen(s)]).toEqual([1, true]);
    const seen = read(null, feed([A, B]), L, "2026-11-13", new Set(["is-b"]));
    expect([inboxUnread(seen), inboxWantsOpen(seen)]).toEqual([0, false]);
  });

  it("unread count: a notice met after the first read is unread, matched or not; it opens by itself only for one not on the map", () => {
    const s0 = read(null, feed([A]), L, "2026-11-13");
    const C = row({ id: "is-c", water: "Duncan River", firstSeen: "2026-11-14T08:00:00Z" });
    const s1 = read(s0, feed([A, C]), links({ "is-a": ["gnis:1"], "is-c": ["gnis:3"] }), "2026-11-14");
    expect([inboxUnread(s1), inboxWantsOpen(s1), inboxMark(s1, "is-c")]).toEqual([1, false, "new"]);
    const s2 = read(s1, feed([A, C, B]), links({ "is-a": ["gnis:1"], "is-c": ["gnis:3"] }), "2026-11-14");
    expect([inboxUnread(s2), inboxWantsOpen(s2)]).toEqual([2, true]);
    // it opened by itself once: not again, still unread
    const s3 = inboxPrompted(s2);
    expect([inboxUnread(s3), inboxWantsOpen(s3)]).toEqual([2, false]);
  });

  it("read on open; Mark all as read; a read notice carries no mark on its page", () => {
    const s0 = read(null, feed([]), L, "2026-11-12");
    const s1 = read(s0, feed([A, B]), L, "2026-11-13");
    expect(inboxUnread(s1)).toBe(2);
    const opened = inboxRead(s1, ["is-a"]);
    expect([inboxUnread(opened), inboxMark(opened, "is-a"), inboxMark(opened, "is-b")]).toEqual([1, null, "new"]);
    expect(inboxUnread(inboxReadAll(s1))).toBe(0);
    // a later read of the same feed keeps it read
    expect(inboxUnread(read(opened, feed([A, B]), L, "2026-11-13"))).toBe(1);
  });

  it("(T694) UPDATED only when the feed links it: status changed + previous; the amended one leaves; an unlinked new id is NEW and the old one ENDS", () => {
    const s0 = read(null, feed([A]), L, "2026-11-10");
    const A2 = row({ id: "is-a2", water: "Kaslo River", change: "No fishing until Dec 31", status: "changed",
                     previous: { change: "No fishing", effective: "Nov 1, 2026" }, changedAt: "2026-11-12T09:00:00Z" });
    const linked = read(s0, feed([A2], [{ id: "is-a", removedAt: "2026-11-12T09:00:00Z" }]), links({ "is-a2": ["gnis:1"] }), "2026-11-12");
    expect(linked.items.map((i) => [i.id, i.arrived, i.read, i.at, i.ended])).toEqual([["is-a2", "updated", false, "2026-11-12", null]]);
    expect(inboxMark(linked, "is-a2")).toBe("updated");
    // the same new text with NO link from the feed: new, and the old notice ended the day it left the page
    const A3 = row({ id: "is-a2", water: "Kaslo River", change: "No fishing until Dec 31", status: "new", changedAt: "2026-11-12T09:00:00Z",
                     firstSeen: "2026-11-12T09:00:00Z" });
    const unlinked = read(s0, feed([A3], [{ id: "is-a", removedAt: "2026-11-12T09:00:00Z" }]), links({ "is-a2": ["gnis:1"] }), "2026-11-12");
    expect(unlinked.items.map((i) => [i.id, i.arrived, i.read, i.ended])).toEqual([["is-a2", "new", false, null], ["is-a", "new", true, "2026-11-12"]]);
    expect(inboxMark(unlinked, "is-a")).toBeNull();
    // a date amended in place (the id kept) and linked: updated, unread again
    const Ad = row({ id: "is-a", effective: "Nov 5, 2026", status: "changed", previous: { change: "No fishing", effective: "Nov 1, 2026" },
                     changedAt: "2026-11-12T09:00:00Z" });
    const inPlace = read(s0, feed([Ad]), L, "2026-11-12");
    expect(inPlace.items.map((i) => [i.id, i.arrived, i.read, i.effective])).toEqual([["is-a", "updated", false, "Nov 5, 2026"]]);
    // read once, the next reading of the same feed does not flag it again
    expect(inboxUnread(read(inboxRead(inPlace, ["is-a"]), feed([Ad]), L, "2026-11-13"))).toBe(0);
  });

  it(`retention: an ended notice stays ${INBOX_KEEP_DAYS_AFTER_END} days after it ended, then drops; at most ${INBOX_MAX_ITEMS}`, () => {
    const T = row({ id: "is-t", from: "2026-11-01", to: "2026-11-20" });
    const s0 = read(null, feed([T, B]), links({ "is-t": ["gnis:1"] }), "2026-11-13");
    // its `to` passed: ended that day
    const s1 = read(s0, feed([T, B]), links({ "is-t": ["gnis:1"] }), "2026-11-21");
    expect(s1.items.find((i) => i.id === "is-t")?.ended).toBe("2026-11-20");
    // B left the page on Nov 25
    const s2 = read(s1, feed([T], [{ id: "is-b", removedAt: "2026-11-25T03:00:00Z" }]), links({ "is-t": ["gnis:1"] }), "2026-11-26");
    expect(s2.items.find((i) => i.id === "is-b")?.ended).toBe("2026-11-25");
    expect(read(s2, feed([T]), links({}), "2026-12-20").items.map((i) => i.id)).toEqual(["is-b", "is-t"]);
    expect(read(s2, feed([T]), links({}), "2026-12-21").items.map((i) => i.id)).toEqual(["is-b"]);
    expect(read(s2, feed([T]), links({}), "2026-12-26").items.map((i) => i.id)).toEqual([]);
    // the cap: the newest are kept
    const many = Array.from({ length: INBOX_MAX_ITEMS + 5 }, (_, n) =>
      row({ id: `is-${String(n).padStart(3, "0")}`, firstSeen: `2026-${n < 100 ? "10" : "11"}-${String((n % 28) + 1).padStart(2, "0")}T00:00:00Z` }));
    const capped = read(null, feed(many), links({}), "2026-11-30");
    expect(capped.items).toHaveLength(INBOX_MAX_ITEMS);
    expect(capped.items.every((i) => i.at >= "2026-10-01")).toBe(true);
    expect(capped.items.some((i) => i.id === "is-000")).toBe(false);
  });

  it("survives a restart: the saved text reads back the same; a history it cannot read throws, naming it", () => {
    const s = inboxRead(read(read(null, feed([]), L, "2026-11-12"), feed([A, B]), L, "2026-11-13"), ["is-b"]);
    expect(parseInbox(inboxText(s))).toEqual(s);
    expect(parseInbox(null)).toBeNull();
    expect(() => parseInbox(JSON.stringify({ format: "inseason-inbox/9", items: [] })))
      .toThrow("in-season inbox: inseason-inbox/9, but this app reads inseason-inbox/1");
    expect(() => parseInbox(JSON.stringify({ format: "inseason-inbox/1", items: [{ id: 1 }] }))).toThrow("in-season inbox: item 0 cannot be read");
    expect(() => parseInbox(JSON.stringify({ format: "inseason-inbox/1", items: [], known: [3] }))).toThrow("in-season inbox: its known notices cannot be read");
  });

  it("where a row goes: its one water's page; several waters, none, or ended: its text", () => {
    const M = row({ id: "is-m", water: "Region 8 lakes" });
    const s = read(read(null, feed([]), L, "2026-11-12"), feed([A, B, M]), links({ "is-a": ["gnis:1"], "is-m": ["gnis:5", "gnis:6"] }), "2026-11-13");
    expect([inboxTarget(s, "is-a"), inboxTarget(s, "is-b"), inboxTarget(s, "is-m")]).toEqual([{ item: "gnis:1" }, null, null]);
    expect(inboxDetail(s, "is-m", "2026-11-13")?.note).toBe("Shown on the Regulations page of each of the 2 waters it applies to.");
    expect(inboxDetail(s, "is-b", "2026-11-13")).toEqual({ id: "is-b", tag: "New", head: "Okanagan River · Region 8", dates: null,
      text: "Closed to all fishing", url: SRC, note: "I have not placed this change on the map yet. Check it before you fish." });
  });

  it("the list: New (unread) ahead of Earlier (read), newest first, each row's words whole", () => {
    const s0 = read(null, feed([row({ id: "is-old", water: "Slocan River", firstSeen: "2025-09-02T00:00:00Z" })]), links({ "is-old": ["gnis:9"] }), "2026-11-12");
    // (T983, owner 2026-11-21) the date is the notice's own (Kaslo: effective Nov 1, first seen Nov 12); one that prints
    // none says when it was first seen, as that
    const s = read(s0, feed([row({ id: "is-old", water: "Slocan River", firstSeen: "2025-09-02T00:00:00Z" }),
                             { ...A, from: "2026-11-01", firstSeen: "2026-11-12T00:00:00Z" },
                             { ...B, firstSeen: "2026-11-13T00:00:00Z" }]), links({ "is-old": ["gnis:9"], "is-a": ["gnis:1"] }), "2026-11-13");
    const vm = inboxVM(s, "2026-11-13");
    expect(vm.groups.map((g) => [g.title, g.rows.map((r) => r.id)])).toEqual([["New", ["is-b", "is-a"]], ["Earlier", ["is-old"]]]);
    expect(vm.groups[0]!.rows.map((r) => r.label)).toEqual([
      "Unread. New: Okanagan River, Region 8, first seen Nov 13. Not on the map yet. Closed to all fishing",
      "Unread. New: Kaslo River, Region 4, Nov 1. No fishing"]);
    expect(vm.groups[0]!.rows.map((r) => r.date)).toEqual(["first seen Nov 13", "Nov 1"]);
    expect(vm.groups[1]!.rows[0]!.label).toBe("Posted: Slocan River, Region 4, first seen Sep 2, 2025. No fishing");
    expect([vm.unread, vm.empty]).toEqual([2, false]);
    expect(inboxVM(null, "2026-11-13")).toEqual({ groups: [], unread: 0, empty: true });
  });
});

describe("(T983) a notice this device met begun is never news again", () => {
  // the production feed's row (2026-11-21): 0.8.3 ended it on Sept 15 from its printed window; 0.8.4 (T548) stopped
  // inferring that end, so it came back into force and showed as "New · Nov 19" (the feeds worker's firstSeen)
  const OK = row({ id: "is-2fa458fef3c3", region: "8", water: "Okanagan River", effective: "July 30, 2026", from: "2026-07-30",
                   change: "No fishing July 30 to September 15.", firstSeen: "2026-11-19T05:14:57.566Z", changedAt: "2026-11-19T05:14:57.566Z" });
  const A = row({ id: "is-a", water: "Kaslo River" });
  const L = links({ "is-a": ["gnis:1"], "is-2fa458fef3c3": ["gnis:2"] });
  /** the links with a curated end on the Okanagan row: the in-force rule ends it on Sept 15, as 0.8.3's did */
  const ended: InSeasonLinks = { ...L, notices: { ...L.notices, "is-2fa458fef3c3": { ...L.notices["is-2fa458fef3c3"]!, to: "2026-09-15" } } };

  it("ended by the in-force rule, then in force again when the rule moved: it enters read, no bell, no mark", () => {
    expect(joinInSeason(feed([A, OK]), ended, "2026-11-20").ended).toEqual(["is-2fa458fef3c3"]);
    const s0 = read(null, feed([A, OK]), ended, "2026-09-20");
    const s1 = read(s0, feed([A, OK]), ended, "2026-11-20");
    expect([s1.items.map((i) => i.id), s1.known]).toEqual([["is-a"], ["is-2fa458fef3c3", "is-a"]]);
    // the history survives the update (the saved text) and the next release reads the rule without the end
    const s2 = read(parseInbox(inboxText(s1)), feed([A, OK]), L, "2026-11-21");
    const ok = s2.items.find((i) => i.id === "is-2fa458fef3c3")!;
    expect([ok.read, ok.ended, inboxUnread(s2), inboxMark(s2, ok.id), inboxWantsOpen(s2)]).toEqual([true, null, 0, null, false]);
    expect(inboxVM(s2, "2026-11-21").groups.map((g) => [g.title, g.rows.map((r) => r.tag)])).toEqual([["Earlier", ["Posted", "Posted"]]]);
    // the notice's own date (the Province's "July 30, 2026"), not the worker's Nov 19
    expect(inboxVM(s2, "2026-11-21").groups[0]!.rows[0]!.label)
      .toBe("Posted: Okanagan River, Region 8, Jul 30. No fishing July 30 to September 15.");
  });

  it("dropped from the history after its 30 days, then in force again: still not news", () => {
    const s0 = read(null, feed([A, OK]), L, "2026-08-01");
    const s1 = read(read(s0, feed([A, OK]), ended, "2026-09-16"), feed([A, OK]), ended, "2026-10-20");
    expect(s1.items.map((i) => i.id)).toEqual(["is-a"]);
    const s2 = read(s1, feed([A, OK]), L, "2026-11-21");
    expect(inboxUnread(s2)).toBe(0);
  });

  it("a notice met BEFORE it began is news the day it begins (known is begun only)", () => {
    const H = row({ id: "is-h", water: "Heber Creek", effective: "November 25, 2026", from: "2026-11-25", firstSeen: "2026-11-19T00:00:00Z" });
    const LH = links({ "is-a": ["gnis:1"], "is-h": ["gnis:3"] });
    const s0 = read(null, feed([A, H]), LH, "2026-11-21");
    expect([s0.items.map((i) => i.id), s0.known]).toEqual([["is-a"], ["is-a"]]);
    const s1 = read(s0, feed([A, H]), LH, "2026-11-25");
    expect([inboxUnread(s1), inboxMark(s1, "is-h")]).toEqual([1, "new"]);
  });

  it("a history an older app wrote (no known: 0.8.4 and before) reads the feed's word once: unchanged rows are not news", () => {
    // what 0.8.3 left on a phone: Kaslo read; the Okanagan row ended before it ever entered
    const old = JSON.stringify({ format: "inseason-inbox/1", items: [{ id: "is-a", region: "4", water: "Kaslo River",
      text: "No fishing", effective: "Nov 1, 2026", from: null, to: null, url: SRC, arrived: "new", at: "2026-11-01",
      waters: ["gnis:1"], read: true, prompted: true, ended: null }] });
    const N = row({ id: "is-n", water: "Duncan River", status: "new", firstSeen: "2026-11-21T08:00:00Z" });
    const s = read(parseInbox(old), feed([A, OK, N]), links({ "is-a": ["gnis:1"], "is-2fa458fef3c3": ["gnis:2"], "is-n": ["gnis:4"] }), "2026-11-21");
    expect(s.items.map((i) => [i.id, i.read])).toEqual([["is-n", false], ["is-2fa458fef3c3", true], ["is-a", true]]);
    expect([inboxUnread(s), s.known]).toEqual([1, ["is-2fa458fef3c3", "is-a", "is-n"]]);
  });

  it("an amendment the feed links is news even for a known id; a row gone from the page is forgotten", () => {
    const s0 = read(null, feed([A, OK]), L, "2026-11-01");
    const Ad = row({ id: "is-a", effective: "Nov 5, 2026", status: "changed", previous: { change: "No fishing", effective: "Nov 1, 2026" },
                     changedAt: "2026-11-12T09:00:00Z" });
    expect(inboxMark(read(s0, feed([Ad, OK]), L, "2026-11-12"), "is-a")).toBe("updated");
    expect(read(s0, feed([OK], [{ id: "is-a", removedAt: "2026-11-12T09:00:00Z" }]), L, "2026-11-12").known).toEqual(["is-2fa458fef3c3"]);
  });
});
