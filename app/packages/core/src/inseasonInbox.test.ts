/**
 * (T700, T694) The in-season inbox: what enters, unread and read, "Updated" only when the feed links it, how long a
 * notice is kept, the device's saved history round trip, and the words — each pinned whole (AGENTS 33).
 */
import { describe, expect, it } from "vitest";
import { EARLIER_DAYS, EARLIER_ROWS, INBOX_WORDS, INBOX_KEEP_DAYS_AFTER_END, INBOX_MAX_ITEMS, inboxDetail, inboxMark, inboxMerge, inboxObserve, inboxEventKind, inboxUnreadIds, inboxPrompted, inboxRead, inboxReadAll,
         inboxTarget, inboxText, inboxUnread, inboxVM, inboxWantsOpen, parseInbox, type InboxState } from "./inseasonInbox";
import { setFailureSink } from "./failure";
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

  it("(T867) a first read keeps a matched notice OUT of the history (it is on its water's page); one not on the map yet is unread (unless T230's notice showed it)", () => {
    const s = read(null, feed([A, B]), L, "2026-11-13");
    expect(s.items.map((i) => [i.id, i.read, i.waters])).toEqual([["is-b", false, null]]);
    expect([inboxUnread(s), inboxWantsOpen(s)]).toEqual([1, true]);
    const seen = read(null, feed([A, B]), L, "2026-11-13", new Set(["is-b"]));
    expect([seen.items.map((i) => [i.id, i.read]), inboxUnread(seen), inboxWantsOpen(seen)]).toEqual([[["is-b", true]], 0, false]);
    // met begun again later (known), it stays out: a notice in effect before install never turns up as a row
    expect(read(s, feed([A, B]), L, "2026-11-14").items.map((i) => i.id)).toEqual(["is-b"]);
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
    const s0 = inboxReadAll(read(read(null, feed([]), L, "2026-11-09"), feed([A]), L, "2026-11-10"));
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
    const s0 = read(read(null, feed([]), L, "2026-11-12"), feed([T, B]), links({ "is-t": ["gnis:1"] }), "2026-11-13");
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
    const old = row({ id: "is-old", water: "Slocan River", firstSeen: "2026-10-20T00:00:00Z" });
    const s0 = inboxReadAll(read(read(null, feed([]), L, "2026-10-19"), feed([old]), links({ "is-old": ["gnis:9"] }), "2026-10-20"));
    // (T983, owner 2026-11-21) the date is the notice's own (Kaslo: effective Nov 1, first seen Nov 12); one that prints
    // none says when it was first seen, as that
    const s = read(s0, feed([old,
                             { ...A, from: "2026-11-01", firstSeen: "2026-11-12T00:00:00Z" },
                             { ...B, firstSeen: "2026-11-13T00:00:00Z" }]), links({ "is-old": ["gnis:9"], "is-a": ["gnis:1"] }), "2026-11-13");
    const vm = inboxVM(s, "2026-11-13");
    expect(vm.groups.map((g) => [g.title, g.rows.map((r) => r.id)])).toEqual([["New", ["is-b", "is-a"]], ["Earlier", ["is-old"]]]);
    expect(vm.groups[0]!.rows.map((r) => r.label)).toEqual([
      "Unread. New: Okanagan River, Region 8, first seen Nov 13. Not on the map yet. Closed to all fishing",
      "Unread. New: Kaslo River, Region 4, Nov 1. No fishing"]);
    expect(vm.groups[0]!.rows.map((r) => r.date)).toEqual(["first seen Nov 13", "Nov 1"]);
    expect(vm.groups[1]!.rows[0]!.label).toBe("Posted: Slocan River, Region 4, first seen Oct 20. No fishing");
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
    // (T867) in effect when this device began counting, on their waters' pages: neither is a history row
    expect([s1.items.map((i) => i.id), s1.known]).toEqual([[], ["is-2fa458fef3c3", "is-a"]]);
    // the history survives the update (the saved text) and the next release reads the rule without the end
    const s2 = read(parseInbox(inboxText(s1)), feed([A, OK]), L, "2026-11-21");
    expect([s2.items, inboxUnread(s2), inboxMark(s2, "is-2fa458fef3c3"), inboxWantsOpen(s2)]).toEqual([[], 0, null, false]);
    expect(inboxVM(s2, "2026-11-21")).toEqual({ groups: [], unread: 0, empty: true });
  });

  it("dropped from the history after its 30 days, then in force again: still not news", () => {
    const s0 = read(null, feed([A, OK]), L, "2026-08-01");
    const s1 = read(read(s0, feed([A, OK]), ended, "2026-09-16"), feed([A, OK]), ended, "2026-10-20");
    expect(s1.items.map((i) => i.id)).toEqual([]);
    const s2 = read(s1, feed([A, OK]), L, "2026-11-21");
    expect(inboxUnread(s2)).toBe(0);
  });

  it("a notice met BEFORE it began is news the day it begins (known is begun only)", () => {
    const H = row({ id: "is-h", water: "Heber Creek", effective: "November 25, 2026", from: "2026-11-25", firstSeen: "2026-11-19T00:00:00Z" });
    const LH = links({ "is-a": ["gnis:1"], "is-h": ["gnis:3"] });
    const s0 = read(null, feed([A, H]), LH, "2026-11-21");
    expect([s0.items.map((i) => i.id), s0.known]).toEqual([[], ["is-a"]]);
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
    expect(s.items.map((i) => [i.id, i.read])).toEqual([["is-n", false], ["is-a", true]]);
    expect([inboxUnread(s), s.known]).toEqual([1, ["is-2fa458fef3c3", "is-a", "is-n"]]);
  });

  it("(review) a legacy history does not take a notice posted since its last read as met, though the page changed since", () => {
    const old = JSON.stringify({ format: "inseason-inbox/1", items: [{ id: "is-a", region: "4", water: "Kaslo River",
      text: "No fishing", effective: "Nov 1, 2026", from: null, to: null, url: SRC, arrived: "new", at: "2026-11-01",
      waters: ["gnis:1"], read: true, prompted: true, ended: null }] });
    // posted Nov 10, after this phone's last read; a later page change turned its status to "unchanged"
    const X = row({ id: "is-x", water: "Duncan River", effective: "Nov 10, 2026", from: "2026-11-10", firstSeen: "2026-11-10T08:00:00Z" });
    const s = read(parseInbox(old), feed([A, X]), links({ "is-a": ["gnis:1"], "is-x": ["gnis:4"] }), "2026-11-21");
    expect([s.items.find((i) => i.id === "is-x")!.read, inboxUnread(s), inboxMark(s, "is-x")]).toEqual([false, 1, "new"]);
  });

  it("an amendment the feed links is news even for a known id; a row gone from the page is forgotten", () => {
    const s0 = read(null, feed([A, OK]), L, "2026-11-01");
    const Ad = row({ id: "is-a", effective: "Nov 5, 2026", status: "changed", previous: { change: "No fishing", effective: "Nov 1, 2026" },
                     changedAt: "2026-11-12T09:00:00Z" });
    expect(inboxMark(read(s0, feed([Ad, OK]), L, "2026-11-12"), "is-a")).toBe("updated");
    expect(read(s0, feed([OK], [{ id: "is-a", removedAt: "2026-11-12T09:00:00Z" }]), L, "2026-11-12").known).toEqual(["is-2fa458fef3c3"]);
  });
});

describe("(T867) since install, read on open, short, with the app's own updates", () => {
  const A = row({ id: "is-a", water: "Kaslo River", from: "2026-11-01" });
  const B = row({ id: "is-b", region: "8", water: "Okanagan River", change: "Closed to all fishing", from: "2026-11-02" });
  const L = links({ "is-a": ["gnis:1"], "is-b": ["gnis:2"] });
  const C = row({ id: "is-c", water: "Duncan River", change: "No fishing", from: "2026-11-14", firstSeen: "2026-11-14T08:00:00Z" });
  const LC = links({ ...Object.fromEntries(Object.keys(L.notices).map((k) => [k, ["gnis:1"]])), "is-c": ["gnis:3"] });
  /** the inbox opened: what was unread is "New" now, read in the history */
  const open = (s: InboxState) => ({ fresh: new Set(inboxUnreadIds(s)), s: inboxReadAll(s) });

  it("opening reads what it shows: New is what was unread at the open, the badge clears, the next open lists it under Earlier", () => {
    const s0 = read(read(null, feed([A]), LC, "2026-11-13"), feed([A, C]), LC, "2026-11-14");
    expect([inboxUnread(s0), s0.items.map((i) => i.id)]).toEqual([1, ["is-c"]]);
    const o = open(s0);
    expect(inboxUnread(o.s)).toBe(0);
    const during = inboxVM(o.s, "2026-11-14", o.fresh);
    expect(during.groups.map((g) => [g.title, g.rows.map((r) => r.label)])).toEqual([["New",
      ["Unread. New: Duncan River, Region 4, Nov 14. No fishing"]]]);
    const next = inboxVM(o.s, "2026-11-15");
    expect(next.groups.map((g) => [g.title, g.rows.map((r) => r.label)])).toEqual([["Earlier",
      ["Posted: Duncan River, Region 4, Nov 14. No fishing"]]]);
    expect(next.unread).toBe(0);
  });

  it("Earlier is the last 30 days and at most 20 rows; New is never cut", () => {
    const many = Array.from({ length: 25 }, (_, n) => row({ id: `is-${String(n).padStart(2, "0")}`, water: `Creek ${n}`, from: `2026-11-${String(n + 1).padStart(2, "0")}`,
      firstSeen: `2026-11-${String(n + 1).padStart(2, "0")}T00:00:00Z` }));
    const s = inboxReadAll(read(read(null, feed([]), links({}), "2026-10-31"), feed(many), links({}), "2026-11-26"));
    const vm = inboxVM(s, "2026-11-26");
    expect(vm.groups.map((g) => [g.title, g.rows.length])).toEqual([["Earlier", EARLIER_ROWS]]);
    expect(vm.groups[0]!.rows[0]!.id).toBe("is-24");
    // an old one (arrived 31 days ago) is not listed
    const old = inboxReadAll(read(read(null, feed([]), links({}), "2026-09-30"), feed([row({ id: "is-o", firstSeen: "2026-10-01T00:00:00Z" })]), links({}), "2026-10-02"));
    expect(EARLIER_DAYS).toBe(30);
    expect(inboxVM(old, "2026-10-31").groups.map((g) => g.rows.length)).toEqual([1]);
    expect(inboxVM(old, "2026-11-01").groups).toEqual([]);
    // unread rows are listed whatever their age
    const unread = read(read(null, feed([]), links({}), "2026-09-30"), feed([row({ id: "is-o", firstSeen: "2026-10-01T00:00:00Z" })]), links({}), "2026-10-02");
    expect(inboxVM(unread, "2026-12-30").groups.map((g) => g.title)).toEqual(["New"]);
  });

  it("an update an older app left is migrated once: matched rows read, one not on the map keeps its state, no backlog turns New", () => {
    const item = (o: object) => ({ region: "4", water: "Kaslo River", text: "No fishing", effective: "Nov 1, 2026", from: null, to: null, url: SRC,
      arrived: "new", at: "2026-11-10", waters: ["gnis:1"], read: false, prompted: true, ended: null, ...o });
    const old = JSON.stringify({ format: "inseason-inbox/1", items: [item({ id: "is-1" }), item({ id: "is-2" }),
      item({ id: "is-3", waters: null, prompted: false })] });
    const s = parseInbox(old)!;
    expect(s.items.map((i) => [i.id, i.read])).toEqual([["is-1", true], ["is-2", true], ["is-3", false]]);
    expect([inboxUnread(s), inboxWantsOpen(s), s.rev]).toEqual([1, true, 2]);
    // saved and read again: the same
    expect(parseInbox(inboxText(s))).toEqual(s);
    // a rev 2 history is not migrated again: an unread matched notice stays unread
    const fresh = read(read(null, feed([]), LC, "2026-11-13"), feed([C]), LC, "2026-11-14");
    expect(inboxUnread(parseInbox(inboxText(fresh)))).toBe(1);
  });

  it("data and app updates are rows: none the first time (since install), one when the release or the version changes, said whole", () => {
    const s0 = inboxObserve(null, { release: { id: "r-1", day: "2026-11-10" }, app: "0.8.5" }, "2026-11-22");
    expect([s0.events, s0.base]).toEqual([undefined, { release: "r-1", app: "0.8.5" }]);
    // nothing moved: the same history back
    expect(inboxObserve(s0, { release: { id: "r-1", day: "2026-11-10" }, app: "0.8.5" }, "2026-11-23")).toBe(s0);
    const s1 = inboxObserve(s0, { release: { id: "r-2", day: "2026-11-20", changed: 12, changedFrom: "r-1" }, app: "0.9.0" }, "2026-11-22");
    expect(inboxUnread(s1)).toBe(2);
    const vm = inboxVM(s1, "2026-11-22");
    expect(vm.groups.map((g) => [g.title, g.rows.map((r) => [r.kind, r.tag, r.water, r.date, r.text, r.label])])).toEqual([["New", [
      ["app", "App update", "Updated to 0.9.0", "Nov 22", "", "Unread. App update: Updated to 0.9.0"],
      ["data", "Data update", "Regulations data updated Nov 20", "", "12 waters changed",
       "Unread. Data update: Regulations data updated Nov 20. 12 waters changed"]]]]);
    expect([inboxEventKind(s1, "app:0.9.0"), inboxEventKind(s1, "data:r-2"), inboxEventKind(s1, "is-a")]).toEqual(["app", "data", null]);
    // a release that does not say how many changed says nothing of it; one water is "1 water changed"
    const s2 = inboxObserve(s1, { release: { id: "r-3", day: null }, app: "0.9.0" }, "2026-11-25");
    expect(inboxVM(s2, "2026-11-25").groups[0]!.rows.find((r) => r.id === "data:r-3")!.label).toBe("Unread. Data update: Regulations data updated Nov 25");
    const s3 = inboxObserve(s2, { release: { id: "r-4", day: "2026-11-26", changed: 1, changedFrom: "r-3" }, app: "0.9.0" }, "2026-11-26");
    expect(inboxVM(s3, "2026-11-26").groups[0]!.rows.find((r) => r.id === "data:r-4")!.text).toBe("1 water changed");
    // opened: read, then Earlier; the saved text reads back
    expect(inboxUnread(inboxReadAll(s3))).toBe(0);
    // a tap reads an update row too (not only a notice)
    expect(inboxUnread(inboxRead(s1, ["app:0.9.0"]))).toBe(1);
    expect(parseInbox(inboxText(s3))).toEqual(s3);
  });
  it("(T1030, owner 2026-11-24 \"Rules only, hide 0\") the row's whole words for a count absent, 0, 1 and many", () => {
    const s0 = inboxObserve(null, { release: { id: "r-1", day: "2026-11-10" }, app: "0.9.1" }, "2026-11-22");
    const row = (changed: number | undefined) => {
      const s = inboxObserve(s0, { release: { id: "r-2", day: "2026-11-20", changed, changedFrom: "r-1" }, app: "0.9.1" }, "2026-11-22");
      const r = inboxVM(s, "2026-11-22").groups.flatMap((g) => g.rows).find((x) => x.id === "data:r-2")!;
      return [s.events!.find((e) => e.id === "data:r-2")!.changed, r.text, r.label];
    };
    expect([row(undefined), row(0), row(1), row(37)]).toEqual([
      [null, "", "Unread. Data update: Regulations data updated Nov 20"],
      // the history keeps the 0 the release stated (a true answer); only the words leave it out
      [0, "", "Unread. Data update: Regulations data updated Nov 20"],
      [1, "1 water changed", "Unread. Data update: Regulations data updated Nov 20. 1 water changed"],
      [37, "37 waters changed", "Unread. Data update: Regulations data updated Nov 20. 37 waters changed"]]);
    expect([INBOX_WORDS.changed(null), INBOX_WORDS.changed(0), INBOX_WORDS.changed(1), INBOX_WORDS.changed(2)])
      .toEqual(["", "", "1 water changed", "2 waters changed"]);
  });
  it("(T1030) the count is said only when it was taken against the release this device held before", () => {
    const s0 = inboxObserve(null, { release: { id: "r-1", day: "2026-11-10" }, app: "0.9.1" }, "2026-11-22");
    const text = (s: ReturnType<typeof inboxObserve>, id: string) =>
      inboxVM(s, "2026-11-30").groups.flatMap((g) => g.rows).find((r) => r.id === id)!.label;
    // counted against r-1, the release held: said whole
    expect(text(inboxObserve(s0, { release: { id: "r-2", day: "2026-11-20", changed: 4, changedFrom: "r-1" }, app: "0.9.1" }, "2026-11-22"), "data:r-2"))
      .toBe("Unread. Data update: Regulations data updated Nov 20. 4 waters changed");
    // counted against a release this device never held (it skipped it, or it never went out): nothing of it
    expect(text(inboxObserve(s0, { release: { id: "r-3", day: "2026-11-20", changed: 4, changedFrom: "r-2" }, app: "0.9.1" }, "2026-11-22"), "data:r-3"))
      .toBe("Unread. Data update: Regulations data updated Nov 20");
    // a count that names no release it was taken against: nothing of it
    expect(text(inboxObserve(s0, { release: { id: "r-4", day: "2026-11-20", changed: 4 }, app: "0.9.1" }, "2026-11-22"), "data:r-4"))
      .toBe("Unread. Data update: Regulations data updated Nov 20");
  });

  it("an upgraded install that had no update record records one now: no row for the update it just took", () => {
    const old = parseInbox(JSON.stringify({ format: "inseason-inbox/1", items: [] }))!;
    const s = inboxObserve(old, { release: { id: "r-9", day: "2026-11-20" }, app: "0.9.0" }, "2026-11-22");
    expect([s.events, s.base]).toEqual([undefined, { release: "r-9", app: "0.9.0" }]);
  });

  it("an update row made before any notice was met does not stop the first read starting quiet", () => {
    const e = inboxObserve(null, { release: { id: "r-1", day: null }, app: "0.9.0" }, "2026-11-22");
    expect(read(e, feed([A, B]), L, "2026-11-22").items).toEqual([]);
  });

  it("(review) a notice met in effect stays quiet when its water's match is lost later: never a New row, never the auto-open", () => {
    const s0 = read(null, feed([A]), links({ "is-a": ["gnis:1"] }), "2026-11-13");
    // the release no longer holds the water / the id was remapped / the link dropped for a poll
    const s1 = read(s0, feed([A]), links({}), "2026-11-14");
    expect([s1.items, inboxUnread(s1), inboxWantsOpen(s1)]).toEqual([[], 0, false]);
    // and when the match comes back: still nothing
    expect(read(s1, feed([A]), links({ "is-a": ["gnis:1"] }), "2026-11-15").items).toEqual([]);
  });

  it("(review) a published day that is not a day is reported and not stored: the saved history stays readable", () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      const s0 = inboxObserve(null, { release: { id: "r-1", day: "2026-11-10" }, app: "0.8.5" }, "2026-11-22");
      const s1 = inboxObserve(s0, { release: { id: "r-2", day: "yesterday" }, app: "0.8.5" }, "2026-11-23");
      expect(s1.events!.map((e) => [e.id, e.at])).toEqual([["data:r-2", "2026-11-23"]]);
      expect(parseInbox(inboxText(s1))).toEqual(s1);
      expect(seen.map(String)).toEqual(['Error: in-season inbox: the data release states a published day that is not a date ("yesterday"); today is used']);
    } finally { undo(); }
  });
});
