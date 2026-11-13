/**
 * (T700, owner 2026-11-13: "a way to see history of notifications in case user wants to see them again? Make it clean in
 * an industry standard way"; T694, owner 2026-11-13: "Yes add it in") THE IN-SEASON INBOX: every provincial in-season
 * notice this device has met, kept per device (nothing is sent anywhere), newest first, unread ahead of read.
 *
 * WHAT ENTERS: each feed row in effect today (`joinInSeason`: matched or not). It arrives NEW, or UPDATED — and UPDATED
 * ONLY when the feed itself links it to an earlier text (the row's `status` "changed" with `previous`, written by the
 * feeds worker's `buildFeed`). The app never guesses a link: a new id of the same water the feed does not link is NEW,
 * and the old one ENDS. An updated notice takes the place of the one it amends (same region + water, the amended text).
 *
 * UNREAD: until the reader opens it (a row tapped, or its "New"/"Updated" mark seen on its water's page) or marks all
 * read. A first read on a device (no history yet) starts quiet: what is matched to a water is read; what is not on the map
 * yet is unread unless the old notice already showed it (`inSeasonSeen`, T230) — as the notice always opened for it.
 *
 * ENDS: when it leaves the page (the feed's `removed`), its `to` passes, or it is no longer in effect. KEPT: until
 * `INBOX_KEEP_DAYS_AFTER_END` days after it ended, and at most `INBOX_MAX_ITEMS` notices (the newest).
 *
 * Every sentence is composed here, whole (AGENTS 33).
 */
import { MONTHS } from "./calendar.generated";
import { isIsoDay } from "./day";
import { INSEASON_WORDS, inSeasonDates, regionWords, type InSeasonChange, type InSeasonFeed } from "./inseason";

/** (T700) a notice stays in the history this many days after it ended (left the page, or its `to` passed) */
export const INBOX_KEEP_DAYS_AFTER_END = 30;
/** (T700) at most this many notices are kept (the newest); older ones drop off */
export const INBOX_MAX_ITEMS = 200;
/** the device's own history file (never a feed: nothing outside this device reads it) */
const INBOX_FORMAT = "inseason-inbox/1";

/** how a notice arrived: a new notice, or the feed's amendment of an earlier one */
export type InboxArrival = "new" | "updated";

export interface InboxItem {
  readonly id: string;
  readonly region: string;
  readonly water: string;
  readonly text: string;
  /** the feed's `effective` as printed (an amendment of the dates alone keeps the id: this tells it apart) */
  readonly effective: string;
  readonly from: string | null;
  readonly to: string | null;
  readonly url: string;
  readonly arrived: InboxArrival;
  /** the day it arrived (the feed's firstSeen / changedAt, else the day this device met it), "YYYY-MM-DD" */
  readonly at: string;
  /** the waters it is matched to on this device's release; null: not on the map yet */
  readonly waters: readonly string[] | null;
  readonly read: boolean;
  /** the inbox has opened by itself for it (a notice not on the map yet opens it once, as T230's notice did) */
  readonly prompted: boolean;
  /** the day it ended; null: in effect */
  readonly ended: string | null;
}

export interface InboxState { readonly format: typeof INBOX_FORMAT; readonly items: readonly InboxItem[] }

const isDay = isIsoDay;
const isStr = (x: unknown): x is string => typeof x === "string";
const dayOrNull = (x: unknown) => x === null || isDay(x);

function isItem(x: unknown): x is InboxItem {
  const i = x as InboxItem | null;
  return !!i && typeof i === "object" && isStr(i.id) && isStr(i.region) && isStr(i.water) && isStr(i.text)
    && isStr(i.effective) && dayOrNull(i.from) && dayOrNull(i.to) && isStr(i.url)
    && (i.arrived === "new" || i.arrived === "updated") && isDay(i.at)
    && (i.waters === null || (Array.isArray(i.waters) && i.waters.every(isStr)))
    && typeof i.read === "boolean" && typeof i.prompted === "boolean" && dayOrNull(i.ended);
}

/**
 * The history as this device wrote it. Null text: none yet (a first read). Anything else it cannot read THROWS, naming
 * it — the caller reports it and starts again; never a quiet empty history.
 */
export function parseInbox(text: string | null): InboxState | null {
  if (text === null) return null;
  const j = JSON.parse(text) as Partial<InboxState> | null;
  if (!j || j.format !== INBOX_FORMAT || !Array.isArray(j.items))
    throw new Error(`in-season inbox: ${(j as { format?: unknown } | null)?.format ?? "no format"}, but this app reads ${INBOX_FORMAT}`);
  const bad = j.items.findIndex((i) => !isItem(i));
  if (bad >= 0) throw new Error(`in-season inbox: item ${bad} cannot be read`);
  return { format: INBOX_FORMAT, items: j.items };
}

export const inboxText = (s: InboxState): string => JSON.stringify(s);

const dayOf = (iso: string | null | undefined): string | null => (iso && isDay(iso.slice(0, 10)) ? iso.slice(0, 10) : null);
function addDays(day: string, n: number): string {
  const d = new Date(`${day}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
const newestFirst = (a: InboxItem, b: InboxItem) => (a.at === b.at ? (a.id < b.id ? -1 : 1) : a.at < b.at ? 1 : -1);

/**
 * (T700, T694) THE HISTORY AFTER A FEED READ: `live` is today's join (`joinInSeason`, with this release's absent waters),
 * `feed` the feed it came from (statuses, links, firstSeen / changedAt, removed). `seen`: the ids T230's notice showed.
 */
export function inboxMerge(state: InboxState | null, feed: InSeasonFeed,
                           live: { readonly matched: readonly InSeasonChange[]; readonly unmatched: readonly InSeasonChange[] },
                           today: string, seen: ReadonlySet<string> = new Set()): InboxState {
  const first = state === null;
  const was = new Map((state?.items ?? []).map((i) => [i.id, i] as const));
  const rows = new Map(feed.rows.map((r) => [r.id, r] as const));
  const removedAt = new Map(feed.removed.map((r) => [r.id, dayOf((r as { removedAt?: string }).removedAt)] as const));
  const current = [...live.matched, ...live.unmatched];
  const liveIds = new Set(current.map((c) => c.id));
  const out = new Map<string, InboxItem>();
  const superseded = new Set<string>();
  for (const c of current) {
    const r = rows.get(c.id);
    // (T694) UPDATED only when the FEED links it: never inferred here
    const linked = r?.status === "changed" && !!r.previous;
    const waters = c.waters ? Object.keys(c.waters).sort() : null;
    const effective = r?.effective ?? "";
    const now = { id: c.id, region: c.region, water: c.water, text: c.text, effective, from: c.from, to: c.to,
                  url: c.source, waters, ended: null };
    const e = was.get(c.id);
    if (e) {
      // the same id amended in place (a date change alone keeps a lone row's id): unread again, as updated
      const amended = linked && (e.text !== c.text || e.effective !== effective);
      out.set(c.id, amended
        ? { ...e, ...now, arrived: "updated", at: dayOf(r?.changedAt) ?? today, read: false, prompted: false }
        : { ...e, ...now });
      continue;
    }
    const quiet = first && (waters !== null || seen.has(c.id));
    out.set(c.id, { ...now, arrived: linked ? "updated" : "new",
                    at: dayOf(linked ? r?.changedAt : r?.firstSeen) ?? today,
                    read: quiet, prompted: quiet || seen.has(c.id) });
    if (linked)
      for (const p of state?.items ?? [])
        if (!liveIds.has(p.id) && p.region === c.region && p.water.toLowerCase() === c.water.toLowerCase()
            && p.text === r!.previous!.change) superseded.add(p.id);
  }
  for (const e of state?.items ?? []) {
    if (out.has(e.id) || superseded.has(e.id)) continue;
    const left = removedAt.get(e.id) ?? null;
    out.set(e.id, e.ended ? e : { ...e, ended: left ?? (e.to && e.to < today ? e.to : today) });
  }
  const items = [...out.values()]
    .filter((i) => !i.ended || addDays(i.ended, INBOX_KEEP_DAYS_AFTER_END) >= today)
    .sort(newestFirst).slice(0, INBOX_MAX_ITEMS);
  return { format: INBOX_FORMAT, items };
}

/** these ids read (a row opened, a mark seen on its page) */
export function inboxRead(s: InboxState, ids: readonly string[]): InboxState {
  const want = new Set(ids);
  return { ...s, items: s.items.map((i) => (want.has(i.id) && !i.read ? { ...i, read: true } : i)) };
}
/** "Mark all as read" */
export const inboxReadAll = (s: InboxState): InboxState => ({ ...s, items: s.items.map((i) => (i.read ? i : { ...i, read: true })) });
/** the inbox opened by itself: every notice it opened for is not opened for again */
export const inboxPrompted = (s: InboxState): InboxState =>
  ({ ...s, items: s.items.map((i) => (i.prompted ? i : { ...i, prompted: true })) });
/** whether the inbox opens by itself: an unread notice in effect, not on the map yet, it has not opened for */
export const inboxWantsOpen = (s: InboxState | null): boolean =>
  !!s && s.items.some((i) => !i.read && !i.prompted && i.waters === null && !i.ended);
export const inboxUnread = (s: InboxState | null): number => (s ? s.items.filter((i) => !i.read).length : 0);
/** (T694) the mark a notice carries on its water's page: unread and in effect only */
export function inboxMark(s: InboxState | null, id: string): InboxArrival | null {
  const i = s?.items.find((x) => x.id === id);
  return i && !i.read && !i.ended ? i.arrived : null;
}
/** where a tap on a row goes: its one water's page (in effect, matched to exactly one water held here), else its text */
export function inboxTarget(s: InboxState | null, id: string): { readonly item: string } | null {
  const i = s?.items.find((x) => x.id === id);
  return i && !i.ended && i.waters?.length === 1 ? { item: i.waters[0]! } : null;
}

export const INBOX_WORDS = {
  title: "Notifications",
  section: "In-season notices",
  unreadGroup: "New",
  readGroup: "Earlier",
  markAll: "Mark all as read",
  ended: "Ended",
  posted: "Posted",
  notOnMap: "Not on the map yet",
  empty: "No notifications",
  emptySub: "In-season changes posted by BC Fish and Wildlife appear here.",
  back: "‹ All notifications",
  backLabel: "Back to all notifications",
  badge: (n: number) => `${n} unread in-season ${n === 1 ? "notice" : "notices"}`,
  notOnMapNote: "We have not placed this change on the map yet. Check it before you fish.",
  endedNote: (day: string) => `This change ended ${day}.`,
  manyWaters: (n: number) => `Shown on the Regulations page of each of the ${n} waters it applies to.`,
  /** the inbox's line when the feed could not be read (the failure itself is reported by the source) */
  failure: "The latest in-season notices could not be read by this version of the app. Shown: the last ones it read.",
} as const;

function dayWords(day: string, today: string): string {
  const [y, m, d] = day.split("-").map(Number);
  return `${MONTHS[m! - 1]} ${d}${day.slice(0, 4) === today.slice(0, 4) ? "" : `, ${y}`}`;
}
/** a read new notice is "Posted": "New" is only ever an unread one */
const tagOf = (i: InboxItem) => (i.ended ? INBOX_WORDS.ended : i.arrived === "new" && i.read ? INBOX_WORDS.posted : INSEASON_WORDS.marks[i.arrived]);

export interface InboxRowVM {
  readonly id: string;
  readonly unread: boolean;
  /** "New" (unread), "Posted" (read), "Updated", "Ended" */
  readonly tag: string;
  /** "Nov 12" (another year: "Nov 12, 2025") */
  readonly date: string;
  readonly water: string;
  /** "Region 4" */
  readonly region: string;
  readonly text: string;
  /** "Not on the map yet", or null */
  readonly note: string | null;
  /** the whole row aloud */
  readonly label: string;
}
export interface InboxVM {
  readonly groups: readonly { readonly title: string; readonly rows: readonly InboxRowVM[] }[];
  readonly unread: number;
  /** nothing in the history */
  readonly empty: boolean;
}

export function inboxVM(s: InboxState | null, today: string): InboxVM {
  const rows = (s?.items ?? []).map((i): InboxRowVM => {
    const tag = tagOf(i), date = dayWords(i.ended ?? i.at, today), region = regionWords(i.region);
    const note = i.waters === null && !i.ended ? INBOX_WORDS.notOnMap : null;
    return { id: i.id, unread: !i.read, tag, date, water: i.water, region, text: i.text, note,
             label: `${i.read ? "" : "Unread. "}${tag}: ${i.water}, ${region}, ${date}${note ? `. ${note}` : ""}. ${i.text}` };
  });
  const unread = rows.filter((r) => r.unread), read = rows.filter((r) => !r.unread);
  return { groups: [...(unread.length ? [{ title: INBOX_WORDS.unreadGroup, rows: unread }] : []),
                    ...(read.length ? [{ title: INBOX_WORDS.readGroup, rows: read }] : [])],
           unread: unread.length, empty: rows.length === 0 };
}

export interface InboxDetailVM {
  readonly id: string;
  readonly tag: string;
  /** "Kaslo River · Region 4" */
  readonly head: string;
  /** "Oct 15–Nov 30", or null */
  readonly dates: string | null;
  readonly text: string;
  readonly url: string;
  /** not on the map / ended / on many waters, or null */
  readonly note: string | null;
}

/** A notice opened that has no one page to go to: its text, in the inbox. */
export function inboxDetail(s: InboxState | null, id: string, today: string): InboxDetailVM | null {
  const i = s?.items.find((x) => x.id === id);
  if (!i) return null;
  const note = i.ended ? INBOX_WORDS.endedNote(dayWords(i.ended, today))
    : i.waters === null ? INBOX_WORDS.notOnMapNote
    : i.waters.length > 1 ? INBOX_WORDS.manyWaters(i.waters.length) : null;
  return { id, tag: tagOf(i), head: `${i.water} · ${regionWords(i.region)}`, dates: inSeasonDates(i.from, i.to),
           text: i.text, url: i.url, note };
}
