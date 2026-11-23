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
 * NEWS IS A NOTICE THAT BEGAN, NOT ONE THE APP STARTED COUNTING (T983, 2026-11-21: 0.8.4 stopped inferring an end from a
 * printed window (T548), and seven summer closures this device had met as ended since September came back as "New · Nov
 * 19"). The history keeps `known`: every feed row this device has met begun (in effect, or ended). A row that enters the
 * history and is `known` (the in-force rule, a curated date or the history's retention moved, not the notice) enters as a
 * first read does: quiet when matched. A history written before `known` (0.8.4 and earlier) reads the feed's own word
 * once: a row the feed does not call "new" or "changed" was on the page before, so it is known.
 *
 * ENDS: when it leaves the page (the feed's `removed`), its `to` passes, or it is no longer in effect. KEPT: until
 * `INBOX_KEEP_DAYS_AFTER_END` days after it ended, and at most `INBOX_MAX_ITEMS` notices (the newest).
 *
 * (T867, owner 2026-11-22 "Build all four") THE INBOX IS WHAT HAPPENED SINCE INSTALL, READ WHEN SEEN, SHORT, AND ALSO SAYS
 * WHAT THE APP DID: (1) a notice in effect at the first read stays out of the history (it is on its water's page) unless
 * it is not on the map — the inbox is its only home; (2) OPENING the inbox reads what it shows: "New" is what was unread
 * at the open (`inboxVM`'s `fresh`), the badge clears, the next open lists them under "Earlier"; (3) "Earlier" is the last
 * `EARLIER_DAYS` days, at most `EARLIER_ROWS` rows; (4) a data release put in use and an app update are rows of their own
 * (`events`, seen by `inboxObserve`). A history an older app wrote is migrated once (`rev`): what it showed matched stays
 * read, so an update never makes the whole backlog "New".
 *
 * Every sentence is composed here, whole (AGENTS 33).
 */
import { MONTHS } from "./calendar.generated";
import { isIsoDay } from "./day";
import { reportFailure } from "./failure";
import { INSEASON_WORDS, inSeasonDates, regionWords, type InSeasonChange, type InSeasonFeed } from "./inseason";

/** (T700) a notice stays in the history this many days after it ended (left the page, or its `to` passed) */
export const INBOX_KEEP_DAYS_AFTER_END = 30;
/** (T700) at most this many notices are kept (the newest); older ones drop off */
export const INBOX_MAX_ITEMS = 200;
/** the device's own history file (never a feed: nothing outside this device reads it) */
const INBOX_FORMAT = "inseason-inbox/1";
/** (T867) "Earlier" lists the last this many days, at most this many rows */
export const EARLIER_DAYS = 30;
export const EARLIER_ROWS = 20;
/** (T867) the history's revision: 2 reads on open, keeps no first-read backlog, and holds data/app events */
const INBOX_REV = 2;

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

/** (T867) a data release put in use, or an app update: a row of the inbox */
export interface InboxEvent {
  /** "data:<release>" or "app:<version>" */
  readonly id: string;
  readonly kind: "data" | "app";
  /** the day it happened: a release's published day (else the day this device met it), "YYYY-MM-DD" */
  readonly at: string;
  /** an app update: the version it went to; a data release: null */
  readonly version: string | null;
  /** a data release: how many waters changed, when the release says (null: it does not, and nothing is said) */
  readonly changed: number | null;
  readonly read: boolean;
}

export interface InboxState {
  readonly format: typeof INBOX_FORMAT;
  /** (T867) absent: written before the inbox read on open (`parseInbox` migrates it once) */
  readonly rev?: typeof INBOX_REV;
  readonly items: readonly InboxItem[];
  /** (T867) data and app updates, newest first */
  readonly events?: readonly InboxEvent[];
  /** (T867) the release id and app version this device last saw: a change from them is an event. Absent: none seen yet. */
  readonly base?: { readonly release: string | null; readonly app: string | null };
  /** (T983) the feed rows (ids, sorted) this device has met begun — in effect or ended — while they are on the page.
   *  Absent: a history an app before 0.8.5 wrote (`inboxMerge` reads the feed's statuses once in its place). */
  readonly known?: readonly string[];
}

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
function isEvent(x: unknown): x is InboxEvent {
  const e = x as InboxEvent | null;
  return !!e && typeof e === "object" && isStr(e.id) && (e.kind === "data" || e.kind === "app") && isDay(e.at)
    && (e.version === null || isStr(e.version)) && (e.changed === null || Number.isInteger(e.changed)) && typeof e.read === "boolean";
}

export function parseInbox(text: string | null): InboxState | null {
  if (text === null) return null;
  const j = JSON.parse(text) as Partial<InboxState> | null;
  if (!j || j.format !== INBOX_FORMAT || !Array.isArray(j.items))
    throw new Error(`in-season inbox: ${(j as { format?: unknown } | null)?.format ?? "no format"}, but this app reads ${INBOX_FORMAT}`);
  const bad = j.items.findIndex((i) => !isItem(i));
  if (bad >= 0) throw new Error(`in-season inbox: item ${bad} cannot be read`);
  if (j.known !== undefined && (!Array.isArray(j.known) || !j.known.every(isStr)))
    throw new Error("in-season inbox: its known notices cannot be read");
  if (j.rev !== undefined && j.rev !== INBOX_REV) throw new Error(`in-season inbox: revision ${String(j.rev)}, but this app reads ${INBOX_REV}`);
  const events = j.events ?? [];
  if (!Array.isArray(events) || events.some((e) => !isEvent(e))) throw new Error("in-season inbox: its update rows cannot be read");
  const b = j.base;
  if (b !== undefined && (!b || typeof b !== "object" || !(b.release === null || isStr(b.release)) || !(b.app === null || isStr(b.app))))
    throw new Error("in-season inbox: its last seen versions cannot be read");
  // (T867) a history an older app wrote: what it showed matched is read (the backlog is not New again); one not on the map
  // stays as it was (the inbox is its only home). Migrated once: `rev` says so.
  const items = j.rev === undefined ? j.items.map((i) => (i.waters !== null && !i.read ? { ...i, read: true } : i)) : j.items;
  return { format: INBOX_FORMAT, rev: INBOX_REV, items, ...(j.events ? { events: j.events } : {}), ...(b ? { base: b } : {}),
           ...(j.known ? { known: j.known } : {}) };
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
 * (T983) `live.ended`: the rows on the page whose end has passed — met, so never news if they come back into force.
 */
export function inboxMerge(state: InboxState | null, feed: InSeasonFeed,
                           live: { readonly matched: readonly InSeasonChange[]; readonly unmatched: readonly InSeasonChange[];
                                   readonly ended: readonly string[] },
                           today: string, seen: ReadonlySet<string> = new Set()): InboxState {
  // (T867) a first read: no history, or one only an update row made (no notice met yet)
  const first = state === null || (state.items.length === 0 && !state.known);
  // (T983) met begun before this read: the history's own list, or — for a history written before it — the feed's word
  // that the row was on the page before ("unchanged": neither "new" nor "changed")
  const knew = state?.known ? new Set(state.known) : null;
  const was = new Map((state?.items ?? []).map((i) => [i.id, i] as const));
  const rows = new Map(feed.rows.map((r) => [r.id, r] as const));
  // (T983 review) a legacy history knows an "unchanged" row only if it began, or the feed first saw it, by the history's
  // newest day: the feed's status is against the page before its LAST change, so a notice posted since this phone last
  // read would pass as met. (The begun date too: the feed's firstSeen can be later than the row's months on the page, the
  // Okanagan row's Nov 19.) A history with no items has no day to go by and keeps the feed's word.
  const lastRead = (state?.items ?? []).reduce<string | null>((m, i) => (i.at && (m === null || i.at > m) ? i.at : m), null);
  const known = (id: string) => {
    if (knew) return knew.has(id);
    const r = rows.get(id);
    if (state === null || r?.status !== "unchanged") return false;
    if (lastRead === null) return true;
    const by = (d: string | null | undefined) => { const x = dayOf(d); return x !== null && x <= lastRead; };
    return by(r.from) || by(r.firstSeen);
  };
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
    // a first read, or (T983) a notice met begun before that enters again (not an amendment the feed links): quiet
    // (T867) in effect before this device began counting and on its water's page (a first read), or already met begun:
    // not a history row. A met notice stays out whether or not it is matched now (a water's release lost, an id remapped,
    // a link dropped for a poll: not news).
    const metBefore = !first && !linked && known(c.id);
    if (metBefore || (first && waters !== null)) continue;
    const quiet = first && seen.has(c.id);
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
  // (T983) what this device has met begun, while it is on the page (a row gone from it is forgotten)
  const gone = new Set(feed.removed.map((r) => r.id));
  const met = [...new Set([...(state?.known ?? []), ...liveIds, ...live.ended])].filter((id) => rows.has(id) && !gone.has(id)).sort();
  return { ...(state ?? { format: INBOX_FORMAT, rev: INBOX_REV }), format: INBOX_FORMAT, rev: INBOX_REV, items, known: met };
}

/** these ids read (a row opened, a mark seen on its page) */
export function inboxRead(s: InboxState, ids: readonly string[]): InboxState {
  const want = new Set(ids);
  return { ...s, items: s.items.map((i) => (want.has(i.id) && !i.read ? { ...i, read: true } : i)),
           ...(s.events ? { events: s.events.map((e) => (want.has(e.id) && !e.read ? { ...e, read: true } : e)) } : {}) };
}
/** (T867) THE INBOX OPENED: everything it shows is read (the badge clears); `inboxUnreadIds` says first what was unread */
export const inboxReadAll = (s: InboxState): InboxState => ({
  ...s, items: s.items.map((i) => (i.read ? i : { ...i, read: true })),
  ...(s.events ? { events: s.events.map((e) => (e.read ? e : { ...e, read: true })) } : {}),
});
/** (T867) the ids unread right now: the open's "New" */
export const inboxUnreadIds = (s: InboxState | null): readonly string[] =>
  s ? [...s.items.filter((i) => !i.read).map((i) => i.id), ...(s.events ?? []).filter((e) => !e.read).map((e) => e.id)] : [];

/**
 * (T867) WHAT THE APP DID: a data release put in use and an app update are rows. `seen`: the release in use (its id and the
 * day it was published, null when the manifest states none) and the app's version. The first time a device sees them
 * there is nothing to compare with: they are recorded, no row (since install only). A release id or version that differs
 * from the last seen is a row. `changed`: how many waters the release changed, when it says (null: nothing is said).
 */
export function inboxObserve(state: InboxState | null,
                             seen: { readonly release: { readonly id: string; readonly day: string | null; readonly changed?: number | null } | null;
                                     readonly app: string | null },
                             today: string): InboxState {
  const s: InboxState = state ?? { format: INBOX_FORMAT, rev: INBOX_REV, items: [] };
  // (T867 review) a published day that is not a day would make the saved history unreadable: said, and today stands in
  let relDay = seen.release?.day ?? null;
  if (relDay !== null && !isDay(relDay)) {
    reportFailure(new Error(`in-season inbox: the data release states a published day that is not a date (${JSON.stringify(relDay)}); today is used`));
    relDay = null;
  }
  const b = s.base;
  const next = { release: seen.release?.id ?? b?.release ?? null, app: seen.app ?? b?.app ?? null };
  if (!b) return { ...s, base: next };
  const added: InboxEvent[] = [];
  if (seen.release && b.release !== null && seen.release.id !== b.release)
    added.push({ id: `data:${seen.release.id}`, kind: "data", at: relDay ?? today, version: null,
                 changed: seen.release.changed ?? null, read: false });
  if (seen.app && b.app !== null && seen.app !== b.app)
    added.push({ id: `app:${seen.app}`, kind: "app", at: today, version: seen.app, changed: null, read: false });
  if (!added.length && b.release === next.release && b.app === next.app) return s;
  const have = new Set((s.events ?? []).map((e) => e.id));
  const events = [...added.filter((e) => !have.has(e.id)), ...(s.events ?? [])]
    .filter((e) => !e.read || addDays(e.at, INBOX_KEEP_DAYS_AFTER_END) >= today).slice(0, EARLIER_ROWS);
  return { ...s, events, base: next };
}
/** (T867) where a tap on an update row goes: the release's, the app's; null: a notice */
export const inboxEventKind = (s: InboxState | null, id: string): "data" | "app" | null => s?.events?.find((e) => e.id === id)?.kind ?? null;
/** the inbox opened by itself: every notice it opened for is not opened for again */
export const inboxPrompted = (s: InboxState): InboxState =>
  ({ ...s, items: s.items.map((i) => (i.prompted ? i : { ...i, prompted: true })) });
/** whether the inbox opens by itself: an unread notice in effect, not on the map yet, it has not opened for */
export const inboxWantsOpen = (s: InboxState | null): boolean =>
  !!s && s.items.some((i) => !i.read && !i.prompted && i.waters === null && !i.ended);
export const inboxUnread = (s: InboxState | null): number => inboxUnreadIds(s).length;
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
  /** (T867) the update rows' marks and sentences */
  dataTag: "Data update",
  appTag: "App update",
  dataTitle: (day: string) => `Regulations data updated ${day}`,
  changed: (n: number) => `${n} ${n === 1 ? "water" : "waters"} changed`,
  appTitle: (version: string) => `Updated to ${version}`,
  ended: "Ended",
  posted: "Posted",
  /** (T983) a notice that prints no date of its own: when it was first seen */
  firstSeen: (day: string) => `first seen ${day}`,
  notOnMap: "Not on the map yet",
  empty: "No notifications",
  emptySub: "In-season changes posted by BC Fish and Wildlife appear here.",
  back: "‹ All notifications",
  backLabel: "Back to all notifications",
  badge: (n: number) => `${n} unread in-season ${n === 1 ? "notice" : "notices"}`,
  notOnMapNote: "I have not placed this change on the map yet. Check it before you fish.",
  endedNote: (day: string) => `This change ended ${day}.`,
  manyWaters: (n: number) => `Shown on the Regulations page of each of the ${n} waters it applies to.`,
  /** the inbox's line when the feed could not be read (the failure itself is reported by the source) */
  failure: "The latest in-season notices could not be read by this version of the app. Shown: the last ones it read.",
  /** (T548 L5) the feed was refused because more than half its notices could not be read: how many, of how many */
  failureRows: (n: number, of: number) =>
    `The latest in-season notices could not be read by this version of the app (${n} of ${of} could not be read). Shown: the last ones it read.`,
  /** (T548 item 4) some rows of the feed could not be read: they were left out, the others are shown (also reported) */
  unreadRows: (n: number) => n === 1
    ? "1 in-season notice could not be read by this version of the app. It is not shown; the others are."
    : `${n} in-season notices could not be read by this version of the app. They are not shown; the others are.`,
} as const;

function dayWords(day: string, today: string): string {
  const [y, m, d] = day.split("-").map(Number);
  return `${MONTHS[m! - 1]} ${d}${day.slice(0, 4) === today.slice(0, 4) ? "" : `, ${y}`}`;
}
/**
 * (T983, owner 2026-11-21) A row's date: an ended notice, the day it ended; else THE NOTICE'S OWN DATE (the Province's
 * effective date, `from`); a notice that prints none, the day it was first seen, said as that ("first seen Nov 19") —
 * never the feeds worker's first-seen day passed off as the notice's.
 */
function rowDate(i: InboxItem, today: string): string {
  if (i.ended) return dayWords(i.ended, today);
  return i.from ? dayWords(i.from, today) : INBOX_WORDS.firstSeen(dayWords(i.at, today));
}
/** a read new notice is "Posted": "New" is only ever an unread one */
const tagOf = (i: InboxItem, unread: boolean) => (i.ended ? INBOX_WORDS.ended : i.arrived === "new" && !unread ? INBOX_WORDS.posted : INSEASON_WORDS.marks[i.arrived]);

export interface InboxRowVM {
  readonly id: string;
  /** (T867) a notice, or the app's own data / app update row */
  readonly kind: "notice" | "data" | "app";
  readonly unread: boolean;
  /** "New" (unread), "Posted" (read), "Updated", "Ended" */
  readonly tag: string;
  /** the notice's own date "Jul 30" (another year: "Jul 30, 2025"); ended: the day it ended; none printed: "first seen
   *  Nov 19" (`rowDate`) */
  readonly date: string;
  readonly water: string;
  /** "Region 4" (an update row: "") */
  readonly region: string;
  /** the change's text (a data update: "12 waters changed", or "" when the release does not say) */
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

/**
 * (T867) The list: "New" is every row unread now or in `fresh` (what was unread when the inbox opened: opening read it, the
 * next open lists it under "Earlier"); "Earlier" is the read rows of the last `EARLIER_DAYS` days, `EARLIER_ROWS` at most.
 */
export function inboxVM(s: InboxState | null, today: string, fresh: ReadonlySet<string> = new Set()): InboxVM {
  const mk = (r: InboxRowVM, at: string) => ({ r, at });
  const notices = (s?.items ?? []).map((i) => {
    const unread = !i.read || fresh.has(i.id);
    const tag = tagOf(i, unread), date = rowDate(i, today), region = regionWords(i.region);
    const note = i.waters === null && !i.ended ? INBOX_WORDS.notOnMap : null;
    return mk({ id: i.id, kind: "notice", unread, tag, date, water: i.water, region, text: i.text, note,
                label: `${unread ? "Unread. " : ""}${tag}: ${i.water}, ${region}, ${date}${note ? `. ${note}` : ""}. ${i.text}` }, i.at);
  });
  const events = (s?.events ?? []).map((e) => {
    const unread = !e.read || fresh.has(e.id);
    const data = e.kind === "data";
    const tag = data ? INBOX_WORDS.dataTag : INBOX_WORDS.appTag;
    const water = data ? INBOX_WORDS.dataTitle(dayWords(e.at, today)) : INBOX_WORDS.appTitle(e.version ?? "");
    const text = data && e.changed !== null ? INBOX_WORDS.changed(e.changed) : "";
    const date = data ? "" : dayWords(e.at, today);
    return mk({ id: e.id, kind: e.kind, unread, tag, date, water, region: "", text, note: null,
                label: `${unread ? "Unread. " : ""}${tag}: ${water}${text ? `. ${text}` : ""}` }, e.at);
  });
  const all = [...notices, ...events].sort((a, b) => (a.at === b.at ? (a.r.id < b.r.id ? -1 : 1) : a.at < b.at ? 1 : -1));
  const unread = all.filter((x) => x.r.unread).map((x) => x.r);
  const since = addDays(today, -EARLIER_DAYS);
  const read = all.filter((x) => !x.r.unread && x.at >= since).slice(0, EARLIER_ROWS).map((x) => x.r);
  return { groups: [...(unread.length ? [{ title: INBOX_WORDS.unreadGroup, rows: unread }] : []),
                    ...(read.length ? [{ title: INBOX_WORDS.readGroup, rows: read }] : [])],
           unread: unread.length, empty: !unread.length && !read.length };
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
  return { id, tag: tagOf(i, !i.read), head: `${i.water} · ${regionWords(i.region)}`, dates: inSeasonDates(i.from, i.to),
           text: i.text, url: i.url, note };
}
