// T627 (owner 2026-11-10/11): THE DAILY DIGEST, EASY TO READ. It opens with a few plain lines — one of counts (problem
// reports, new error kinds, known kinds that came back), the feeds line, and what else is in it — then each
// problem report as a short block (what and where, the reporter's words trimmed, when in B.C. time and on which build,
// the id last), each new error kind as one entry (message, screen, build, count), repeats as a count, and after those
// every other kind exactly as its own email says it (src/email.ts `sectionFor`: DFO, in-season, feed health, HYDAT, run
// timing, roster, model scores, gauge cards).
//
// NOTHING IS DROPPED UNSAID. Words cut short say how many more there are; a block names what else the full report
// holds and where to read it (Review app → Reports; GET /crashes); past MAX_LINES_PER_KIND (T609) a line counts the
// rest; an event of a kind this file does not know still gets its section.
import { bcStampWords } from "../../../packages/core/src/day";
import { JOB_NAME, MAX_LINES_PER_KIND, render, sectionFor, type Item, type Rendered, type RenderCtx, type Section } from "./email";
import type { EventKind, FeedEvent } from "./events";

/** the order of a digest: what people said first, then what broke, then the feeds, then the rest as before */
export const DIGEST_ORDER: readonly EventKind[] = ["report", "crash", "health", "dfo", "inseason", "hydat", "runtiming", "roster", "skill", "card"];
/** a reporter's words in a block: about three lines of the plain text */
export const WORDS_MAX = 240;
/** an error's message in its entry */
export const MESSAGE_MAX = 140;
const REPORTS_WHERE = "Review app → Reports (Sync)";
const CRASHES_WHERE = "GET /crashes (Bearer RUN_TOKEN)";

const plural = (n: number, one: string, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;
const DEFAULT_CTX: RenderCtx = { env: "staging", statusUrl: null };

/** `s` on one line, cut at a word within `max` characters; `more` = the words left out (0: whole) */
export function trimWords(s: string, max: number): { text: string; more: number } {
  const words = s.replace(/\s+/g, " ").trim().split(" ").filter(Boolean);
  let text = "";
  let used = 0;
  for (const w of words) {
    if (used && text.length + 1 + w.length > max) break;
    text += (used ? " " : "") + w;
    used++;
  }
  // one unbroken run longer than `max`: cut it, and count it as left out in part
  if (text.length > max) return { text: text.slice(0, max), more: words.length - used + 1 };
  return { text, more: words.length - used };
}
/** an instant in B.C. words ("Nov 9, 11:04 a.m. PT"); a stamp that does not parse is shown as it is */
const when = (iso: unknown): string => (typeof iso === "string" && !Number.isNaN(Date.parse(iso)) ? bcStampWords(Date.parse(iso)) : String(iso ?? "?"));

function reportItem(e: FeedEvent): Item {
  const d = e.data as Record<string, any> | undefined; // eslint-disable-line @typescript-eslint/no-explicit-any
  const id = e.key.replace(/^report:/, "");
  if (!d?.id) {
    // queued before T627 (no fields): its title's head, its whole text trimmed the same way
    const t = trimWords(e.text, WORDS_MAX);
    return { name: e.title.split(" — ")[0], text: t.more ? `${t.text}…` : t.text, note: `told ${when(e.at)}`,
             tail: `${t.more ? `Not shown here: ${plural(t.more, "more word")}. Full report: ${REPORTS_WHERE}. ` : ""}id ${id}` };
  }
  const t = trimWords(String(d.words ?? ""), WORDS_MAX);
  const left = [t.more ? plural(t.more, "more word") : null, ...(d.also ?? [])].filter(Boolean);
  return {
    name: `${d.reason}: ${d.where}`,
    text: `“${t.text}${t.more ? "…" : ""}”`,
    note: [when(d.received), d.day ? `looking at ${d.day}` : null, d.mode ? `map ${d.mode}` : null, `app ${d.app ?? "?"}${d.env ? ` (${d.env})` : ""}`, d.platform ?? "?", `release ${d.release ?? "?"}`]
      .filter(Boolean).join(" · "),
    tail: `${left.length ? `Not shown here: ${left.join(", ")}. Full report: ${REPORTS_WHERE}. ` : ""}Reply: ${d.reply} · id ${d.id}`,
  };
}

export function reportSection(events: FeedEvent[]): Section {
  const shown = events.slice(0, MAX_LINES_PER_KIND).map(reportItem);
  if (events.length > MAX_LINES_PER_KIND) shown.push({ text: `…and ${events.length - MAX_LINES_PER_KIND} more: ${REPORTS_WHERE}.` });
  return {
    heading: `Problem reports from the app: ${events.length}`,
    subject: `${plural(events.length, "problem report")} from the app`,
    blocks: [{ items: shown }],
    steps: [{ text: `${REPORTS_WHERE}: each report in full, then fix or dismiss each.` }],
    why: "Someone used Report a problem in the app.",
  };
}

const isNewKind = (e: FeedEvent) => /^crash:/.test(e.key);
const isRepeats = (e: FeedEvent) => typeof e.data?.repeats === "number";

function crashItem(e: FeedEvent): Item {
  const d = e.data as Record<string, any> | undefined; // eslint-disable-line @typescript-eslint/no-explicit-any
  const hash = e.key.replace(/^crash:/, "");
  // queued before T627 (no fields): the title is already "Name: message (platform, screen)"
  if (!d?.hash) return { name: e.title, note: `told ${when(e.at)} · stack and counts: ${CRASHES_WHERE} · id ${hash}` };
  const m = trimWords(`${d.name}: ${d.message}`, MESSAGE_MAX);
  return {
    name: m.more ? `${m.text}…` : m.text,
    // the one line: where, which build, how often
    note: [
      `${d.route} screen${d.pane ? ` (${d.pane})` : ""}`,
      `app ${(d.versions ?? []).join(", ") || d.app}`,
      d.platform,
      `${plural(d.count, "report")} so far${d.installs ? ` from ${plural(d.installs, "install")}` : ""}`,
      d.native ? "the system ended the app" : null,
      d.fatal ? "fatal" : null,
    ].filter(Boolean).join(" · "),
    tail: [
      m.more ? `Message cut: ${plural(m.more, "more word")}` : null,
      `first ${when(d.first)}`, d.env, `release ${d.release}`, `id ${d.hash}`,
    ].filter(Boolean).join(" · "),
  };
}

export function crashSection(events: FeedEvent[]): Section {
  const fresh = events.filter(isNewKind);
  const repeats = events.filter(isRepeats);
  const other = events.filter((e) => !isNewKind(e) && !isRepeats(e));
  const again = repeats.reduce((n, e) => n + (e.data.repeats as number), 0);
  const shown = fresh.slice(0, MAX_LINES_PER_KIND).map(crashItem);
  if (fresh.length > MAX_LINES_PER_KIND) shown.push({ text: `…and ${fresh.length - MAX_LINES_PER_KIND} more: ${CRASHES_WHERE}.` });
  const counts = [fresh.length ? plural(fresh.length, "new kind") : "", again ? `${plural(again, "known kind")} seen again` : ""].filter(Boolean).join(", ");
  return {
    heading: fresh.length ? `New app errors: ${fresh.length}${again ? `, and ${plural(again, "known kind")} seen again` : ""}`
      : `App errors: ${counts || plural(other.length, "note")}`,
    subject: fresh.length ? plural(fresh.length, "new app error") : (repeats[0] ?? other[0])!.title,
    blocks: [
      ...(shown.length ? [{ title: "New", items: shown }] : []),
      // a count, never a list: a known kind is not told again (its line was in the digest of the day it was new)
      ...(repeats.length ? [{ title: "Seen again", items: repeats.map((e) => ({ text: e.text })) }] : []),
      ...(other.length ? [{ title: "Not defects", items: other.map((e) => ({ text: e.text })) }] : []),
    ],
    steps: [{ text: `Each error's stack and every report of it: ${CRASHES_WHERE}. Fix or ignore each.` }],
    why: "The app reported an error kind it had not reported before (Send crash reports, More → Data).",
  };
}

/** what one health event says of its feed, short: "Gauge feed failing", "DFO salmon feed recovered" */
function healthWords(e: FeedEvent): string {
  const job = e.data?.job as string | undefined;
  if (!job) return e.title.split(" — ")[0]!;
  return `${JOB_NAME[job] ?? job} ${e.fine ? "recovered" : String(e.data.to ?? "changed")}`;
}

/** THE OPENING LINES. Each says only what this digest holds: a kind with nothing in it is said as "none in this
 *  digest", never as a claim about the app or the feeds now (the status page has the current state). */
export function digestSummary(events: FeedEvent[], rest: Section[], ctx: RenderCtx): string[] {
  const of = (k: EventKind) => events.filter((e) => e.kind === k);
  const reports = of("report").length;
  const crash = of("crash");
  const fresh = crash.filter(isNewKind).length;
  const again = crash.filter(isRepeats).reduce((n, e) => n + (e.data.repeats as number), 0);
  const health = of("health");
  const out = [
    // ONE LINE OF COUNTS (owner 2026-11-11); a repeat count is said only when the day's count was taken
    `In this digest: ${[reports ? plural(reports, "problem report") : "no problem reports",
                        fresh ? plural(fresh, "new error kind") : "no new error kinds",
                        again ? `${plural(again, "known error kind")} seen again` : ""].filter(Boolean).join(" · ")}`,
    `Feeds: ${health.length ? health.map(healthWords).join("; ") : `no change of state in this digest${ctx.statusUrl ? " (the status page has the state now)" : ""}`}`,
  ];
  if (rest.length) out.push(`Also: ${rest.map((s) => s.subject).join("; ")}`);
  return out;
}

/** The digest of `events`: every event is in it, in `DIGEST_ORDER` (a kind that list does not name comes last). */
export function renderDigest(events: FeedEvent[], ctx: RenderCtx = DEFAULT_CTX): Rendered {
  const present = [...new Set(events.map((e) => e.kind))];
  const kinds = [...DIGEST_ORDER.filter((k) => present.includes(k)), ...present.filter((k) => !DIGEST_ORDER.includes(k)).sort()];
  const sections = kinds.map((k) => {
    const es = events.filter((e) => e.kind === k);
    return k === "report" ? reportSection(es) : k === "crash" ? crashSection(es) : sectionFor(k, es, ctx);
  });
  const rest = sections.filter((_, i) => !["report", "crash", "health"].includes(kinds[i]!));
  return render(sections, ctx, true, digestSummary(events, rest, ctx));
}
