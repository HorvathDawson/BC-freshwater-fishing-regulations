// T225 follow-up (owner 2026-10-28): EVERYTHING THE OWNER SHOULD HEAR ABOUT, ONE OUTBOX. Each job turns what it found
// into `FeedEvent`s (a stable `key` each); this module dedupes them (a key is told once), groups them into messages and
// delivers them:
//   email    Cloudflare Email Workers (`send_email` binding EMAIL) from NOTIFY_EMAIL_FROM to NOTIFY_EMAIL_TO (the owner's
//            verified Email Routing address) — when both are set
//   webhook  FEED_NOTIFY_URL (ntfy / Discord / Slack) — when set, and as the fallback when the email send fails
//   log      neither configured: logged, and still recorded as told (nothing replays when a channel is added later)
// POLICY (owner 2026-10-28): DFO places to review and in-season changes are URGENT — emailed at once (T366, owner
// 2026-11-03: EXCEPT DFO's rule-row changes on known places, `urgent: false`, which ride the digest). With
// NOTIFY_DIGEST=daily everything else (HYDAT, roster drift, feed health) waits for ONE digest a day (first run at/after
// NOTIFY_DIGEST_HOUR_UTC, default 15 = 08:00 PDT), sent ONLY when the day holds at least one issue or action — a
// "recovered" health event alone (`fine`) never makes a digest; current state is /status's job, not email's.
// Owner 2026-10-28 (Phase A): a WHOLE feed failing or going stale is the exception — `urgent`, emailed at once; a
// degraded feed (some stations / a region / a source failing) and a source down 3 runs running wait for the digest.
// Owner 2026-10-29: GAUGE failures are the exception to the exception — a gauge feed failing, stale or degraded NEVER
// emails alone; it rides only in the daily digest (DFO + in-season stay urgent).
// Dry run: nothing sent, nothing recorded — the messages are returned (POST /run/<job>?dryNotify=1, /notify/preview).
//
// State (private): notify/state.json — {told: {key: at}, pending: [...], lastDigest}. A key is forgotten after a year.
import { body as webhookBody } from "./notify";
import { isVersioned, readStored, writeMerged, type Fetch, type Store } from "./io";
import { apkSection, render, sectionFor, type ApkInfo, type RenderCtx } from "./email";
import { renderDigest } from "./digest";

export type EventKind = "hydat" | "roster" | "inseason" | "dfo" | "health" | "report" | "crash" | "skill" | "runtiming" | "card";
export interface FeedEvent {
  key: string;
  kind: EventKind;
  /** one line */
  title: string;
  /** the detail, plain text (a list, a diff, the command to run) */
  text: string;
  at: string;
  /** good news only (a feed recovered): rides along in a digest, never the reason for one */
  fine?: boolean;
  /** sent at once even in digest mode (a whole feed failing / stale; owner 2026-10-28). For a URGENT kind (dfo,
   *  inseason) `false` sends it to the digest instead (T366: DFO rule-row changes) */
  urgent?: boolean;
  /** what the email template renders (src/email.ts), per kind; absent → the title + text, once */
  data?: any; // eslint-disable-line @typescript-eslint/no-explicit-any
}
/** emailed at once even in digest mode */
export const URGENT: readonly EventKind[] = ["dfo", "inseason"];
export interface Message {
  subject: string;
  text: string;
  /** the HTML part (multipart/alternative with `text`) */
  html?: string;
  keys: string[];
}
export interface NotifyConfig {
  emailTo?: string;
  emailFrom?: string;
  webhook?: string;
  digest?: boolean;
  digestHourUtc?: number;
  /** the env tag and the status link every email carries */
  ctx?: RenderCtx;
  /** sends one RFC 5322 message through the EMAIL binding (index.ts wires cloudflare:email) */
  sendEmail?: (from: string, to: string, raw: string) => Promise<void>;
}
/** (T558) how owner messages go out: email (address, sender and binding all set), else the webhook, else only the log */
export function channelOf(c: NotifyConfig): "email" | "webhook" | "log" {
  return c.emailTo && c.emailFrom && c.sendEmail ? "email" : c.webhook ? "webhook" : "log";
}
export interface NotifyState {
  told: Record<string, string>;
  pending: FeedEvent[];
  lastDigest: string | null;
}
export const NOTIFY_STATE = "notify/state.json";
const FORGET_MS = 366 * 86_400_000;
const KIND_TITLE: Record<EventKind, string> = {
  card: "Gauge share cards",
  hydat: "HYDAT",
  roster: "Gauge roster",
  inseason: "In-season regulation changes",
  dfo: "DFO salmon: places to review",
  health: "Feed health",
  report: "Problem reports from the app",
  crash: "New app errors",
  skill: "Forecast model scores",
  runtiming: "Run timing",
};

/** The outbox. Absent: a fresh one. (T558) A failed read or a body of the wrong shape THROWS (readStored): written back
 *  empty it would erase every told key and the queued digest. */
export async function readState(store: Store): Promise<NotifyState> {
  return checkState(await readStored<NotifyState>(store, NOTIFY_STATE));
}
function checkState(s: NotifyState | null): NotifyState {
  if (s === null) return { told: {}, pending: [], lastDigest: null };
  if (s && typeof s.told === "object" && s.told !== null && Array.isArray(s.pending)) return s;
  throw new Error(`${NOTIFY_STATE} is not an outbox (no told / pending); fix or delete it`);
}
/** (T558 W13) the outbox and its version tag, so the write can refuse to overwrite another job's change */
async function readStateAt(store: Store): Promise<{ st: NotifyState; etag: string | null }> {
  if (!isVersioned(store)) return { st: await readState(store), etag: null };
  const v = await store.getVersioned(NOTIFY_STATE);
  if (!v) return { st: checkState(null), etag: null };
  let s: NotifyState;
  try { s = JSON.parse(v.text); } catch (e) { throw new Error(`${NOTIFY_STATE} is stored but does not parse (${String(e)}); fix or delete it`); }
  return { st: checkState(s), etag: v.etag };
}

/**
 * (T558 W13) This run's change to the outbox (`orig` → `mine`) laid over what another job wrote meanwhile (`base`): keys
 * told now are told; keys forgotten now are forgotten; events this run took out of the queue leave it, the ones it queued
 * join it (unless told); the newer digest time stands.
 */
export function mergeState(base: NotifyState, orig: NotifyState, mine: NotifyState): NotifyState {
  const out: NotifyState = { told: { ...base.told }, pending: [...base.pending], lastDigest: base.lastDigest };
  for (const [k, at] of Object.entries(mine.told)) if (orig.told[k] !== at) out.told[k] = at;
  for (const k of Object.keys(orig.told)) if (!(k in mine.told)) delete out.told[k];
  const had = new Set(orig.pending.map((e) => e.key));
  const keep = new Set(mine.pending.map((e) => e.key));
  out.pending = out.pending.filter((e) => !(had.has(e.key) && !keep.has(e.key)) && !out.told[e.key]);
  for (const e of mine.pending) if (!had.has(e.key) && !out.told[e.key] && !out.pending.some((p) => p.key === e.key)) out.pending.push(e);
  if (mine.lastDigest && (!out.lastDigest || mine.lastDigest > out.lastDigest)) out.lastDigest = mine.lastDigest;
  return out;
}

// (T627) "card" was missing: a gauge-card event was in no email, and a digest recorded it as told all the same
const ORDER: EventKind[] = ["dfo", "inseason", "health", "report", "crash", "hydat", "runtiming", "roster", "skill", "card"];
const DEFAULT_CTX: RenderCtx = { env: "staging", statusUrl: null };

/** Events → messages through the one template (src/email.ts): one email per kind per run (all DFO places, all in-season
 *  rows, all health changes together), or — digest — one email with a section per kind. */
export function compose(events: FeedEvent[], digest = false, ctx: RenderCtx = DEFAULT_CTX): Message[] {
  if (!events.length) return [];
  // (T627) the digest has its own order and layout (src/digest.ts): a summary, then short blocks; every event is in it
  if (digest) return [{ ...renderDigest(events, ctx), keys: events.map((e) => e.key) }];
  const kinds = ORDER.filter((k) => events.some((e) => e.kind === k));
  return kinds.map((k) => {
    const es = events.filter((e) => e.kind === k);
    return { ...render([sectionFor(k, es, ctx)], ctx), keys: es.map((e) => e.key) };
  });
}
void KIND_TITLE;

/** RFC 5322 multipart/alternative (text + HTML), UTF-8, base64 parts (Cloudflare's send_email needs a Message-ID). */
export function mime(from: string, to: string, m: Message, now: Date): string {
  const enc = (s: string) => (/^[\x20-\x7e]*$/.test(s) ? s : `=?UTF-8?B?${b64(s)}?=`);
  const part = (t: string) => b64(t).replace(/.{76}/g, "$&\r\n");
  const domain = from.split("@")[1] ?? "localhost";
  const id = `${now.getTime().toString(36)}.${Math.random().toString(36).slice(2)}`;
  const head = [
    `From: BC fishing feeds <${from}>`,
    `To: ${to}`,
    `Subject: ${enc(m.subject)}`,
    `Date: ${now.toUTCString()}`,
    `Message-ID: <${id}@${domain}>`,
    "MIME-Version: 1.0",
  ];
  if (!m.html) return [...head, "Content-Type: text/plain; charset=utf-8", "Content-Transfer-Encoding: base64", "", part(m.text)].join("\r\n");
  const b = `=_feeds_${id}`;
  return [
    ...head,
    `Content-Type: multipart/alternative; boundary="${b}"`,
    "",
    `--${b}`,
    "Content-Type: text/plain; charset=utf-8",
    "Content-Transfer-Encoding: base64",
    "",
    part(m.text),
    `--${b}`,
    "Content-Type: text/html; charset=utf-8",
    "Content-Transfer-Encoding: base64",
    "",
    part(m.html),
    `--${b}--`,
    "",
  ].join("\r\n");
}
function b64(s: string): string {
  const bytes = new TextEncoder().encode(s);
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin);
}

export interface NotifyResult {
  /** the messages composed this run (sent, or — dry run — only shown) */
  messages: Message[];
  sentVia: ("email" | "webhook" | "log")[];
  queued: number;
  failed: number;
  dryRun: boolean;
}

/** Dedupe, queue or send, record. A failed send is retried next run; (T558) an outbox that cannot be read or written throws. */
export async function deliver(
  events: FeedEvent[],
  o: { store: Store; config: NotifyConfig; fetch: Fetch; now: Date; dryRun?: boolean; log?: (s: string) => void; flushDigest?: boolean },
): Promise<NotifyResult> {
  const log = o.log ?? (() => {});
  const { st, etag } = await readStateAt(o.store);
  const orig: NotifyState = JSON.parse(JSON.stringify(st));
  const nowIso = o.now.toISOString();
  for (const [k, at] of Object.entries(st.told)) if (o.now.getTime() - Date.parse(at) > FORGET_MS) delete st.told[k];
  const queuedKeys = new Set(st.pending.map((e) => e.key));
  const fresh: FeedEvent[] = [];
  for (const e of events) {
    if (st.told[e.key] || queuedKeys.has(e.key) || fresh.some((f) => f.key === e.key)) continue;
    fresh.push(e);
  }
  const digest = !!o.config.digest;
  // owner 2026-10-29: gauge health is never urgent, whatever flagged it
  // (T366) a DFO / in-season event marked `urgent: false` (DFO's rule-row digest) waits for the digest like the rest
  const isUrgent = (e: FeedEvent) =>
    URGENT.includes(e.kind) ? e.urgent !== false : !!e.urgent && !(e.kind === "health" && e.data?.job === "gauges");
  const now: FeedEvent[] = digest ? fresh.filter(isUrgent) : fresh;
  const later = digest ? fresh.filter((e) => !isUrgent(e)) : [];
  const pending = [...st.pending, ...later];
  const hour = o.config.digestHourUtc ?? 15;
  const due =
    digest &&
    pending.some((e) => !e.fine) &&
    (o.flushDigest || ((!st.lastDigest || o.now.getTime() - Date.parse(st.lastDigest) >= 20 * 3600_000) && o.now.getUTCHours() >= hour));
  const messages = [...compose(now, false, o.config.ctx), ...(due ? compose(pending, true, o.config.ctx) : [])];
  const res: NotifyResult = { messages, sentVia: [], queued: due ? 0 : pending.length, failed: 0, dryRun: !!o.dryRun };
  if (o.dryRun) return res;
  // a day that held only good news sends nothing: drop it once the digest hour has passed
  const quietDay = digest && !due && pending.length > 0 && pending.every((e) => e.fine) && o.now.getUTCHours() >= hour;

  const told: string[] = [];
  const failedKeys = new Set<string>();
  for (const m of messages) {
    const via = await send(m, o.config, o.fetch, o.now, log);
    if (via) {
      res.sentVia.push(via);
      told.push(...m.keys);
    } else {
      res.failed++;
      for (const k of m.keys) failedKeys.add(k);
    }
  }
  for (const k of told) st.told[k] = nowIso;
  // what failed waits in `pending` and goes again next run (digest or not)
  st.pending = due ? pending.filter((e) => failedKeys.has(e.key)) : quietDay ? [] : pending;
  if (quietDay) for (const e of pending) st.told[e.key] = nowIso;
  for (const e of now) if (failedKeys.has(e.key) && !st.pending.some((p) => p.key === e.key)) st.pending.push(e);
  if (due && !res.failed) st.lastDigest = nowIso;
  if (!digest && st.pending.length) {
    // retry of earlier failures in instant mode
    const retry = compose(st.pending.filter((e) => !failedKeys.has(e.key)), false, o.config.ctx);
    for (const m of retry) {
      const via = await send(m, o.config, o.fetch, o.now, log);
      if (via) {
        res.sentVia.push(via);
        for (const k of m.keys) st.told[k] = nowIso;
        st.pending = st.pending.filter((e) => !m.keys.includes(e.key));
      }
    }
  }
  // (T558 W13) written only over the state read (etag), else this run's change is merged onto the other job's; (W8) a
  // write that fails THROWS: the caller prints it (messages sent but not recorded may be sent again: never lost)
  try {
    await writeMerged(o.store, NOTIFY_STATE, st, etag, (fresh) => mergeState(checkState(fresh), orig, st), { contentType: "application/json" });
  } catch (e) {
    throw new Error(`notify: the outbox was not written (${e instanceof Error ? e.message : String(e)}); messages may repeat`);
  }
  return res;
}

/** One clearly-labelled test message through the configured channel (POST /notify/test): no dedupe, no state. */
export async function sendTest(c: NotifyConfig, fetch: Fetch, now: Date, label: string, log: (s: string) => void = () => {}, sample: FeedEvent[] = []): Promise<"email" | "webhook" | "log" | null> {
  // `sample` events render through the real template, the subject marked TEST
  const m: Message = sample.length
    ? (() => {
        const r = compose(sample, false, c.ctx)[0]!;
        return { ...r, subject: r.subject.replace(/^(\[[^\]]+\] )?/, (t) => `${t}TEST — `) };
      })()
    : {
        subject: `[${label}] TEST — notifications are wired`,
        text: `This is a TEST message from the ${label} feeds worker, sent ${now.toISOString()}.\n\nPolicy: DFO places to review and in-season changes are emailed at once; HYDAT, roster drift and feed health come in one daily digest, sent only on a day with an issue (gauge failures too, even a whole gauge feed failing). Current state: /status on the worker.\n\nNothing needs doing.\n`,
        keys: [],
      };
  return send(m, c, fetch, now, log);
}

/** (owner 2026-10-29) the new-APK email: urgent, sent at once, no dedupe / state. */
export async function sendApk(c: NotifyConfig, fetch: Fetch, now: Date, a: ApkInfo, log: (s: string) => void = () => {}): Promise<"email" | "webhook" | "log" | null> {
  const r = render([apkSection(a)], c.ctx ?? DEFAULT_CTX);
  return send({ ...r, keys: [] }, c, fetch, now, log);
}

async function send(m: Message, c: NotifyConfig, fetch: Fetch, now: Date, log: (s: string) => void): Promise<"email" | "webhook" | "log" | null> {
  if (c.emailTo && c.emailFrom && c.sendEmail) {
    try {
      await c.sendEmail(c.emailFrom, c.emailTo, mime(c.emailFrom, c.emailTo, m, now));
      return "email";
    } catch (e) {
      log(`notify: email failed (${String(e)})${c.webhook ? "; trying the webhook" : ""}`);
    }
  }
  if (c.webhook) {
    try {
      const [data, ctype] = webhookBody(c.webhook, `${m.subject}\n\n${m.text}`);
      const r = await fetch(c.webhook, { method: "POST", body: data, headers: { "Content-Type": ctype, Title: m.subject.slice(0, 120) } });
      if (r.status < 200 || r.status >= 300) throw new Error(`HTTP ${r.status}`);
      return "webhook";
    } catch (e) {
      log(`notify: webhook failed (${String(e)}); retried next run`);
      return null;
    }
  }
  if (c.emailTo && c.emailFrom && c.sendEmail) return null; // email configured but failed, no fallback: retry
  // (T558) production with no channel (NOTIFY_EMAIL_TO unset, no EMAIL binding): the message is KEPT to send, never
  // recorded as told; GET /status.json says `notify.channel: "log"`. Staging may still run log-only.
  if (c.ctx?.env === "production") {
    log(`notify: NO CHANNEL on production (NOTIFY_EMAIL_TO / EMAIL binding missing): kept to send: ${m.subject}`);
    return null;
  }
  log(`notify (no channel configured): ${m.subject}`);
  return "log";
}
