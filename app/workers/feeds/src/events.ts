// T225 follow-up (owner 2026-10-28): EVERYTHING THE OWNER SHOULD HEAR ABOUT, ONE OUTBOX. Each job turns what it found
// into `FeedEvent`s (a stable `key` each); this module dedupes them (a key is told once), groups them into messages and
// delivers them:
//   email    Cloudflare Email Workers (`send_email` binding EMAIL) from NOTIFY_EMAIL_FROM to NOTIFY_EMAIL_TO (the owner's
//            verified Email Routing address) — when both are set
//   webhook  FEED_NOTIFY_URL (ntfy / Discord / Slack) — when set, and as the fallback when the email send fails
//   log      neither configured: logged, and still recorded as told (nothing replays when a channel is added later)
// NOTIFY_DIGEST=daily queues every kind but `health` and sends ONE digest a day (first run at/after
// NOTIFY_DIGEST_HOUR_UTC, default 15 = 08:00 PDT); `health` (a feed failing / stale / recovering) is always immediate.
// Dry run: nothing sent, nothing recorded — the messages are returned (POST /run/<job>?dryNotify=1, /notify/preview).
//
// State (private): notify/state.json — {told: {key: at}, pending: [...], lastDigest}. A key is forgotten after a year.
import { body as webhookBody } from "./notify";
import type { Fetch, Store } from "./io";

export type EventKind = "hydat" | "roster" | "inseason" | "dfo" | "health";
export interface FeedEvent {
  key: string;
  kind: EventKind;
  /** one line */
  title: string;
  /** the detail, plain text (a list, a diff, the command to run) */
  text: string;
  at: string;
}
export interface Message {
  subject: string;
  text: string;
  keys: string[];
}
export interface NotifyConfig {
  emailTo?: string;
  emailFrom?: string;
  webhook?: string;
  digest?: boolean;
  digestHourUtc?: number;
  /** sends one RFC 5322 message through the EMAIL binding (index.ts wires cloudflare:email) */
  sendEmail?: (from: string, to: string, raw: string) => Promise<void>;
}
export interface NotifyState {
  told: Record<string, string>;
  pending: FeedEvent[];
  lastDigest: string | null;
}
export const NOTIFY_STATE = "notify/state.json";
const FORGET_MS = 366 * 86_400_000;
const KIND_TITLE: Record<EventKind, string> = {
  hydat: "HYDAT",
  roster: "Gauge roster",
  inseason: "In-season regulation changes",
  dfo: "DFO salmon: places to review",
  health: "Feed health",
};

export async function readState(store: Store): Promise<NotifyState> {
  try {
    const s = JSON.parse((await store.get(NOTIFY_STATE)) ?? "null");
    if (s && typeof s.told === "object" && Array.isArray(s.pending)) return s;
  } catch {
    /* fresh */
  }
  return { told: {}, pending: [], lastDigest: null };
}

/** Events → messages: HYDAT, roster and health one per event; in-season and DFO one per kind (a list). */
export function compose(events: FeedEvent[], digest = false): Message[] {
  if (!events.length) return [];
  if (digest) {
    const kinds = [...new Set(events.map((e) => e.kind))];
    const text = kinds
      .map((k) => {
        const es = events.filter((e) => e.kind === k);
        return `== ${KIND_TITLE[k]} (${es.length}) ==\n\n${es.map((e) => `* ${e.title}\n${indent(e.text)}`).join("\n\n")}`;
      })
      .join("\n\n");
    return [{ subject: `BC fishing feeds — daily digest: ${events.length} item${events.length > 1 ? "s" : ""}`, text, keys: events.map((e) => e.key) }];
  }
  const out: Message[] = [];
  for (const e of events.filter((x) => x.kind === "hydat" || x.kind === "roster" || x.kind === "health"))
    out.push({ subject: `BC fishing feeds — ${e.title}`, text: e.text, keys: [e.key] });
  for (const k of ["inseason", "dfo"] as const) {
    const es = events.filter((e) => e.kind === k);
    if (!es.length) continue;
    out.push({
      subject: `BC fishing feeds — ${KIND_TITLE[k]}: ${es.length} new`,
      text: es.map((e) => `* ${e.title}\n${indent(e.text)}`).join("\n\n"),
      keys: es.map((e) => e.key),
    });
  }
  return out;
}
const indent = (t: string) => t.split("\n").map((l) => (l ? `  ${l}` : l)).join("\n");

/** RFC 5322 text/plain message, UTF-8, base64 body (Cloudflare's send_email needs a Message-ID). */
export function mime(from: string, to: string, m: Message, now: Date): string {
  const enc = (s: string) => (/^[\x20-\x7e]*$/.test(s) ? s : `=?UTF-8?B?${b64(s)}?=`);
  const body = b64(m.text).replace(/.{76}/g, "$&\r\n");
  const domain = from.split("@")[1] ?? "localhost";
  return [
    `From: BC fishing feeds <${from}>`,
    `To: ${to}`,
    `Subject: ${enc(m.subject)}`,
    `Date: ${now.toUTCString()}`,
    `Message-ID: <${now.getTime().toString(36)}.${Math.random().toString(36).slice(2)}@${domain}>`,
    "MIME-Version: 1.0",
    "Content-Type: text/plain; charset=utf-8",
    "Content-Transfer-Encoding: base64",
    "",
    body,
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

/** Dedupe, queue or send, record. Never throws (a failed send is retried next run). */
export async function deliver(
  events: FeedEvent[],
  o: { store: Store; config: NotifyConfig; fetch: Fetch; now: Date; dryRun?: boolean; log?: (s: string) => void; flushDigest?: boolean },
): Promise<NotifyResult> {
  const log = o.log ?? (() => {});
  const st = await readState(o.store);
  const nowIso = o.now.toISOString();
  for (const [k, at] of Object.entries(st.told)) if (o.now.getTime() - Date.parse(at) > FORGET_MS) delete st.told[k];
  const queuedKeys = new Set(st.pending.map((e) => e.key));
  const fresh: FeedEvent[] = [];
  for (const e of events) {
    if (st.told[e.key] || queuedKeys.has(e.key) || fresh.some((f) => f.key === e.key)) continue;
    fresh.push(e);
  }
  const digest = !!o.config.digest;
  const now: FeedEvent[] = digest ? fresh.filter((e) => e.kind === "health") : fresh;
  const later = digest ? fresh.filter((e) => e.kind !== "health") : [];
  const pending = [...st.pending, ...later];
  const hour = o.config.digestHourUtc ?? 15;
  const due =
    digest &&
    pending.length > 0 &&
    (o.flushDigest || ((!st.lastDigest || o.now.getTime() - Date.parse(st.lastDigest) >= 20 * 3600_000) && o.now.getUTCHours() >= hour));
  const messages = [...compose(now), ...(due ? compose(pending, true) : [])];
  const res: NotifyResult = { messages, sentVia: [], queued: due ? 0 : pending.length, failed: 0, dryRun: !!o.dryRun };
  if (o.dryRun) return res;

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
  st.pending = due ? pending.filter((e) => failedKeys.has(e.key)) : pending;
  for (const e of now) if (failedKeys.has(e.key) && !st.pending.some((p) => p.key === e.key)) st.pending.push(e);
  if (due && !res.failed) st.lastDigest = nowIso;
  if (!digest && st.pending.length) {
    // retry of earlier failures in instant mode
    const retry = compose(st.pending.filter((e) => !failedKeys.has(e.key)));
    for (const m of retry) {
      const via = await send(m, o.config, o.fetch, o.now, log);
      if (via) {
        res.sentVia.push(via);
        for (const k of m.keys) st.told[k] = nowIso;
        st.pending = st.pending.filter((e) => !m.keys.includes(e.key));
      }
    }
  }
  try {
    await o.store.put(NOTIFY_STATE, JSON.stringify(st), { contentType: "application/json" });
  } catch (e) {
    log(`notify: state not written (${String(e)}); messages may repeat`);
  }
  return res;
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
  log(`notify (no channel configured): ${m.subject}`);
  return "log";
}
