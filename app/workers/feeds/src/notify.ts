// T225 port of pipeline/gauges/feed/notify.py: tell the owner ONCE per notice key. The sent keys live in a PRIVATE
// store (never the published prefix), as `<feed>.notified.json` lives beside the feed dir. A failed send is not
// recorded (retried next run) and never fails the feed.
import type { Fetch, Store } from "./io";

export interface Notice {
  [k: string]: unknown;
}
export type KeyOf = (n: any) => string;
export type MessageOf = (n: any) => string;

/** Gauge maintenance notices: `kind:latest`. */
export const key: KeyOf = (n) => `${n.kind}:${n.latest}`;

export const message: MessageOf = (n) => {
  if (n.kind === "hydat") return `HYDAT ${n.latest} is out; the gauge feed is on ${n.current}. Run: ${n.action}`;
  if (n.kind === "roster") return `ECCC's real-time station list differs from ${n.current} (${n.latest}). Run: ${n.action}`;
  return `${n.kind}: ${n.latest} is due (on ${n.current}). Run: ${n.action}`;
};

export function body(url: string, text: string): [string, string] {
  if (url.includes("discord.com/api/webhooks/") || url.includes("discordapp.com/api/webhooks/"))
    return [JSON.stringify({ content: text }), "application/json"];
  if (url.includes("hooks.slack.com/")) return [JSON.stringify({ text }), "application/json"];
  return [text, "text/plain; charset=utf-8"];
}

export interface SendDueOptions {
  url?: string | null;
  fetch: Fetch;
  /** private store + key holding `{sent: [...]}` */
  state: Store;
  stateKey: string;
  keyOf?: KeyOf;
  messageOf?: MessageOf;
  log?: (s: string) => void;
}

export async function sendDue(
  notices: Notice[],
  o: SendDueOptions,
): Promise<{ sent: string[]; failed: string[]; off: boolean }> {
  const log = o.log ?? (() => {});
  const keyOf = o.keyOf ?? key;
  const messageOf = o.messageOf ?? message;
  if (!o.url) {
    if (notices.length) log(`notify: off (FEED_NOTIFY_URL unset); ${notices.length} notice${notices.length !== 1 ? "s" : ""}`);
    return { sent: [], failed: [], off: true };
  }
  let done: string[] = [];
  try {
    const got = JSON.parse((await o.state.get(o.stateKey)) ?? "{}").sent;
    if (Array.isArray(got)) done = got.map(String);
  } catch {
    done = [];
  }
  const sent: string[] = [];
  const failed: string[] = [];
  for (const n of notices) {
    const k = keyOf(n);
    if (done.includes(k) || sent.includes(k)) continue;
    try {
      const [data, ctype] = body(o.url, messageOf(n));
      const r = await o.fetch(o.url, {
        method: "POST",
        body: data,
        headers: { "Content-Type": ctype, Title: "BC fishing feed: update due" },
      });
      if (r.status < 200 || r.status >= 300) throw new Error(`HTTP ${r.status}`);
      sent.push(k);
    } catch (e) {
      failed.push(k);
      log(`notify: ${k} not sent (${String(e)}); retried next run`);
    }
  }
  if (sent.length) {
    try {
      await o.state.put(o.stateKey, JSON.stringify({ sent: [...done, ...sent] }));
    } catch (e) {
      log(`notify: could not record what was sent (${String(e)}); it may be sent again`);
    }
  }
  return { sent, failed, off: false };
}
