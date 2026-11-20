/**
 * (T739) THE PUSH TOKEN, KEPT IN STEP WITH THE READER'S NOTIFICATIONS. Platform-free: the app hands in the platform's
 * token seam (`apps/mobile` `pushToken.ios.ts`: expo-notifications' device token; none on the web and none on Android,
 * owner 2026-11-16: no Android push), our worker's address (`POST <url>/register|unregister`,
 * app/workers/feeds src/push.ts) and a small store of what was last sent.
 *
 *   on    (the reader has notifications on: `notifyActive`) the token is read and registered when it, its environment
 *         or our address changed, or when the last register is older than PUSH_REFRESH_MS (7 days) — so a token the
 *         worker deleted (APNs called it dead, its store was lost) comes back within a week; an older one is
 *         unregistered first; a new token from the system (`onToken`) is sent the same way. (Review 2026-11-20, L1: it
 *         was once per app start, and several iPhones behind one address relaunching together on a wake met the
 *         worker's per-address limit.)
 *   off   the token last sent is unregistered and forgotten.
 *   WHAT IS SENT: {platform, token, env} — never an alert, a water, a location or the install id.
 *
 * A failed send THROWS (the caller reports it; offline is the environment's and never reported) and the store keeps
 * what was really sent, so the next sync (the next start, the next switch, or "Try again") tries again.
 *
 * (T822) NEVER SILENT. On iPhone build 36 the owner turned notifications on and the staging worker never heard from the
 * phone, with nothing in the crash inbox: two paths here said nothing. (1) The system's token call had no deadline:
 * expo-notifications' `getDevicePushTokenAsync` waits for iOS's `didRegisterForRemoteNotifications…` callback and never
 * settles when iOS does not answer, which held the sync queue for the whole run (every later switch queued behind it).
 * Now the token has `tokenWaitMs` (30 s) and then a REPORTED error; a token that comes later still arrives through
 * `onToken`. (2) Offline and the worker's 429 were "the environment's": thrown, never reported, and never shown. Now a 429
 * is waited out once (`retryMs`, past the worker's 2 s gap: a quick off → on hit it and lost the token); a second one
 * leaves the register PENDING (said on the page; not reported, the worker's limit is the shared address's, L1) and
 * the next start tries again; offline stays unreported (failure.ts) but is SHOWN. Every outcome is the registrar's
 * `status()` (More → Notifications says it, with "Try again").
 */
import { environmentError, isEnvironment, reportFailure } from "@app/core";

/** (owner 2026-11-16 12:08, on Firebase: "we dont want this right now") iPhone only; the worker refuses "android" */
export type PushPlatformName = "ios";
export type PushEnvName = "sandbox" | "production";

/** The platform's half (`apps/mobile` `pushToken.*`). */
export interface PushSeam {
  readonly platform: PushPlatformName;
  /** APNs environment of this build ("sandbox" for a simulator / development build) */
  readonly env: PushEnvName;
  /** the device push token (APNs hex); throws when the system will not give one */
  token(): Promise<string>;
  /** the system handed the app a new token; returns the undo */
  onToken(on: (token: string) => void): () => void;
}

export interface PushSent {
  readonly platform: PushPlatformName; readonly token: string; readonly env: PushEnvName; readonly url: string;
  /** (L1) when our worker last answered this register (ms); absent in a record from before: registered again */
  readonly at?: number;
}

export interface PushStoreText { read(): string | null; write(text: string): void }

/**
 * (T822) What the registrar last did, for More → Notifications: never sent (notifications off this run), a send in
 * flight, registered with our server, withdrawn, or failed (with why; `environment`: offline, not reported but shown).
 */
export type PushStatus =
  | { readonly state: "idle" }
  | { readonly state: "sending"; readonly since: number }
  | { readonly state: "registered"; readonly at: number }
  | { readonly state: "withdrawn"; readonly at: number }
  | { readonly state: "failed"; readonly at: number; readonly why: string; readonly environment: boolean }
  /** (L1) the worker's per-address limit answered twice: not registered yet, tried again at the next start */
  | { readonly state: "pending"; readonly at: number };

export interface PushRegistrar {
  /** register (true) or unregister (false); resolves once our server has answered */
  sync(active: boolean): Promise<void>;
  /** (T822) the last outcome, said on the page */
  status(): PushStatus;
  /** (T822) told of every status change; returns the undo */
  subscribe(on: () => void): () => void;
}

/** (T822) how long the system may take to hand over the device token before the sync fails, said and reported */
export const PUSH_TOKEN_WAIT_MS = 30_000;
/** (T822) a 429 is waited out once this long (the worker's per-address gap is 2 s, `PUSH_GAP_MS`) */
export const PUSH_RETRY_MS = 3_000;
/** (L1) an unchanged token is registered again once its last register is this old (a worker that lost it gets it back) */
export const PUSH_REFRESH_MS = 7 * 86_400_000;

/** (L1) the worker's limit twice: an environment error (never reported), the status says "pending" */
class PushBusy extends Error {}

type Fetch = (u: string, i?: RequestInit) => Promise<Response>;

function readSent(store: PushStoreText): PushSent | null {
  const t = store.read();
  if (!t) return null;
  try {
    const j = JSON.parse(t) as PushSent;
    return j && typeof j.token === "string" && typeof j.url === "string" ? j : null;
  } catch (e) { reportFailure(e); return null; }   // our own small file, unreadable: reported, and the next sync registers again
}

const sleep = (ms: number) => new Promise<void>((ok) => setTimeout(ok, ms));

export function createPushRegistrar(o: {
  seam: PushSeam; url: string; fetch: Fetch; store: PushStoreText;
  /** (T822) tests: the token deadline, the 429 wait, the clock */
  tokenWaitMs?: number; retryMs?: number; now?: () => number; wait?: (ms: number) => Promise<void>;
}): PushRegistrar {
  const base = o.url.replace(/\/+$/, "");
  const tokenWaitMs = o.tokenWaitMs ?? PUSH_TOKEN_WAIT_MS;
  const now = o.now ?? Date.now;
  const wait = o.wait ?? sleep;
  let status: PushStatus = { state: "idle" };
  const subs = new Set<() => void>();
  const set = (st: PushStatus) => { status = st; for (const f of [...subs]) f(); };
  const once = async (what: "register" | "unregister", s: Omit<PushSent, "url">): Promise<Response> => {
    try {
      return await o.fetch(`${base}/${what}`, { method: "POST", headers: { "Content-Type": "application/json" },
                                                body: JSON.stringify({ platform: s.platform, token: s.token, env: s.env }) });
    } catch (e) { throw e instanceof Error ? environmentError(e) : e; }   // no line: the environment's (shown, not reported)
  };
  const post = async (what: "register" | "unregister", s: Omit<PushSent, "url">) => {
    let res = await once(what, s);
    // (T822) the worker's per-address gap (2 s): waited out once; a second refusal is the day's cap, a defect of our limits
    if (res.status === 429) { await wait(o.retryMs ?? PUSH_RETRY_MS); res = await once(what, s); }
    if (res.status === 429) throw environmentError(new PushBusy(`push ${what}: HTTP 429 twice (the worker's per-address limit)`));
    if (!res.ok) throw new Error(`push ${what}: HTTP ${res.status}`);
  };
  /** (T822) the system's token, or a REPORTED error once `tokenWaitMs` has passed without one */
  const token = async (): Promise<string> => {
    let t: ReturnType<typeof setTimeout> | undefined;
    const late = new Promise<never>((_, fail) => {
      t = setTimeout(() => fail(new Error(`push register: the system gave no push token in ${Math.round(tokenWaitMs / 1000)} s `
        + `(${o.seam.platform}: the request for one was never answered)`)), tokenWaitMs);
    });
    try { return await Promise.race([o.seam.token(), late]); } finally { clearTimeout(t); }
  };
  /** (T822) every sync's outcome is the status; the error goes on to the caller (reported unless the environment's) */
  const said = async (f: () => Promise<void>, done: () => PushStatus) => {
    set({ state: "sending", since: now() });
    try { await f(); set(done()); } catch (e) {
      set(e instanceof PushBusy ? { state: "pending", at: now() }
        : { state: "failed", at: now(), why: e instanceof Error ? e.message : String(e), environment: isEnvironment(e) });
      throw e;
    }
  };
  let active = false;
  let undo: (() => void) | null = null;
  // one sync at a time, in order (a quick on → off → on ends where the reader left it)
  let chain: Promise<void> = Promise.resolve();
  /** when the register the status shows was answered (a skipped one shows the stored time) */
  let registeredAt = 0;
  const register = async (token: string) => {
    const was = readSent(o.store);
    const cur = { platform: o.seam.platform, token, env: o.seam.env };
    const same = !!was && was.token === token && was.env === cur.env && was.url === base;
    // (L1) unchanged and registered within PUSH_REFRESH_MS: nothing sent
    if (same && typeof was.at === "number" && now() - was.at < PUSH_REFRESH_MS) { registeredAt = was.at; return; }
    if (was && was.url === base && was.token !== token) await post("unregister", was);
    await post("register", cur);
    registeredAt = now();
    o.store.write(JSON.stringify({ ...cur, url: base, at: registeredAt }));
  };
  const unregister = async () => {
    const was = readSent(o.store);
    if (!was) return;
    // a token sent to another address (a build switched environment) is dropped there
    await post("unregister", was).catch((e) => { if (was.url === base) throw e; reportFailure(e); });
    o.store.write("");
  };
  const run = (f: () => Promise<void>) => (chain = chain.then(f, f));
  const registered = (): PushStatus => ({ state: "registered", at: registeredAt });
  return {
    sync(on) {
      active = on;
      if (on && !undo) {
        undo = o.seam.onToken((t) => { if (active) run(() => said(() => register(t), registered)).catch(reportFailure); });
      }
      if (!on && undo) { undo(); undo = null; }
      return run(() => (on ? said(async () => register(await token()), registered)
                           : said(unregister, () => ({ state: "withdrawn", at: now() }))));
    },
    status: () => status,
    subscribe(f) { subs.add(f); return () => { subs.delete(f); }; },
  };
}
