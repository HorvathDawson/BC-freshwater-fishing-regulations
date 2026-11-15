/**
 * (T739) THE PUSH TOKEN, KEPT IN STEP WITH THE READER'S NOTIFICATIONS. Platform-free: the app hands in the platform's
 * token seam (`apps/mobile` `pushToken.ios.ts` / `.android.ts`: expo-notifications' device token; none on the web and
 * none on Android until the Firebase file is in the build), our worker's address (`POST <url>/register|unregister`,
 * app/workers/feeds src/push.ts) and a small store of what was last sent.
 *
 *   on    (the reader has notifications on: `notifyActive`) the token is read and, when it is not the one last sent,
 *         registered (an older one unregistered first); a new token from the system (`onToken`) is sent the same way.
 *   off   the token last sent is unregistered and forgotten.
 *   WHAT IS SENT: {platform, token, env} — never an alert, a water, a location or the install id.
 *
 * A failed send THROWS (the caller reports it; offline is the environment's and never reported) and the store keeps
 * what was really sent, so the next sync (the next start, or the next switch) tries again.
 */
import { environmentError, reportFailure } from "@app/core";

export type PushPlatformName = "ios" | "android";
export type PushEnvName = "sandbox" | "production";

/** The platform's half (`apps/mobile` `pushToken.*`). */
export interface PushSeam {
  readonly platform: PushPlatformName;
  /** APNs environment of this build ("sandbox" for a simulator / development build); Android is always "production" */
  readonly env: PushEnvName;
  /** the device push token (APNs hex / FCM token); throws when the system will not give one */
  token(): Promise<string>;
  /** the system handed the app a new token; returns the undo */
  onToken(on: (token: string) => void): () => void;
}

export interface PushSent { readonly platform: PushPlatformName; readonly token: string; readonly env: PushEnvName; readonly url: string }

export interface PushStoreText { read(): string | null; write(text: string): void }

export interface PushRegistrar {
  /** register (true) or unregister (false); resolves once our server has answered */
  sync(active: boolean): Promise<void>;
}

type Fetch = (u: string, i?: RequestInit) => Promise<Response>;

function readSent(store: PushStoreText): PushSent | null {
  const t = store.read();
  if (!t) return null;
  try {
    const j = JSON.parse(t) as PushSent;
    return j && typeof j.token === "string" && typeof j.url === "string" ? j : null;
  } catch (e) { reportFailure(e); return null; }   // our own small file, unreadable: reported, and the next sync registers again
}

export function createPushRegistrar(o: { seam: PushSeam; url: string; fetch: Fetch; store: PushStoreText }): PushRegistrar {
  const base = o.url.replace(/\/+$/, "");
  const post = async (what: "register" | "unregister", s: Omit<PushSent, "url">) => {
    let res: Response;
    try {
      res = await o.fetch(`${base}/${what}`, { method: "POST", headers: { "Content-Type": "application/json" },
                                               body: JSON.stringify({ platform: s.platform, token: s.token, env: s.env }) });
    } catch (e) { throw e instanceof Error ? environmentError(e) : e; }   // no line: the environment's, tried again later
    if (!res.ok) throw new Error(`push ${what}: HTTP ${res.status}`);
  };
  let active = false;
  let undo: (() => void) | null = null;
  // one sync at a time, in order (a quick on → off → on ends where the reader left it)
  let chain: Promise<void> = Promise.resolve();
  const register = async (token: string) => {
    const was = readSent(o.store);
    const now = { platform: o.seam.platform, token, env: o.seam.env };
    if (was && was.token === token && was.env === now.env && was.url === base) return;
    if (was && was.url === base && was.token !== token) await post("unregister", was);
    await post("register", now);
    o.store.write(JSON.stringify({ ...now, url: base }));
  };
  const unregister = async () => {
    const was = readSent(o.store);
    if (!was) return;
    // a token sent to another address (a build switched environment) is dropped there
    await post("unregister", was).catch((e) => { if (was.url === base) throw e; reportFailure(e); });
    o.store.write("");
  };
  const run = (f: () => Promise<void>) => (chain = chain.then(f, f));
  return {
    sync(on) {
      active = on;
      if (on && !undo) undo = o.seam.onToken((t) => { if (active) run(() => register(t)).catch(reportFailure); });
      if (!on && undo) { undo(); undo = null; }
      return run(async () => { if (on) await register(await o.seam.token()); else await unregister(); });
    },
  };
}
