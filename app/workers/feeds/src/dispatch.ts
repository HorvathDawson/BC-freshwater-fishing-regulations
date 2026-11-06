// T413: THE WORKER STARTS A GITHUB WORKFLOW ITSELF when a slow source moved (a new HYDAT today; run timing next, T412).
// Owner 2026-11-05: GitHub is never the scheduler — the worker notices the change and starts the run.
//
// Needs the secret GITHUB_DISPATCH_TOKEN (a fine-grained token, Actions: write on this repo only; the OWNER makes it).
// Without it nothing is started and the email keeps today's "press the button" steps. A start is recorded per workflow +
// version in R2 (private/feeds/dispatch/<workflow>.json), so one version starts one run; a failed start is retried at
// most every RETRY_MS and the email says why it failed (never silent).
import type { Store } from "./io";

export const REPO = "HorvathDawson/BC-freshwater-fishing-regulations";
/** workflow_dispatch runs the file as it is on this ref (the dev branch until the workflows are on main) */
export const DEFAULT_REF = "redesign/stream-sections";
export const RETRY_MS = 6 * 3600_000;

export type Dispatch =
  | { state: "no-token" }
  | { state: "started"; at: string; workflow: string; ref: string }
  | { state: "failed"; at: string; workflow: string; ref: string; error: string };

export interface DispatchEnv {
  GITHUB_DISPATCH_TOKEN?: string;
  GITHUB_DISPATCH_REF?: string;
}

type Fetch = (url: string, init?: RequestInit) => Promise<Response>;

/** Start `workflow` once for `version` (a repeat call for a started version returns the recorded start). */
export async function dispatchOnce(
  env: DispatchEnv,
  fetch_: Fetch,
  store: Store,
  workflow: string,
  version: string,
  inputs: Record<string, string>,
  now: Date,
): Promise<Dispatch> {
  if (!env.GITHUB_DISPATCH_TOKEN) return { state: "no-token" };
  const ref = env.GITHUB_DISPATCH_REF || DEFAULT_REF;
  const key = `${workflow}.json`;
  let last: (Dispatch & { version?: string }) | null = null;
  try {
    last = JSON.parse((await store.get(key)) ?? "null");
  } catch {
    last = null;
  }
  if (last && last.version === version && last.state === "started") return last;
  if (last && last.version === version && last.state === "failed" && now.getTime() - Date.parse(last.at) < RETRY_MS) return last;

  const at = now.toISOString();
  let out: Dispatch;
  try {
    const r = await fetch_(`https://api.github.com/repos/${REPO}/actions/workflows/${workflow}/dispatches`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.GITHUB_DISPATCH_TOKEN}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "bc-fishing-feeds-worker",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ ref, inputs }),
    });
    out = r.status === 204
      ? { state: "started", at, workflow, ref }
      : { state: "failed", at, workflow, ref, error: `GitHub answered ${r.status}: ${(await r.text()).slice(0, 300)}` };
  } catch (e) {
    out = { state: "failed", at, workflow, ref, error: String(e) };
  }
  await store.put(key, JSON.stringify({ ...out, version }), { contentType: "application/json" });
  return out;
}

/** The "what to do" words for an email, from a dispatch result (the button steps when nothing was started). */
export function dispatchSteps(d: Dispatch | undefined, button: string): { text: string; code?: string }[] {
  if (!d || d.state === "no-token") return [{ text: button }];
  if (d.state === "started")
    return [{ text: `Nothing to press: the worker started "${d.workflow}" on ${d.ref} at ${d.at}. Check the run in GitHub → Actions.` }];
  return [
    { text: `The worker could not start "${d.workflow}" (${d.error}). It retries every ${RETRY_MS / 3600_000} h; or start it by hand:` },
    { text: button },
  ];
}

/** (T413) Is the token good for this repo? Reads the HYDAT workflow with it — starts nothing. GitHub cannot show a
 *  fine-grained token's write permission without using it: a read that answers 200 proves the token and the repo. */
export async function checkToken(env: DispatchEnv, fetch_: Fetch): Promise<{ ok: boolean; status: number | null; says: string }> {
  if (!env.GITHUB_DISPATCH_TOKEN) return { ok: false, status: null, says: "GITHUB_DISPATCH_TOKEN is not set" };
  const r = await fetch_(`https://api.github.com/repos/${REPO}/actions/workflows/feeds-hydat.yml`, {
    headers: { Authorization: `Bearer ${env.GITHUB_DISPATCH_TOKEN}`, Accept: "application/vnd.github+json",
               "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "bc-fishing-feeds-worker" },
  });
  const body = await r.text();
  let says = body.slice(0, 200);
  try { const j = JSON.parse(body); says = r.ok ? `${j.name} (${j.state})` : String(j.message ?? says); } catch { /* the text */ }
  return { ok: r.status === 200, status: r.status, says };
}
