/**
 * bc-fishing-cron — a tiny Cloudflare Worker on a Cron Trigger whose only job is
 * to DISPATCH GitHub Actions workflows on a reliable clock.
 *
 * Why this exists: GitHub's own `schedule:` trigger drifts 20-30 min under load.
 * Cloudflare Cron Triggers fire punctually. So the Worker fires on schedule and
 * calls the GitHub REST API `workflow_dispatch` — GHA still does all the actual
 * work (fetch/export/upload). No container, no image, no Durable Object: this is
 * a plain Worker (free tier) that makes a few HTTPS calls and exits.
 *
 * Config (wrangler.toml [vars]):
 *   GITHUB_OWNER, GITHUB_REPO — the repo hosting the workflows.
 *   GITHUB_REF                — branch to run on (default "main").
 *   WORKFLOWS                 — comma-separated workflow *file names* to dispatch
 *                               (e.g. "update-hydro.yml").
 * Secrets (wrangler secret put):
 *   GITHUB_TOKEN   — fine-grained PAT with Actions: read/write on this repo.
 *   TRIGGER_TOKEN  — optional; guards the manual GET /__run?token=… endpoint.
 */

interface Env {
  GITHUB_OWNER: string;
  GITHUB_REPO: string;
  GITHUB_REF?: string;
  WORKFLOWS: string; // comma-separated workflow file names
  GITHUB_TOKEN: string;
  TRIGGER_TOKEN?: string;
}

/** POST a single workflow_dispatch. Returns the HTTP status for logging. */
async function dispatchWorkflow(env: Env, workflow: string): Promise<number> {
  const ref = env.GITHUB_REF || "main";
  const url =
    `https://api.github.com/repos/${env.GITHUB_OWNER}/${env.GITHUB_REPO}` +
    `/actions/workflows/${encodeURIComponent(workflow)}/dispatches`;

  const resp = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.GITHUB_TOKEN}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      // GitHub rejects requests without a User-Agent.
      "User-Agent": "bc-fishing-cron",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ ref }),
  });

  if (resp.status !== 204) {
    // 204 No Content = accepted. Anything else is worth logging the body for.
    const body = await resp.text();
    console.error(`dispatch ${workflow} → ${resp.status}: ${body.slice(0, 500)}`);
  } else {
    console.log(`dispatched ${workflow} on ${ref} → 204`);
  }
  return resp.status;
}

/** Dispatch every workflow named in WORKFLOWS. */
async function dispatchAll(env: Env): Promise<{ workflow: string; status: number }[]> {
  const names = (env.WORKFLOWS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const results: { workflow: string; status: number }[] = [];
  for (const name of names) {
    results.push({ workflow: name, status: await dispatchWorkflow(env, name) });
  }
  return results;
}

export default {
  // Fired by the Cron Trigger(s) in wrangler.toml.
  async scheduled(_controller: ScheduledController, env: Env): Promise<void> {
    await dispatchAll(env);
  },

  // Manual trigger for testing without waiting for the cron. Guarded by a secret
  // token so a public URL can't kick off runs. GET /__run?token=…
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname === "/__run") {
      if (!env.TRIGGER_TOKEN || url.searchParams.get("token") !== env.TRIGGER_TOKEN) {
        return new Response("forbidden\n", { status: 403 });
      }
      const results = await dispatchAll(env);
      return new Response(JSON.stringify({ dispatched: results }, null, 2) + "\n", {
        headers: { "Content-Type": "application/json" },
      });
    }
    return new Response(
      `bc-fishing-cron: dispatches GitHub Actions workflows on a Cron Trigger.\n` +
        `Targets: ${env.WORKFLOWS || "(none configured)"}\n`,
    );
  },
};
