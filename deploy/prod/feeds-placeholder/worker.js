// PRODUCTION feeds worker PLACEHOLDER (T404 prep, owner 2026-11-05): the name exists so its secrets can be set ahead of
// the prod switch. No bindings, no crons, no feed code: every request answers 503. T404 replaces it with
// app/workers/feeds (a production wrangler config), keeping the secrets.
export default {
  async fetch() {
    return new Response("feeds (production) is not live yet\n", { status: 503, headers: { "Cache-Control": "no-store" } });
  },
};
