/**
 * (T265) ONE REQUEST PER URL, WHATEVER HOW MANY ASK: concurrent plain GETs of the same URL share one network request.
 *
 * The gauge feed's `index.json` was fetched three times at first paint (a CORS preflight, the update service's check
 * and the feed loader's own read, all in the same millisecond): each asked for the file without knowing the other had.
 * Both are handed this wrapper, so the second asker joins the request already in the air and gets its own copy of the
 * Response (`clone()`, so each reads the body once).
 *
 *   JOINS     a GET with no `Range` and no conditional header (`If-None-Match` / `If-Modified-Since` / `If-Match`: the
 *             answer is the caller's own 304 or not). A `no-store` asker joins only a `no-store` request (it wants the
 *             origin's word); a default-cache asker may join either.
 *   ABORTS    each asker's `signal` cuts only that asker. The request itself is cut when every asker has gone, so one
 *             caller's timeout (T254) never fails the others'.
 *   SCOPE     "in the air" = until the response HEADERS arrive. Nothing is cached after: the next ask is a fresh fetch.
 */
type Entry = { p: Promise<Response>; ctl: AbortController; askers: number };

const abortError = () => { const e = new Error("aborted"); e.name = "AbortError"; return e; };

export function shareFetch(inner: typeof fetch): typeof fetch {
  const air = new Map<string, Entry>();

  const eligible = (input: RequestInfo | URL, init?: RequestInit): string | null => {
    if (typeof input !== "string" && !(input instanceof URL)) return null;        // a Request carries its own body/headers
    if ((init?.method ?? "GET").toUpperCase() !== "GET" || init?.body) return null;
    const h = new Headers(init?.headers);
    if (h.has("range") || h.has("if-none-match") || h.has("if-modified-since") || h.has("if-match")) return null;
    return String(input);
  };

  return ((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = eligible(input, init);
    if (url === null) return inner(input, init);
    const noStore = init?.cache === "no-store";
    const ns = `${url}#no-store`;
    let e = (noStore ? air.get(ns) : air.get(ns) ?? air.get(url));
    const signal = init?.signal ?? undefined;
    if (signal?.aborted) return Promise.reject(abortError());
    if (!e) {
      const ctl = new AbortController();
      const key = noStore ? ns : url;
      const { signal: _gone, ...rest } = init ?? {};
      const entry: Entry = { ctl, askers: 0, p: undefined as unknown as Promise<Response> };
      entry.p = inner(url, { ...rest, signal: ctl.signal });
      const done = () => { if (air.get(key) === entry) air.delete(key); };
      entry.p.then(done, done);
      air.set(key, entry);
      e = entry;
    }
    const entry = e;
    entry.askers++;
    return new Promise<Response>((ok, fail) => {
      let over = false;
      const left = () => { if (over) return false; over = true; signal?.removeEventListener("abort", cut); return true; };
      const cut = () => { if (left()) { if (--entry.askers === 0) entry.ctl.abort(); fail(abortError()); } };
      signal?.addEventListener("abort", cut, { once: true });
      entry.p.then(
        (res) => { if (left()) { entry.askers--; ok(res.clone()); } },
        (err) => { if (left()) { entry.askers--; fail(err); } });
    });
  }) as typeof fetch;
}
