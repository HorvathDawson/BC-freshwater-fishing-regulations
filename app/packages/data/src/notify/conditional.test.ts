/**
 * (T741) `conditionalJson`: the background check's manifest and version file by conditional GET, exactly as fresh as a
 * whole read (the cases are the module's header).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { setFailureSink } from "@app/core";
import { conditionalJson, type HeldCopy } from "./conditional";

const URL_ = "http://x/release/manifest.json";
let undo: (() => void) | null = null;
afterEach(() => { undo?.(); undo = null; vi.restoreAllMocks(); });
const reports = () => { const got: string[] = []; undo = setFailureSink((e) => got.push(String(e))); return got; };
const memHeld = (text: string | null = null): HeldCopy & { text: string | null } => {
  const h = { text, read: () => h.text, write: (t: string) => { h.text = t; } };
  return h;
};
/** an origin with one object; the edge weakens its ETag, and a matching STRONG If-None-Match is answered 304 */
function origin(o: { body: unknown; etag: string | null; status?: number }) {
  const asked: (string | null)[] = [];
  const f = (async (_u: string, init?: RequestInit) => {
    const inm = new Headers(init?.headers).get("If-None-Match");
    asked.push(inm);
    if (inm && o.etag && inm === o.etag.replace(/^W\//, "")) return new Response(null, { status: 304 });
    return new Response(JSON.stringify(o.body), { status: o.status ?? 200, headers: o.etag ? { ETag: o.etag } : {} });
  }) as typeof fetch;
  return { f, asked };
}
const A = { data_release: "r-a" }, B = { data_release: "r-b" };

describe("(T741) conditionalJson", () => {
  it("unchanged: kept after a whole read; the next read sends the strong validator and answers the kept copy on 304", async () => {
    const srv = origin({ body: A, etag: 'W/"1"' }), held = memHeld();
    expect(await conditionalJson(URL_, srv.f, held)).toEqual(A);
    expect(await conditionalJson(URL_, srv.f, held)).toEqual(A);
    expect(srv.asked).toEqual([null, '"1"']);
    expect(JSON.parse(held.text!)).toEqual({ url: URL_, etag: 'W/"1"', value: A });
  });

  it("changed: the validator no longer matches, the new body is read whole and kept", async () => {
    const o = { body: A, etag: '"1"' }, srv = origin(o), held = memHeld();
    await conditionalJson(URL_, srv.f, held);
    o.body = B; o.etag = '"2"';
    expect(await conditionalJson(URL_, srv.f, held)).toEqual(B);
    expect(JSON.parse(held.text!).etag).toBe('"2"');
  });

  it("no validator: every read is whole and nothing is kept, and it is said", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => {});
    const srv = origin({ body: A, etag: null }), held = memHeld();
    await conditionalJson(URL_, srv.f, held);
    await conditionalJson(URL_, srv.f, held);
    expect(srv.asked).toEqual([null, null]);
    expect(held.text).toBe("");
    expect(info).toHaveBeenCalledWith(`[held] ${URL_}: no ETag in the answer; the next check reads the whole file again`);
  });

  it("a body the reader refuses throws and is never kept; a kept copy it refuses is reported and read whole", async () => {
    const got = reports();
    const refuse = (v: unknown) => { if ((v as { data_release?: unknown }).data_release !== "r-a") throw new Error("not this app's manifest"); };
    const bad = origin({ body: B, etag: '"2"' }), held = memHeld();
    await expect(conditionalJson(URL_, bad.f, held, refuse)).rejects.toThrow("not this app's manifest");
    expect(held.text).toBeNull();
    const good = origin({ body: A, etag: '"1"' });
    const kept = memHeld(JSON.stringify({ url: URL_, etag: '"1"', value: B }));
    expect(await conditionalJson(URL_, good.f, kept, refuse)).toEqual(A);
    expect(good.asked).toEqual([null]);
    expect(got).toEqual([`Error: kept copy of ${URL_} is refused (not this app's manifest); reading it whole`]);
  });

  it("a kept copy that is not JSON or is another url's: no conditional header, a whole read", async () => {
    const got = reports();
    const srv = origin({ body: A, etag: '"1"' });
    expect(await conditionalJson(URL_, srv.f, memHeld("{nope"))).toEqual(A);
    expect(await conditionalJson(URL_, srv.f, memHeld(JSON.stringify({ url: "http://y", etag: '"1"', value: B })))).toEqual(A);
    expect(srv.asked).toEqual([null, null]);
    expect(got).toHaveLength(1);
    expect(got[0]).toMatch(new RegExp(`^Error: kept copy of ${URL_.replace(/[./]/g, "\\$&")} is not JSON \\(.+\\); reading it whole$`));
  });

  it("an HTTP failure and a stray 304 throw as the plain read did; the fetch's own error (offline) passes through", async () => {
    await expect(conditionalJson(URL_, origin({ body: A, etag: '"1"', status: 503 }).f, memHeld())).rejects.toThrow(`${URL_}: HTTP 503`);
    const stray = (async () => new Response(null, { status: 304 })) as typeof fetch;
    await expect(conditionalJson(URL_, stray, memHeld())).rejects.toThrow(`${URL_}: HTTP 304`);
    const offline = new TypeError("Network request failed");
    await expect(conditionalJson(URL_, (async () => { throw offline; }) as typeof fetch, memHeld())).rejects.toBe(offline);
  });
});
