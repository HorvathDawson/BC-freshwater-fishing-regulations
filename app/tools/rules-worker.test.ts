/**
 * (T910) THE WEB'S RULES, INFLATED IN A WORKER (packages/data/src/regs/rulesDecode.web.ts): the same rules, refusals and
 * order as the phone's `decodeRulesHere`; the worker runs the installed fflate (its generated copy is current); a browser
 * without a worker is said and reported, and still gets its rules. The worker is played in-process (node has no Web
 * Worker): its code — the generated fflate plus the handler — is run in a fresh `vm` context, as a Blob worker runs it.
 */
import { afterEach, describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import vm from "node:vm";
import { deflateSync } from "fflate";
import { setFailureSink } from "@app/core";
// @ts-expect-error a plain .mjs tool (no types)
import { FFLATE_WORKER_MODULE, fflateWorkerModule, installedFflate } from "./emit-fflate-worker.mjs";
import { decodeRules } from "../packages/data/src/regs/rulesDecode.web";
import { decodeRulesHere } from "../packages/data/src/regs/rulesShared";
import { unpackBlob } from "../packages/data/src/regs/blob";
import type { Row } from "../packages/data/src/sql/db";

const enc = (v: unknown) => new TextEncoder().encode(JSON.stringify(v));
const DICT = { rule: enc({ species: "rainbow trout", daily: 2, gear: "single barbless hook" }), display_rule: enc({ text: "Release all wild trout" }) };
const blob = (v: unknown, dict: Uint8Array | null) => {
  const raw = deflateSync(enc(v), dict ? { dictionary: dict } : {});
  const out = new Uint8Array(raw.length + 1); out[0] = dict ? 0x02 : 0x01; out.set(raw, 1); return out;
};
const base = { val: <T>(t: string, b: unknown) => unpackBlob(b as Uint8Array, DICT[t as keyof typeof DICT] ?? null) as T,
               dict: (t: string) => DICT[t as keyof typeof DICT] ?? null };
const rows = (n: number): Row[] => Array.from({ length: n }, (_, i) => ({
  rix: i, e: i % 3 ? blob({ rix: i, species: "rainbow trout", daily: i % 5 }, DICT.rule) : i % 2 ? blob({ rix: i }, null) : enc({ rix: i, plain: true }),
  f: blob({ text: `Release all wild trout ${i}` }, DICT.display_rule) }));

/** A Blob worker played in-process: `URL.createObjectURL` keeps the Blob, the Worker runs its text in a fresh context. */
function fakeWorkers() {
  const blobs = new Map<string, Blob>(); let made = 0;
  class FakeWorker {
    onmessage: ((m: { data: unknown }) => void) | null = null; onerror: unknown = null; onmessageerror: unknown = null;
    private ctx: vm.Context;
    private ready: Promise<void>;
    constructor(url: string) {
      made++;
      const b = blobs.get(url); if (!b) throw new Error("no such blob");
      this.ctx = vm.createContext({ Uint8Array, Uint16Array, Int32Array, Array, Error, Math, Object, TextDecoder });
      (this.ctx as { self: unknown }).self = this.ctx;
      (this.ctx as { postMessage: unknown }).postMessage = (data: unknown) => setTimeout(() => this.onmessage?.({ data }));
      this.ready = b.text().then((src) => { vm.runInContext(src, this.ctx); });
    }
    postMessage(data: unknown) { void this.ready.then(() => (this.ctx as { onmessage: (m: unknown) => void }).onmessage({ data: structuredClone(data) })); }
    terminate() {}
  }
  const g = globalThis as { Worker?: unknown };
  const before = { Worker: g.Worker, create: URL.createObjectURL, revoke: URL.revokeObjectURL };
  g.Worker = FakeWorker;
  URL.createObjectURL = (b: Blob) => { const u = `blob:t910/${blobs.size}`; blobs.set(u, b); return u; };
  URL.revokeObjectURL = () => {};
  return { made: () => made, restore: () => { g.Worker = before.Worker; URL.createObjectURL = before.create; URL.revokeObjectURL = before.revoke; } };
}

describe("(T910) the web's rules, inflated in a worker", () => {
  let restore = () => {};
  afterEach(() => restore());

  it("the worker's fflate is the installed fflate's own UMD build (rerun node tools/emit-fflate-worker.mjs)", () => {
    expect(readFileSync(FFLATE_WORKER_MODULE, "utf8")).toBe(fflateWorkerModule(installedFflate()));
  });

  it("decodes every rule exactly as the phone does, in one worker", async () => {
    const w = fakeWorkers(); const said: unknown[] = []; const un = setFailureSink((e) => said.push(e));
    restore = () => { un(); w.restore(); };
    const rs = rows(600);   // more than two slices
    const web = await decodeRules(rs, base);
    expect([w.made(), said]).toEqual([1, []]);                       // the worker did it: nothing fell back
    expect([...web]).toEqual([...(await decodeRulesHere(rs, base))]);
    expect(web.get(4)).toEqual({ rix: 4, export: { rix: 4, species: "rainbow trout", daily: 4 }, facts: { text: "Release all wild trout 4" } });
  });

  it("refuses what the phone refuses, the same words, the first one met", async () => {
    const w = fakeWorkers(); restore = w.restore;
    const same = async (rs: Row[]) => {
      const phone = await decodeRulesHere(rs, base).then(() => null, (e: Error) => `${e.name}: ${e.message}`);
      const web = await decodeRules(rs, base).then(() => null, (e: Error) => `${e.name}: ${e.message}`);
      expect(phone).not.toBeNull();
      expect(web).toBe(phone);
    };
    const corrupt = rows(5); corrupt[3] = { ...corrupt[3]!, e: new Uint8Array([0x01, 0xff, 0x00, 0x13]) };
    await same(corrupt);                                                                  // a blob is not JSON
    const outOfOrder = rows(5); outOfOrder[2] = { ...outOfOrder[2]!, rix: 7 };
    await same(outOfOrder);                                                               // out of sequence at 2
    const noDisplay = rows(5); noDisplay[1] = { ...noDisplay[1]!, f: null };
    await same(noDisplay);                                                                // rule 1 has no display_rule
    const both = rows(6); both[1] = { ...both[1]!, f: new Uint8Array(0) }; both[4] = { ...both[4]!, rix: 9 };
    await same(both);                                                                     // the empty blob comes first
    const noDict = rows(4);
    const bare = { val: base.val, dict: (t: string) => (t === "rule" ? null : base.dict(t)) };
    const phone = await decodeRulesHere(noDict, { val: <T>(t: string, b: unknown) => unpackBlob(b as Uint8Array, bare.dict(t)) as T })
      .then(() => null, (e: Error) => e.message);
    expect(await decodeRules(noDict, { ...bare, val: <T>(t: string, b: unknown) => unpackBlob(b as Uint8Array, bare.dict(t)) as T })
      .then(() => null, (e: Error) => e.message)).toBe(phone);
  });

  it("a browser with no worker: said and reported, and the rules still decoded (on the page's thread)", async () => {
    const g = globalThis as { Worker?: unknown }; const had = g.Worker; delete g.Worker;
    const said: unknown[] = []; const un = setFailureSink((e) => said.push(e));
    restore = () => { un(); g.Worker = had; };
    const rs = rows(10);
    expect([...(await decodeRules(rs, base))]).toEqual([...(await decodeRulesHere(rs, base))]);
    expect(said.map(String)).toEqual([expect.stringContaining("this browser has no Web Worker")]);
  });

  it("the web half is the only one with a worker; the phone's is the T287 loop", () => {
    const native = readFileSync(new URL("../packages/data/src/regs/rulesDecode.native.ts", import.meta.url), "utf8");
    expect(native).toMatch(/return decodeRulesHere\(rs, base\);/);
    expect(native).not.toMatch(/Worker|fflateWorker/);
  });
});
