/**
 * (T910, owner 2026-11-19 "yes build all 3 on webapp") THE WEB'S RULES ARE INFLATED OFF THE PAGE'S THREAD. A first visit's
 * CPU profile (4x CPU) had ~0.46 s of fflate on the main thread, all of it `reads.rules()`: 3,404 `rule` + 3,404
 * `display_rule` blobs, raw DEFLATE with each table's preset dictionary, read as soon as the map's waters are looked up.
 * The release files themselves are already gunzipped by the browser (`DecompressionStream`, source.web.ts, T409); these
 * blobs cannot be — `DecompressionStream` has no preset dictionary. So the inflating runs in a worker made from fflate's
 * own UMD build (`fflateWorker.generated.ts`, the same fflate the bundle carries), one message for all the rules; the
 * JSON parsing stays here (`parseBlob`, a slice per turn, T287). Same bytes, same rows, same refusals in the same order
 * as the phone's `decodeRulesHere`.
 *
 * NO SILENT FALL BACK: a browser with no `Worker` (or one that refuses to start it, or a worker that dies) is said and
 * reported (`reportOnce`), and that store's rules are decoded on this thread the phone's way.
 */
import { reportOnce } from "@app/core";
import type { Cell, Row } from "../sql/db";
import { asBytes } from "./errors";
import { parseBlob, RegsBlobError } from "./blob";
import { FFLATE_UMD_B64 } from "./fflateWorker.generated";
import type { StoredRule } from "./reads";
import { checkRuleRow, decodeRulesHere, RULES_SLICE, turnOf, type RulesBase } from "./rulesShared";
import type { ExportRule, RuleFacts } from "@app/core";

/** The worker's own code after fflate's: inflate each item (dictionary 0 none, 1 rule's, 2 display_rule's). */
const HANDLER = `;self.onmessage=function(m){var d=m.data,o=[];for(var i=0;i<d.items.length;i++){var t=d.items[i];` +
  `try{o.push(self.fflate.inflateSync(t[0],t[1]?{dictionary:d.dicts[t[1]-1]}:undefined))}catch(e){o.push(null)}}` +
  `var t=[];o.forEach(function(x){if(x&&t.indexOf(x.buffer)<0)t.push(x.buffer)});self.postMessage({out:o},t)};`;

/** A cell, ready for the worker: what to inflate, or the value / refusal it already is. */
type Slot = { send: number } | { raw: Uint8Array } | { error: unknown };

/** The worker's answer: per item its inflated bytes, null where fflate refused (corrupt data). */
function inflateInWorker(items: readonly (readonly [Uint8Array, number])[], dicts: readonly Uint8Array[]):
    Promise<(Uint8Array | null)[]> {
  return new Promise((resolve, reject) => {
    const url = URL.createObjectURL(new Blob([atob(FFLATE_UMD_B64), HANDLER], { type: "text/javascript" }));
    let w: Worker;
    try { w = new Worker(url); } finally { URL.revokeObjectURL(url); }
    const done = () => w.terminate();
    w.onmessage = (m: MessageEvent<{ out: (Uint8Array | null)[] }>) => { done(); resolve(m.data.out); };
    w.onerror = (e: ErrorEvent) => { done(); reject(new Error(`the rules worker failed: ${e.message || "no message"}`)); };
    w.onmessageerror = () => { done(); reject(new Error("the rules worker's answer could not be read")); };
    w.postMessage({ items, dicts });
  });
}

const NOT_JSON = () => new RegsBlobError("regs store: a blob is not JSON");

export async function decodeRules(rs: readonly Row[], base: RulesBase): Promise<ReadonlyMap<number, StoredRule>> {
  if (typeof Worker === "undefined" || typeof URL.createObjectURL !== "function") {
    reportOnce("regs-rules-worker", new Error("regs rules: this browser has no Web Worker; the rules are inflated on the page's thread"));
    return decodeRulesHere(rs, base);
  }
  const dicts = [base.dict?.("rule") ?? null, base.dict?.("display_rule") ?? null];
  const items: [Uint8Array, number][] = [];
  // each cell as `val` takes it (asBytes, then the head byte: 0x01 raw DEFLATE, 0x02 with the table's dictionary,
  // anything else the JSON itself); a refusal is kept and thrown where the phone's loop would meet it
  const slot = (c: Cell, d: 1 | 2): Slot => {
    try {
      const b = asBytes(c);
      if (b.length === 0) return { error: new RegsBlobError("regs store: an empty blob") };
      if (b[0] === 0x01) return { send: items.push([b.subarray(1), 0]) - 1 };
      if (b[0] === 0x02) {
        if (!dicts[d - 1]) return { error: new RegsBlobError("regs store: a dictionary-deflated blob in a table with no dictionary") };
        return { send: items.push([b.subarray(1), d]) - 1 };
      }
      return { raw: b };
    } catch (e) { return { error: e }; }
  };
  const slots = rs.map((r) => [slot(r.e, 1), slot(r.f, 2)] as const);
  let inflated: (Uint8Array | null)[];
  try {
    inflated = await inflateInWorker(items, dicts.map((d) => d ?? new Uint8Array(0)));   // an absent one is never named
  } catch (e) {
    reportOnce("regs-rules-worker", e instanceof Error ? new Error(`regs rules: ${e.message}; the rules are inflated on the page's thread`) : e);
    return decodeRulesHere(rs, base);
  }
  const value = <T>(s: Slot): T => {
    if ("error" in s) throw s.error;
    if ("raw" in s) return parseBlob(s.raw) as T;
    const b = inflated[s.send];
    if (!b) throw NOT_JSON();
    return parseBlob(b) as T;
  };
  const out = new Map<number, StoredRule>();
  for (let i = 0; i < rs.length; i++) {
    if (i && i % RULES_SLICE === 0) await turnOf();
    checkRuleRow(rs[i]!, i);
    const [e, f] = slots[i]!;
    out.set(i, { rix: i, export: value<ExportRule>(e), facts: value<RuleFacts>(f) });
  }
  return out;
}
