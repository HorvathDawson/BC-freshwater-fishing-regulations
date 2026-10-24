/** The regs store's refusal (T03), shared by the store (store.ts) and its reads (reads.ts). */
import type { Cell } from "../sql/db";

export type RefusalWhy = "format" | "codec" | "version" | "digest" | "missing" | "damaged";

/** A release this app will not read, or a read that found it damaged. The UI says "Regulations unavailable". */
export class RegsStoreError extends Error {
  override name = "RegsStoreError";
  constructor(readonly why: RefusalWhy, message: string) { super(message); }
}

/** A blob cell as bytes: Uint8Array as is; an ArrayBuffer wrapped; any other value is a damaged store. */
export function asBytes(c: Cell): Uint8Array {
  if (c instanceof Uint8Array) return c;
  if ((c as unknown) instanceof ArrayBuffer) return new Uint8Array(c as unknown as ArrayBuffer);
  throw new RegsStoreError("damaged", `regs store: a blob column holds ${c === null ? "null" : typeof c} — refused`);
}
