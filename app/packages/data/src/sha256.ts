/**
 * SHA-256 in plain TypeScript (P10): the one hash every release file is checked against, on every platform. Hermes (the
 * native engine) has no `crypto.subtle`, and a native module for 60 lines of arithmetic is a dependency without a
 * reason. FIPS 180-4; pinned to the standard vectors in `sha256.test.ts`.
 */
const K = new Int32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);


/**
 * SHA-256 fed in pieces (P11): the native install verifies a 531 MB tile archive by reading it off disk in chunks, which
 * a one-shot hash over a whole `Uint8Array` cannot do on a phone. `sha256Hex` is this class over one piece.
 */
export class Sha256 {
  private readonly h = new Int32Array([0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                                        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19]);
  private readonly block = new Uint8Array(64);
  private readonly w = new Int32Array(64);
  private fill = 0;
  private length = 0;
  private done = false;

  update(data: Uint8Array): this {
    if (this.done) throw new Error("sha256: update after hex()");
    this.length += data.length;
    let i = 0;
    if (this.fill) {
      const take = Math.min(64 - this.fill, data.length);
      this.block.set(data.subarray(0, take), this.fill);
      this.fill += take; i = take;
      if (this.fill < 64) return this;
      this.compress(this.block, 0);
      this.fill = 0;
    }
    for (; i + 64 <= data.length; i += 64) this.compress(data, i);
    if (i < data.length) { this.block.set(data.subarray(i), 0); this.fill = data.length - i; }
    return this;
  }

  /** The digest, as 64 lowercase hex digits. Ends the hash. */
  hex(): string {
    if (!this.done) {
      const bits = this.length * 8, hi = Math.floor(bits / 0x100000000), lo = bits >>> 0;
      const tail = new Uint8Array(this.fill + 9 > 64 ? 128 : 64);
      tail.set(this.block.subarray(0, this.fill), 0);
      tail[this.fill] = 0x80;
      const n = tail.length;
      tail[n - 8] = hi >>> 24; tail[n - 7] = hi >>> 16; tail[n - 6] = hi >>> 8; tail[n - 5] = hi;
      tail[n - 4] = lo >>> 24; tail[n - 3] = lo >>> 16; tail[n - 2] = lo >>> 8; tail[n - 1] = lo;
      for (let off = 0; off < n; off += 64) this.compress(tail, off);
      this.done = true;
    }
    return Array.from(this.h, (x) => (x >>> 0).toString(16).padStart(8, "0")).join("");
  }

  /**
   * (T741) One 64-byte block. The 64 rounds run eight at a time with the working variables RENAMED instead of moved (the
   * classic unrolling: no eight-way shuffle per round), Ch and Maj in their short forms, and every word kept in an
   * Int32Array: an interpreter (Hermes has no JIT) pays per operation, and a Uint32 read above 2^31 is a double.
   */
  private compress(m: Uint8Array, at: number): void {
    const w = this.w, s = this.h;
    for (let i = 0; i < 16; i++) {
      const o = at + 4 * i;
      w[i] = (m[o]! << 24) | (m[o + 1]! << 16) | (m[o + 2]! << 8) | m[o + 3]!;
    }
    for (let i = 16; i < 64; i++) {
      const x = w[i - 15]!, y = w[i - 2]!;
      w[i] = (w[i - 16]! + (((x >>> 7) | (x << 25)) ^ ((x >>> 18) | (x << 14)) ^ (x >>> 3)) + w[i - 7]!
              + (((y >>> 17) | (y << 15)) ^ ((y >>> 19) | (y << 13)) ^ (y >>> 10))) | 0;
    }
    let a = s[0]!, b = s[1]!, c = s[2]!, d = s[3]!, e = s[4]!, f = s[5]!, g = s[6]!, h = s[7]!;
    for (let i = 0; i < 64; i += 8) {
      h = (h + ((((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7)))) + (g ^ (e & (f ^ g))) + K[i + 0]! + w[i + 0]!) | 0;
      d = (d + h) | 0;
      h = (h + ((((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10)))) + ((a & b) ^ (c & (a ^ b)))) | 0;
      g = (g + ((((d >>> 6) | (d << 26)) ^ ((d >>> 11) | (d << 21)) ^ ((d >>> 25) | (d << 7)))) + (f ^ (d & (e ^ f))) + K[i + 1]! + w[i + 1]!) | 0;
      c = (c + g) | 0;
      g = (g + ((((h >>> 2) | (h << 30)) ^ ((h >>> 13) | (h << 19)) ^ ((h >>> 22) | (h << 10)))) + ((h & a) ^ (b & (h ^ a)))) | 0;
      f = (f + ((((c >>> 6) | (c << 26)) ^ ((c >>> 11) | (c << 21)) ^ ((c >>> 25) | (c << 7)))) + (e ^ (c & (d ^ e))) + K[i + 2]! + w[i + 2]!) | 0;
      b = (b + f) | 0;
      f = (f + ((((g >>> 2) | (g << 30)) ^ ((g >>> 13) | (g << 19)) ^ ((g >>> 22) | (g << 10)))) + ((g & h) ^ (a & (g ^ h)))) | 0;
      e = (e + ((((b >>> 6) | (b << 26)) ^ ((b >>> 11) | (b << 21)) ^ ((b >>> 25) | (b << 7)))) + (d ^ (b & (c ^ d))) + K[i + 3]! + w[i + 3]!) | 0;
      a = (a + e) | 0;
      e = (e + ((((f >>> 2) | (f << 30)) ^ ((f >>> 13) | (f << 19)) ^ ((f >>> 22) | (f << 10)))) + ((f & g) ^ (h & (f ^ g)))) | 0;
      d = (d + ((((a >>> 6) | (a << 26)) ^ ((a >>> 11) | (a << 21)) ^ ((a >>> 25) | (a << 7)))) + (c ^ (a & (b ^ c))) + K[i + 4]! + w[i + 4]!) | 0;
      h = (h + d) | 0;
      d = (d + ((((e >>> 2) | (e << 30)) ^ ((e >>> 13) | (e << 19)) ^ ((e >>> 22) | (e << 10)))) + ((e & f) ^ (g & (e ^ f)))) | 0;
      c = (c + ((((h >>> 6) | (h << 26)) ^ ((h >>> 11) | (h << 21)) ^ ((h >>> 25) | (h << 7)))) + (b ^ (h & (a ^ b))) + K[i + 5]! + w[i + 5]!) | 0;
      g = (g + c) | 0;
      c = (c + ((((d >>> 2) | (d << 30)) ^ ((d >>> 13) | (d << 19)) ^ ((d >>> 22) | (d << 10)))) + ((d & e) ^ (f & (d ^ e)))) | 0;
      b = (b + ((((g >>> 6) | (g << 26)) ^ ((g >>> 11) | (g << 21)) ^ ((g >>> 25) | (g << 7)))) + (a ^ (g & (h ^ a))) + K[i + 6]! + w[i + 6]!) | 0;
      f = (f + b) | 0;
      b = (b + ((((c >>> 2) | (c << 30)) ^ ((c >>> 13) | (c << 19)) ^ ((c >>> 22) | (c << 10)))) + ((c & d) ^ (e & (c ^ d)))) | 0;
      a = (a + ((((f >>> 6) | (f << 26)) ^ ((f >>> 11) | (f << 21)) ^ ((f >>> 25) | (f << 7)))) + (h ^ (f & (g ^ h))) + K[i + 7]! + w[i + 7]!) | 0;
      e = (e + a) | 0;
      a = (a + ((((b >>> 2) | (b << 30)) ^ ((b >>> 13) | (b << 19)) ^ ((b >>> 22) | (b << 10)))) + ((b & c) ^ (d & (b ^ c)))) | 0;
    }
    s[0] = (s[0]! + a) | 0; s[1] = (s[1]! + b) | 0; s[2] = (s[2]! + c) | 0; s[3] = (s[3]! + d) | 0;
    s[4] = (s[4]! + e) | 0; s[5] = (s[5]! + f) | 0; s[6] = (s[6]! + g) | 0; s[7] = (s[7]! + h) | 0;
  }
}

/** The SHA-256 of `data`, as 64 lowercase hex digits. */
export function sha256Hex(data: Uint8Array): string {
  return new Sha256().update(data).hex();
}

/** (T287) How much one turn of `sha256HexAsync` hashes: ~20 ms of the JS thread on Hermes (~12 MB/s, T272). */
export const HASH_SLICE = 1 << 18;
const turn = () => new Promise<void>((r) => setTimeout(r, 0));
/** (T287) The test bed's "before" build (`EXPO_PUBLIC_T287_BEFORE=1`): every hash and inflate in one go, as before. */
export const oneShot = (): boolean => (globalThis as { __hashOneShot?: boolean }).__hashOneShot === true;

/**
 * (T287) `sha256Hex` that never holds the JS thread for long: a slice at a time, the screen's turn between. A release
 * file is up to ~6 MB; hashed in one go on a phone that is a ~0.5 s freeze, felt as the app hanging when a download
 * finishes. Same answer as `sha256Hex`.
 */
export async function sha256HexAsync(data: Uint8Array, slice = HASH_SLICE): Promise<string> {
  if (oneShot()) return sha256Hex(data);
  const h = new Sha256();
  for (let at = 0; at < data.length; at += slice) {
    if (at) await turn();
    h.update(data.subarray(at, at + slice));
  }
  return h.hex();
}
