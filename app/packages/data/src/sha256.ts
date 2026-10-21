/**
 * SHA-256 in plain TypeScript (P10): the one hash every release file is checked against, on every platform. Hermes (the
 * native engine) has no `crypto.subtle`, and a native module for 60 lines of arithmetic is a dependency without a
 * reason. FIPS 180-4; pinned to the standard vectors in `sha256.test.ts`.
 */
const K = new Uint32Array([
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
  private readonly h = new Uint32Array([0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                                        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19]);
  private readonly block = new Uint8Array(64);
  private readonly w = new Uint32Array(64);
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

  private compress(b: Uint8Array, at: number): void {
    const w = this.w, h = this.h;
    for (let i = 0; i < 16; i++) {
      const o = at + 4 * i;
      w[i] = (b[o]! << 24) | (b[o + 1]! << 16) | (b[o + 2]! << 8) | b[o + 3]!;
    }
    for (let i = 16; i < 64; i++) {
      const a = w[i - 15]!, c = w[i - 2]!;
      const s0 = ((a >>> 7) | (a << 25)) ^ ((a >>> 18) | (a << 14)) ^ (a >>> 3);
      const s1 = ((c >>> 17) | (c << 15)) ^ ((c >>> 19) | (c << 13)) ^ (c >>> 10);
      w[i] = (w[i - 16]! + s0 + w[i - 7]! + s1) | 0;
    }
    let a = h[0]!, bb = h[1]!, c = h[2]!, d = h[3]!, e = h[4]!, f = h[5]!, g = h[6]!, k = h[7]!;
    for (let i = 0; i < 64; i++) {
      const S1 = ((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7));
      const t1 = (k + S1 + ((e & f) ^ (~e & g)) + K[i]! + w[i]!) | 0;
      const S0 = ((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10));
      const t2 = (S0 + ((a & bb) ^ (a & c) ^ (bb & c))) | 0;
      k = g; g = f; f = e; e = (d + t1) | 0; d = c; c = bb; bb = a; a = (t1 + t2) | 0;
    }
    h[0] = (h[0]! + a) | 0; h[1] = (h[1]! + bb) | 0; h[2] = (h[2]! + c) | 0; h[3] = (h[3]! + d) | 0;
    h[4] = (h[4]! + e) | 0; h[5] = (h[5]! + f) | 0; h[6] = (h[6]! + g) | 0; h[7] = (h[7]! + k) | 0;
  }
}

/** The SHA-256 of `data`, as 64 lowercase hex digits. */
export function sha256Hex(data: Uint8Array): string {
  return new Sha256().update(data).hex();
}
