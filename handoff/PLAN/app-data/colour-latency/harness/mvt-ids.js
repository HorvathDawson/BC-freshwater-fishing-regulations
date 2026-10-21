// Colour-latency harness: read ONLY the feature ids of named layers out of an MVT (protobuf), no geometry decode.
// Tile{layers=3} Layer{name=1, features=2} Feature{id=1}. Shared by build-cell-index.mjs (node) and the page.
export function mvtIds(buf, wanted) {
  const b = buf instanceof Uint8Array ? buf : new Uint8Array(buf);
  const out = {};
  let p = 0;
  const varint = () => {
    let n = 0, mul = 1, c;
    do { c = b[p++]; n += (c & 0x7f) * mul; mul *= 128; } while (c & 0x80);
    return n;
  };
  const skip = (wt) => {
    if (wt === 0) varint();
    else if (wt === 1) p += 8;
    else if (wt === 2) { const l = varint(); p += l; }
    else if (wt === 5) p += 4;
    else throw new Error("wire type " + wt);
  };
  while (p < b.length) {
    const tag = varint(), f = tag >> 3, wt = tag & 7;
    if (f !== 3 || wt !== 2) { skip(wt); continue; }
    const llen = varint(); const lend = p + llen;
    let name = null; const feats = [];
    while (p < lend) {
      const t = varint(), lf = t >> 3, lwt = t & 7;
      if (lf === 1 && lwt === 2) { const l = varint(); name = new TextDecoder().decode(b.subarray(p, p + l)); p += l; }
      else if (lf === 2 && lwt === 2) { const l = varint(); feats.push([p, p + l]); p += l; }
      else skip(lwt);
    }
    if (name && wanted.includes(name)) {
      const ids = out[name] ?? (out[name] = []);
      for (const [s, e] of feats) {
        const save = p; p = s;
        while (p < e) {
          const t = varint();
          if (t === 8) { ids.push(varint()); break; }
          skip(t & 7);
        }
        p = save;
      }
    }
    p = lend;
  }
  return out;
}
