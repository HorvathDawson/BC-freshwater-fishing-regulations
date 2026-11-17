"""Decode a release's salmon head + shards (salmon/4, salmon-shard/1) — the app's own format, read independently.
Usage as a module: load(release_dir) -> (payload, chain_of: dict sid->chain)."""
import gzip, json, struct, sys
from pathlib import Path

def _head(b):
    assert b[:4] == b"BCSA" and b[4] == 4, "not salmon/4"
    n = struct.unpack("<I", b[13:17])[0]
    assert 17 + n == len(b)
    return json.loads(b[17:17 + n].decode()), b[5:13].hex()

def _shard(b):
    assert b[:4] == b"BCSR" and b[4] == 1
    k = b[13]; region = b[14:14 + k].decode(); i = 14 + k
    def varint():
        nonlocal i
        v, mul = 0, 1
        while True:
            x = b[i]; i += 1
            v += (x & 0x7f) * mul
            if not x & 0x80: return v
            mul *= 128
    count = varint(); end = 0; runs = []
    for _ in range(count):
        s0 = end + varint(); c = varint(); ch = varint()
        runs.append((s0, c, ch)); end = s0 + c
    assert i == len(b)
    return region, runs

def load(rel: Path):
    m = json.loads((rel / "manifest.json").read_text())["files"]
    base = rel.parent
    payload, dig = _head(gzip.decompress((base / m["salmon"]["url"]).read_bytes()))
    chain_of = {}
    for k, v in m.items():
        if k.startswith("salmon-"):
            region, runs = _shard(gzip.decompress((base / v["url"]).read_bytes()))
            for s0, c, ch in runs:
                for s in range(s0, s0 + c):
                    chain_of.setdefault(s, ch)   # first slice holding it wins (the app's order: manifest order)
    return payload, chain_of, dig

def at(runs, day):
    v = runs[1] if len(runs) > 1 else -1
    for k in range(0, len(runs), 2):
        if runs[k] > day: break
        v = runs[k + 1]
    return v
