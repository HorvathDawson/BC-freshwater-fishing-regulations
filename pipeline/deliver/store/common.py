"""What the store's builder and decoder share: serialisation, the blob codecs, the digests, and the
export subset the store carries."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import zlib
from typing import Dict, Iterable, Optional

from pipeline.deliver.store import FORMAT  # noqa: F401  (re-exported)


class StoreError(Exception):
    """A store that cannot be built or read exactly — never written, never trusted."""


# --------------------------------------------------------------------------------------------
# Serialisation: the answers CLI's (`answers.cli._raw`), value by value
# --------------------------------------------------------------------------------------------

def dumps(v) -> bytes:
    """One JSON value, compact, key order kept — the bytes the answers file holds for it."""
    return json.dumps(v, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def dumps_s(v) -> str:
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def answers_file_bytes(wire: dict) -> bytes:
    """`ui-rules-answers.json` exactly as `pipeline.deliver.answers.cli` writes it."""
    return dumps(wire) + b"\n"


def canonical(v) -> bytes:
    """Order-free bytes of a value (for digests of by-value content)."""
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


# --------------------------------------------------------------------------------------------
# Blob codecs (meta.blob_codec)
# --------------------------------------------------------------------------------------------

CODECS = ("json", "deflate", "zdict")
#: Marker bytes: a JSON text never starts with either.
DEFLATED, DEFLATED_DICT = b"\x01", b"\x02"
#: zlib's dictionary window.
ZDICT_MAX = 32768

#: Every blob column the codec applies to: {table: column}.
BLOB_COLUMNS = {
    "static": "j",
    "ladder_verdict": "j", "ladder_frame": "j",
    "rows_decided": "j", "rows_row": "j", "rows_frame": "j",
    "gear_frame": "j",
    "licence_hold": "j", "licence_answer": "j", "licence_doc": "j", "licence_frame": "j",
    "display_frame": "j", "display_rule": "j",
    "item": "x", "xpart": "j", "picker": "j", "runs": "j",
    "rule": "j", "lic": "j", "entry": "j", "ruleset": "j", "lset": "j",
}


def pack(raw: bytes, codec: str, zd: Optional[bytes]) -> bytes:
    """A blob as stored: raw JSON, or deflated (with the table's dictionary) when that is smaller."""
    if codec == "json":
        return raw
    if codec == "zdict" and zd:
        c = zlib.compressobj(9, zlib.DEFLATED, -15, 9, zlib.Z_DEFAULT_STRATEGY, zd)
        out = DEFLATED_DICT + c.compress(raw) + c.flush()
    else:
        c = zlib.compressobj(9, zlib.DEFLATED, -15, 9)
        out = DEFLATED + c.compress(raw) + c.flush()
    return out if len(out) < len(raw) else raw


def unpack(b: bytes, zd: Optional[bytes]) -> bytes:
    if not b:
        raise StoreError("store: an empty blob")
    head = b[:1]
    if head == DEFLATED:
        return zlib.decompressobj(-15).decompress(b[1:])
    if head == DEFLATED_DICT:
        if zd is None:
            raise StoreError("store: a dictionary-deflated blob in a table with no dictionary")
        return zlib.decompressobj(-15, zdict=zd).decompress(b[1:])
    return bytes(b)


def train_zdict(blobs: Iterable[bytes]) -> bytes:
    """A deterministic deflate dictionary for one table: an even sample of its blobs (in id order),
    concatenated, its last `ZDICT_MAX` bytes (zlib reaches the end of a dictionary most cheaply)."""
    blobs = list(blobs)
    if not blobs:
        return b""
    total = sum(len(b) for b in blobs)
    step = max(1, -(-total // ZDICT_MAX))          # ~ZDICT_MAX bytes, spread over the table
    return b"".join(blobs[::step])[-ZDICT_MAX:]


# --------------------------------------------------------------------------------------------
# The store digest: every row of every table but meta's own digest row
# --------------------------------------------------------------------------------------------

def _pk(con: sqlite3.Connection, table: str) -> list:
    cols = con.execute(f"PRAGMA table_info({table})").fetchall()
    pk = sorted((c[5], c[1]) for c in cols if c[5])
    return [n for _, n in pk] or [c[1] for c in cols]


def store_digest(con: sqlite3.Connection) -> str:
    h = hashlib.sha256()
    tables = [r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name")]
    for t in tables:
        h.update(t.encode() + b"\x00")
        order = ", ".join(_pk(con, t))
        where = " WHERE k <> 'store_digest'" if t == "meta" else ""
        for row in con.execute(f"SELECT * FROM {t}{where} ORDER BY {order}"):
            h.update(json.dumps([x.hex() if isinstance(x, bytes) else x for x in row],
                                separators=(",", ":"), ensure_ascii=False).encode() + b"\n")
    return h.hexdigest()[:16]


# --------------------------------------------------------------------------------------------
# The export subset the store carries: what a page reads that the answers lack
# (the consumer page's slice, `pipeline/deliver/answers/reference/page_data.py`: rule text, sources
# and verbatims, licensing records, entries, licences, species, the conduct sentences, the rule and
# licensing sets a part names, and the waters' and parts' export words).
# --------------------------------------------------------------------------------------------

RULE_KEEP = ("entry_id", "rule_id", "type", "family", "dimension", "label", "parts", "verbatim",
             "fields", "binds", "not_yet_mapped")
RULE_PROV_KEEP = ("rank", "who", "authority", "binds_to", "uncertain", "why", "entry_name")
LIC_KEEP = ("entry_id", "record_id", "kind", "label", "verbatim", "fields", "placement", "period",
            "stamp_waiver", "records", "not_yet_mapped", "parts")
LIC_PROV_KEEP = ("entry_name", "uncertain", "why")
ENTRY_KEEP = ("kind", "name", "full_name", "pages", "scope_note", "mus")
WATER_KEEP = ("name", "kind", "sections", "outside_bc", "part_of", "entries", "steelhead",
              "steelhead_source", "steelhead_rules", "tidal", "divided_into")
PART_KEEP = ("ruleset", "licensing_set", "sections", "runs", "anadromous_rainbow", "steelhead",
             "steelhead_rules", "province_except", "home_region")


def slim(d: dict, keep) -> dict:
    return {k: d[k] for k in keep if k in d}


def rule_words(r: dict) -> dict:
    return {**slim(r, RULE_KEEP), "prov": slim(r["provenance"], RULE_PROV_KEEP)}


def lic_words(l: dict) -> dict:
    return {**slim(l, LIC_KEEP), "prov": slim(l["provenance"], LIC_PROV_KEEP)}


def rule_id_of(r: dict) -> str:
    return f"{r['entry_id']}::{r['rule_id']}"


def lic_id_of(l: dict) -> str:
    return f"{l['entry_id']}#{l['record_id']}"


def export_subset(X: dict) -> dict:
    """The export words the store carries, from the DECODED export (`export_codec.expand`)."""
    return {
        "rule_ids": list(X["rules"]),
        "licensing_ids": list(X["licensing"]),
        "rules": {k: rule_words(r) for k, r in X["rules"].items()},
        "licensing": {k: lic_words(l) for k, l in X["licensing"].items()},
        "entries": {e: slim(v, ENTRY_KEEP) for e, v in X["entries"].items()},
        "licences": X["licences"],
        "species": X["species"],
        "conduct": {k: v["means"] if isinstance(v, dict) else v
                    for k, v in X["guide"]["gear"]["conduct"]["acts"].items()},
        "rulesets": X["rulesets"],
        "licensing_sets": X["licensing_sets"],
        "waters": {wid: {**slim(w, WATER_KEEP), "parts": [slim(p, PART_KEEP) for p in w["parts"]]}
                   for wid, w in X["waters"].items()},
    }


#: The export subset's static (whole-value) members.
EXPORT_STATIC = ("licences", "species", "conduct")

#: answers keys row (`encode.keys`) steelhead codes -> the bundle's (`part.steelhead`).
STEELHEAD_CODE: Dict[str, int] = {"known": 1, "possible": 2}
STEELHEAD_NAME = {v: k for k, v in STEELHEAD_CODE.items()}

#: section_akey geometry.
BLOCK_BITS = 12
BLOCK = 1 << BLOCK_BITS
