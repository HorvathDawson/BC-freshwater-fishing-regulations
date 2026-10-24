"""THE ONE FINGERPRINT MODULE: what a reviewer's mark is held against (curation-review PLAN §4
phase 1, T97). The review tool, migrations (`carry.carry_marks`, the app's split rename) and the
tests call these functions (ingest's `--replace-entry` diff is to call them too: T135); nothing
else hashes an entry for review.

A mark used to hash an entry's raw JSON, so a migration that kept every meaning made it stale, and a
split moved under a rule left it "verified". A mark now holds FOUR fingerprints, each over one
question the reviewer answered, so a change says which question it reopens:

    text      the book's row as stored: raw `name`, `regs_verbatim`, `source_pages`, `symbols`.
              -> "book text changed"
    meaning   per record (each rule, each licensing record, and the entry's own fields): a
              CANONICAL dump of what it says — the model's own stored form, record by record (what
              `io.dump_entry` writes), so key order and spelled-out defaults are no change —
              without `label` (served),
              `review_reason` (the parser's flag), its id, and its PLACEMENT fields.
              -> "rule changed"
    binding   per record, where it lands, from the curated inputs: the matched waters (folded through
              the registry's absorbed-id map, the corpus' one read point), the placement fields, every
              split it names AND every split on its waters — each as its canonical anchor JSON
              (`splits.split_anchor`), never coordinates resolved by a build (the 102 confluence anchors
              have none) — and the matcher override that decides the row's water.
              Plus `reach`, its bundle half: per named water the bound main-stem km (0.1 km), and
              the count of bound sections in no named water. NEVER SECTION IDS: the atlas is not
              deterministic, and a section id may never leave the bundle (AGENTS 5).
              -> "binding moved"
    answer    per record, from the bundle: the rule's `parts` and `label`, and for the entry the
              steelhead/tidal/province facts of each of its waters' parts — the backstop for what
              overrides, the steelhead list and the registry change without touching the entry.
              -> "answer changed"

A record with no bundle copy now, whose mark had one, reads "awaiting build".

Determinism: every hash is SHA-256 over sorted-key compact JSON, first 16 hex.
"""
from __future__ import annotations

from pipeline.common.entry_id import is_table, mus_of
import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Iterable, Mapping

from pydantic import ValidationError

#: Rule / licensing fields that say WHERE, not WHAT — they go to `binding`, never `meaning`, so a
#: placement edit reads "binding moved" and a split rename (an id in `extents`) is a binding change.
PLACEMENT_FIELDS = frozenset({
    "extents", "includes_tributaries", "tributaries_only", "tributary_excludes",
    "unresolved_locators", "extent_text", "undrawn_part", "side",
})
#: Fields no reviewer answers for: the served label, the parser's flag, and the record's own id
#: (rule identity is the id PLUS the meaning, `marks.match_records`).
NOT_MEANING = frozenset({"label", "review_reason", "rule_id", "id"})
#: The entry's own fields that are part of the BOOK TEXT (`text`).
TEXT_FIELDS = ("name", "regs_verbatim", "source_pages", "symbols")
#: The entry's own fields that place it (its pseudo-record "entry" binding).
ENTRY_PLACEMENT = ("matched", "extents", "includes_tributaries", "tidal")
#: A rule's fields that NAME ANOTHER RULE OF ITS ENTRY by `rule_id` (130 `within`, 6 `condition_of`,
#: 3 `derived_from`; and `siblings` inside an extent). A fingerprint holds the NAMED RULE'S MEANING in
#: place of its id (`_deref`), so a re-parse that renumbers the rules moves no mark: an id is not
#: what the reviewer answered for (T97 adversarial review).
RULE_REF_FIELDS = ("within", "condition_of", "derived_from")


def digest(obj) -> str:
    """SHA-256 (16 hex) of `obj` as sorted-key compact JSON — every fingerprint is one of these."""
    blob = json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


# --------------------------------------------------------------------------------------------
# The bundle's half: read once per bundle
# --------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class BundleFacts:
    """What the bundle says per (entry_id, rule_id) and per water — the inputs of `reach` and
    `answer`. Built by `BundleFacts.read`; a test builds a fixture bundle and reads it the same way."""
    rule_answer: Mapping[tuple[str, str], tuple[str, str]] = field(default_factory=dict)
    rule_reach: Mapping[tuple[str, str], tuple] = field(default_factory=dict)
    licensing_label: Mapping[tuple[str, str], str] = field(default_factory=dict)
    water_parts: Mapping[str, tuple] = field(default_factory=dict)
    entries: frozenset = frozenset()

    @classmethod
    def read(cls, db_path: str | Path) -> "BundleFacts":
        """Read the facts of EVERY rule from a bundle (read-only). Raises on a missing file."""
        p = Path(db_path)
        if not p.exists():
            raise FileNotFoundError(f"no bundle at {p} — `python -m pipeline.deliver` makes it")
        db = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        try:
            return cls(rule_answer={(e, r): (lab, parts) for e, r, lab, parts in db.execute(
                           "SELECT entry_id, rule_id, label, parts FROM rule")},
                       rule_reach=_rule_reaches(db), licensing_label=_licensing_labels(db),
                       water_parts=_water_parts(db),
                       entries=frozenset(e for (e,) in db.execute("SELECT entry_id FROM entry")))
        finally:
            db.close()


def _rule_reaches(db) -> dict[tuple[str, str], tuple]:
    """(entry_id, rule_id) -> `reach_summary` of the sections the bundle binds it to. Per rule SET
    first (2 thousand sets over 2 million sections), then each rule adds up the sets it is in:
    joining rules to sections directly took two minutes."""
    by_set: dict[int, list] = {}
    for set_id, item, n, metres in db.execute(
            "SELECT s.set_id, COALESCE(i.item_id, ''), COUNT(*), "
            "SUM(CASE WHEN sp.lo_m IS NOT NULL AND sp.off_stem = 0 "
            "THEN sp.hi_m - sp.lo_m ELSE 0 END) "
            "FROM section_ruleset s LEFT JOIN item_section x ON x.sid = s.sid "
            "LEFT JOIN item i ON i.ord = x.ord LEFT JOIN section_span sp ON sp.sid = s.sid "
            "GROUP BY s.set_id, i.item_id"):
        by_set.setdefault(set_id, []).append((item, n, metres or 0))
    reach: dict[tuple[str, str], dict] = {}
    for set_id, e, r in db.execute("SELECT set_id, entry_id, rule_id FROM ruleset"):
        acc = reach.setdefault((e, r), {})
        for item, n, metres in by_set.get(set_id, ()):
            had = acc.get(item, (0, 0))
            acc[item] = (had[0] + n, had[1] + metres)
    return {k: reach_summary(v) for k, v in reach.items()}


def rule_section_counts(db, entry_id: str) -> dict[str, int]:
    """THE BUNDLE'S ANSWER TO "how many sections does each rule of this entry bind": rule_id ->
    distinct sections over every rule set it is in. One query, read by the review tool's "bundle
    matches" (`vintage.bundle_matches`) and its bundle panel (`answer.entry_in_bundle`)."""
    return dict(db.execute(
        "SELECT r.rule_id, COUNT(DISTINCT s.sid) FROM ruleset r JOIN section_ruleset s "
        "ON s.set_id = r.set_id WHERE r.entry_id = ? GROUP BY r.rule_id", (entry_id,)))


def _licensing_labels(db) -> dict[tuple[str, str], str]:
    lic: dict[tuple[str, str], str] = {}
    for table, idcol in (("designation", "designation_id"), ("not_classified", "not_classified_id"),
                         ("requirement", "req_id"), ("alternative", "alternative_id")):
        for e, rid, lab in db.execute(f"SELECT entry_id, {idcol}, label FROM {table}"):
            lic[(e, str(rid))] = lab
    return lic


def _water_parts(db) -> dict[str, tuple]:
    """item_id -> the facts of each of its parts AS THE BUNDLE WROTE THEM (`part`; read, never
    regrouped — the partition is the bundle builder's, dataflow gate 3)."""
    parts: dict[str, set] = {}
    for item, *row in db.execute(
            "SELECT i.item_id, p.province_except, p.steelhead_water, p.steelhead, "
            "p.steelhead_rules, p.home_regions, p.tidal FROM part p JOIN item i ON i.ord = p.ord"):
        parts.setdefault(item, set()).add(tuple(row))
    return {k: tuple(sorted(v, key=repr)) for k, v in parts.items()}


def reach_summary(by_item: Mapping[str, tuple[int, float]]) -> tuple:
    """A rule's bound sections, as the binding fingerprint holds them: per NAMED water its bound
    main-stem length to 0.1 km (or "polygon" when it has no stem span — a lake), and the COUNT of
    bound sections in no named water. Not a total section count: a split cuts one section into two
    without moving anything, and must not stale every rule that walks past it."""
    out = []
    for item, (n, metres) in sorted(by_item.items()):
        if item == "":
            out.append(("", int(n)))
        else:
            out.append((item, round(metres / 1000.0, 1) if metres else "polygon"))
    return tuple(out)


# --------------------------------------------------------------------------------------------
# The context: every input outside the entry
# --------------------------------------------------------------------------------------------

@dataclass(eq=False)
class Context:
    """The inputs a fingerprint reads beside the entry. `splits` is splits.json as a dict;
    `overrides` the matcher overrides (list); `registry` (optional) folds absorbed ids and gives
    each matched item its ref_ids; `bundle` (optional) the built facts. Build one per vintage and
    reuse it: its indexes are computed once."""
    splits: Mapping = field(default_factory=dict)
    overrides: tuple = ()
    registry: Mapping | None = None
    bundle: BundleFacts | None = None

    @classmethod
    def on_disk(cls, splits_path: str | Path, overrides_path: str | Path | None,
                registry: "str | Path | Mapping", bundle_path: str | Path) -> "Context":
        """THE VINTAGE ON DISK as a context: splits.json, the matcher overrides, the served registry
        and the bundle's facts (no bundle yet is a state, not an error: `bundle` None). The review
        app (`reuse._review_context`) and the CLIs that judge marks (`precheck`, `tickets`) read
        it here, so a mark's status is the same wherever it is asked (T135 adversarial review).
        `registry`: its path, or the registry already loaded (the app's own, cached)."""
        import json
        from pipeline.atlas.registry import load_registry
        from pipeline.regs.matching.matcher import load_overrides
        ov = Path(overrides_path) if overrides_path else None
        return cls(splits=json.loads(Path(splits_path).read_text(encoding="utf-8")),
                   overrides=tuple(load_overrides(ov if ov and ov.exists() else None)),
                   registry=(registry if isinstance(registry, Mapping)
                             else load_registry(Path(registry))),
                   bundle=BundleFacts.read(bundle_path) if Path(bundle_path).exists() else None)

    @cached_property
    def split_index(self) -> dict[str, dict]:
        """split id -> the split, carrying its waterbody's `applies_to` beside its own
        (`splits.split_index`)."""
        from pipeline.atlas.splits.splits import split_index
        return split_index(self.splits)

    @cached_property
    def override_index(self):
        from pipeline.regs.matching.matcher import build_override_index
        return build_override_index(list(self.overrides))


def _anchor_digest(split: Mapping) -> str:
    """A split's canonical anchor as a digest — `splits.anchor_digest`, the atlas's own (the atlas
    records it per split, `splits.ANCHORS_FILE`, so the two can never disagree about a cut)."""
    from pipeline.atlas.splits.splits import anchor_digest
    return anchor_digest(dict(split))


def _split_ids(obj) -> set[str]:
    """Every split id an extent list (any depth) binds."""
    out: set[str] = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "splits" and isinstance(v, list):
                out.update(str(x) for x in v)
            else:
                out |= _split_ids(v)
    elif isinstance(obj, list):
        for x in obj:
            out |= _split_ids(x)
    return out


# --------------------------------------------------------------------------------------------
# The four fingerprints
# --------------------------------------------------------------------------------------------

def _terse(model, raw):
    """One record as the model STORES it — `exclude_defaults`, `by_alias`, a licensing record's
    `kind` kept (what `io.dump_entry` writes for it; measured equal over the whole corpus) — or,
    when the model refuses it, as it is less its defaulted fields, so an entry that fails the gate
    is fingerprinted in the same spelling as one that passes."""
    from pydantic import TypeAdapter
    from pipeline.regs.parsing import io
    try:
        ta = model if isinstance(model, TypeAdapter) else TypeAdapter(model)
        obj = ta.validate_python(raw)
        full = json.loads(ta.dump_json(obj, by_alias=True))
        terse = json.loads(ta.dump_json(obj, by_alias=True, exclude_defaults=True))
        io._restore_kind(terse, full)
        return terse
    except (ValidationError, TypeError, ValueError):
        return json.loads(json.dumps(raw, default=str))


def _without_defaults(model_cls, data: Mapping) -> dict:
    """`data` less every field spelled out at its model default (`tidal: false`, `see: []`)."""
    out = {}
    for k, v in data.items():
        f = model_cls.model_fields.get(k)
        if f is not None and not f.is_required() and v == f.get_default(call_default_factory=True):
            continue
        out[k] = v
    return out


def canonical_entry(entry: Mapping, ctx: Context | None = None) -> dict:
    """The entry in the model's stored spelling, RECORD BY RECORD (each rule through
    `CatalogueRule`, each licensing record through its union), absorbed ids folded through the
    registry (`flowing.canonical_ids`), served labels dropped. Per record, so an entry the gate
    refuses as a whole (a quote not in its verbatim) keeps the spelling of its other records:
    a mark is refused on such an entry (`marks.refuse_on_gate`), but its old marks must still
    read the right reason."""
    from pipeline.regs.parsing.catalogue import CatalogueEntry, CatalogueRule
    data = dict(entry)
    data.pop("label", None)
    for key in ("rules", "licensing"):
        if isinstance(data.get(key), list):
            data[key] = [{k: v for k, v in x.items() if k != "label"} if isinstance(x, dict)
                         else x for x in data[key]]
    if ctx is not None and ctx.registry is not None:
        from pipeline.atlas.registry.flowing import canonical_ids
        data = canonical_ids(data, ctx.registry)
    lic_type = CatalogueEntry.model_fields["licensing"].annotation.__args__[0]
    out = _without_defaults(CatalogueEntry, {k: v for k, v in data.items()
                                             if k not in ("rules", "licensing")})
    out = json.loads(json.dumps(out, default=str))
    out["rules"] = [_terse(CatalogueRule, r) for r in data.get("rules") or []]
    out["licensing"] = [_terse(lic_type, x) for x in data.get("licensing") or []]
    return out


def _deref(record: Mapping, named: Mapping[str, str]) -> dict:
    """`record` with every rule id it names (`RULE_REF_FIELDS`, an extent's `siblings`) replaced by
    `{"rule": named[id]}` — the named rule's meaning with ITS OWN references blanked (`entry_records`),
    so no id survives at any depth and a cycle cannot recurse. An id naming no rule of the entry
    stays as it is."""
    def ref(v):
        return {"rule": named[v]} if isinstance(v, str) and v in named else v

    def ext(v):
        if not isinstance(v, list):
            return v
        return [dict(x, siblings=[ref(s) for s in x["siblings"]])
                if isinstance(x, dict) and isinstance(x.get("siblings"), list) else x for x in v]
    out = dict(record)
    for k in RULE_REF_FIELDS:
        if k in out:
            out[k] = ref(out[k])
    for k in ("extents", "tributary_excludes"):
        if k in out:
            out[k] = ext(out[k])
    return out


def text_fp(entry: Mapping) -> str:
    """The book's row as stored: raw `name`, `regs_verbatim`, `source_pages`, `symbols`."""
    return digest({k: entry.get(k) for k in TEXT_FIELDS})


def meaning_fp(record: Mapping) -> str:
    """What one rule or licensing record SAYS: its canonical fields without placement, label,
    review flag or id."""
    return digest({k: v for k, v in record.items()
                   if k not in PLACEMENT_FIELDS and k not in NOT_MEANING})


def _splits_named(ids: Iterable[str], ctx: Context) -> list:
    idx = ctx.split_index
    return [(sid, _anchor_digest(idx[sid]) if sid in idx else None) for sid in sorted(ids)]


def _water_refs(matched: Iterable[str], ctx: Context) -> set[str]:
    refs: set[str] = set()
    for iid in matched:
        refs.add(iid)
        it = (ctx.registry or {}).get(iid) if ctx.registry is not None else None
        if it is not None:
            refs.update(getattr(it, "ref_ids", ()) or ())
    return refs


def _splits_on_waters(entry: Mapping, ctx: Context) -> list[dict]:
    from pipeline.atlas.splits.splits import splits_on
    return splits_on(ctx.splits or {}, _water_refs(entry.get("matched") or [], ctx))


def entry_split_ids(entry: Mapping, ctx: Context) -> set[str]:
    """EVERY SPLIT THE ENTRY'S BINDING READS — the splits its binding fingerprints hold: each one
    on its matched waters (`entry_binding_fp`) and each one its own extents or any rule's or
    licensing record's placement names (`record_binding_fp`). What the review tool asks the
    served atlas about before it calls the entry built (`vintage.entry_state`, T133)."""
    out = {str(sp["id"]) for sp in _splits_on_waters(entry, ctx) if sp.get("id")}
    out |= _split_ids(entry.get("extents"))
    for key in ("rules", "licensing"):
        for x in entry.get(key) or []:
            if isinstance(x, dict):
                out |= _split_ids(_placed(x, PLACEMENT_FIELDS))
    return out


def entry_binding_fp(entry: Mapping, ctx: Context) -> str:
    """Where the ENTRY lands: its matched waters, its clip and tributary scope, `tidal`, every
    authored split on its waters (canonical anchors), and the matcher override deciding its row."""
    from pipeline.regs.matching.matcher import override_for
    placed = _placed(entry, ENTRY_PLACEMENT)
    on_waters = _splits_on_waters(entry, ctx)
    idx = ctx.split_index
    waters = sorted((sp["id"], _anchor_digest(idx.get(sp["id"], sp))) for sp in on_waters
                    if sp.get("id"))
    named = _splits_named(_split_ids(entry.get("extents")), ctx)
    ov = None
    if ctx.overrides and not is_table(entry.get("entry_id", "")):
        eid = str(entry.get("entry_id", ""))
        mus = list(mus_of(eid))
        got = override_for({"water": entry.get("name", ""), "region": str(entry.get("region", "")),
                            "mu": mus}, ctx.override_index)
        ov = {k: v for k, v in got.items() if k != "note"} if got else None
    return digest({"placed": placed, "splits_on_waters": waters, "splits_named": named,
                   "override": ov})


def _placed(obj: Mapping, fields) -> dict:
    """The placement fields of `obj`, each EXTENT (`extents`, `tributary_excludes`) less the keys
    spelled out at the `Extent` model's default (`splits: []`), so a writer that spells defaults
    moves nothing. Extents are plain dicts in the catalogue model, so `_terse` cannot do this."""
    from pipeline.regs.parsing.entry_models import Extent

    def ext(v):
        if isinstance(v, list):
            return [_without_defaults(Extent, x) if isinstance(x, dict) else x for x in v]
        return v
    return {k: ext(obj.get(k)) if k in ("extents", "tributary_excludes") else obj.get(k)
            for k in sorted(fields) if k in obj}


def record_binding_fp(record: Mapping, ctx: Context) -> str:
    """Where one rule or licensing record lands, from its own placement fields and the canonical
    anchor of every split it names. (The entry's own placement is the "entry" record's.)"""
    placed = _placed(record, PLACEMENT_FIELDS)
    return digest({"placed": placed, "splits": _splits_named(_split_ids(placed), ctx)})


def rule_reach_fp(entry_id: str, rule_id: str, ctx: Context) -> str | None:
    """The bundle half of a rule's binding (`reach_summary`), or None when the bundle has no
    copy of the rule (or there is no bundle)."""
    b = ctx.bundle
    if b is None or (entry_id, rule_id) not in b.rule_answer:
        return None
    return digest(b.rule_reach.get((entry_id, rule_id), ()))


def rule_answer_fp(entry_id: str, rule_id: str, ctx: Context) -> str | None:
    """The bundle's `label` and `parts` of the rule, or None when the bundle has no copy."""
    b = ctx.bundle
    if b is None or (entry_id, rule_id) not in b.rule_answer:
        return None
    label, parts = b.rule_answer[(entry_id, rule_id)]
    try:
        parts = json.loads(parts) if isinstance(parts, str) else parts
    except ValueError:
        pass
    return digest({"label": label, "parts": parts})


def entry_answer_fp(entry: Mapping, ctx: Context) -> str | None:
    """The steelhead / tidal / province facts of each part of the entry's waters, from the
    bundle — where a steelhead-list or registry change shows on an entry that did not change."""
    b = ctx.bundle
    if b is None or str(entry.get("entry_id")) not in b.entries:
        return None
    return digest({iid: list(b.water_parts.get(iid, ())) for iid in sorted(entry.get("matched") or [])})


# --------------------------------------------------------------------------------------------
# One entry's fingerprints, as a mark holds them
# --------------------------------------------------------------------------------------------

def entry_records(entry: Mapping, ctx: Context) -> list[dict]:
    """Every record of one entry with its fingerprints: {key, meaning, binding, reach, answer}.
    Keys: "entry" (the entry's own fields), "rule:<rule_id>", "lic:<id>"."""
    canon = canonical_entry(entry, ctx)
    eid = str(canon.get("entry_id", entry.get("entry_id", "")))
    own = {k: v for k, v in canon.items()
           if k not in ("rules", "licensing", "entry_id", "region", *TEXT_FIELDS, *ENTRY_PLACEMENT)}
    out = [{"key": "entry", "meaning": digest(own), "binding": entry_binding_fp(canon, ctx),
            "reach": None, "answer": entry_answer_fp(canon, ctx)}]
    rules = [r for r in canon.get("rules") or [] if isinstance(r, dict)]
    ids = {str(r.get("rule_id")) for r in rules}
    named = {str(r.get("rule_id")): meaning_fp(_deref(r, dict.fromkeys(ids, "")))
             for r in rules}
    for r in rules:
        rid = str(r.get("rule_id"))
        d = _deref(r, named)
        out.append({"key": f"rule:{rid}", "meaning": meaning_fp(d),
                    "binding": record_binding_fp(d, ctx),
                    "reach": rule_reach_fp(eid, rid, ctx), "answer": rule_answer_fp(eid, rid, ctx)})
    for x in canon.get("licensing") or []:
        xid = str(x.get("id"))
        lab = ctx.bundle.licensing_label.get((eid, xid)) if ctx.bundle is not None else None
        out.append({"key": f"lic:{xid}", "meaning": meaning_fp(x),
                    "binding": record_binding_fp(x, ctx), "reach": None,
                    "answer": digest(lab) if lab is not None else None})
    return out


def entry_fingerprints(entry: Mapping, ctx: Context, rule_id: str | None = None) -> dict:
    """The fingerprints a mark on this entry holds: {text, records}. With `rule_id` (a per-rule
    mark, zone entries) only the entry's own record and that rule's."""
    recs = entry_records(entry, ctx)
    if rule_id is not None:
        keep = {"entry", f"rule:{rule_id}"}
        if f"rule:{rule_id}" not in {r["key"] for r in recs}:
            raise KeyError(f"{entry.get('entry_id')} has no rule {rule_id!r}")
        recs = [r for r in recs if r["key"] in keep]
    return {"text": text_fp(entry), "records": recs}


def split_fingerprints(split_id: str, ctx: Context) -> dict:
    """The fingerprints a `split` mark holds: the canonical anchor (binding) and its kind
    (meaning). Raises KeyError for an id splits.json does not have."""
    sp = ctx.split_index.get(split_id)
    if sp is None:
        raise KeyError(f"split {split_id!r} is not in splits.json")
    return {"text": None, "records": [{"key": f"split:{split_id}",
                                       "meaning": digest({"kind": sp.get("kind")}),
                                       "binding": _anchor_digest(sp),
                                       "reach": None, "answer": None}]}
