"""Match a synopsis row to a registry item — the bridge the batch exporter needs to build a row's
constrained parse context, and the first half of Goal 2 (reg → features).

Overrides are the hand-curated truth (archive schema: a LIST of override objects, each with a
`criteria` {name_verbatim, region, mus} and typed FWA ids — gnis_ids / waterbody_keys /
fwa_watershed_codes / blue_line_keys — plus notes / skip / variant_of / admin targets). They are
NEVER deleted and NEVER silently fall back to name matching.

Strategy (conservative — a wrong match is worse than no match):
  1. Override wins. Match the row's water name to an override (region/MU-scoped). Then:
       - `skip`            -> the row is intentionally dropped (in-season correction, not-found, …).
       - typed ids         -> resolve each against the registry's `id_index` (ref_id -> item_id, built
                              from every item's `ref_ids`). Any that hit a NAMED item -> `override`
                              match. Ids that hit NOTHING (a nameless oxbow channel by wsc, an unnamed
                              lake by wbk, an admin/area target, a poly/fid needing an FWA lookup) are
                              carried as `unresolved_ids` for the RESOLVER (Goal 2), which has the full
                              graph — the matcher never guesses them by name.
       - variant_of        -> resolve the curator's CORRECTED name (a legit name lookup of the right
                              name, not a fallback to the wrong one).
  2. Else name matching: index the registry by normalized name + variants (already baked in from the
     graph's name_tuples) and look up the row's water name.
  3. One hit -> matched. Several -> disambiguate by MU (item.mus ∩ row.mus), else region number. Still
     several, or none -> UNMATCHED (reported, hand-curated), never guessed.

Name normalization folds diacritics ('Barrière'->'barriere') and drops apostrophes/parentheticals so the
synopsis spelling and the FWA gazetteer spelling compare equal. It KEEPS the Lake/Creek/River type word.
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from pipeline.common.models import RegistryItem
from pipeline.common.utils.wsc import trim_wsc

_log = logging.getLogger(__name__)

_MU_RE = re.compile(r"\b(\d+)\s*-\s*(\d+)\b")


@dataclass(frozen=True)
class MatchResult:
    index: int
    water: str
    item_id: str | None
    status: str                     # matched | override | ambiguous | unmatched | skip | feature_pin
    reason: str = ""
    candidates: tuple[str, ...] = ()
    also: tuple[str, ...] = ()      # extra NAMED item_ids for a combined-entry override
    via: str = ""                   # override | override_alias | override_skip | override_feature |
                                    # name | name_alias
    unresolved_ids: tuple[str, ...] = ()   # curated typed ids with NO named registry item -> the
                                           # resolver binds these to graph sections (nameless features)
    admin_targets: tuple[str, ...] = ()    # curated admin/area targets (parks/WMA/zones) -> resolver


def _fold(text: str) -> str:
    """Fold accented Latin letters to ASCII ('Barrière'->'Barriere') so both spellings key the same."""
    return "".join(ch for ch in unicodedata.normalize("NFKD", text or "") if not unicodedata.combining(ch))


def _norm(s: str) -> str:
    # Fold diacritics, REMOVE "(...)" spans (a mid-name "(Nanaimo)"/"(Extension)" qualifier, keeping
    # the LAKE/CREEK type word), drop possessive apostrophes, collapse to single spaces.
    s = _fold(s).lower()
    s = re.sub(r"\([^)]*\)", " ", s)
    s = s.replace("'", "").replace("’", "")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def parse_reg_mus(row: dict) -> set[str]:
    """The MUs a synopsis row applies to, from its `mu` field (a string or list, e.g. '1-5' / ['1-5','1-6'])."""
    mu = row.get("mu")
    parts = mu if isinstance(mu, list) else [mu]
    out: set[str] = set()
    for p in parts:
        for a, b in _MU_RE.findall(str(p or "")):
            out.add(f"{a}-{b}")
    return out


def region_num(row: dict) -> str:
    """The management region number from the row's region string ('REGION 1 - ...') or its MU ('1-2')."""
    m = re.search(r"\b(\d+)\b", str(row.get("region") or ""))
    if m:
        return m.group(1)
    for a, _ in _MU_RE.findall(str(row.get("mu") or "")):
        return a
    return ""


def build_name_index(registry: dict[str, RegistryItem]) -> dict[str, list[str]]:
    """normalized name/variant -> [item_id]. Areas are skipped (never a name-match target for a row)."""
    idx: dict[str, list[str]] = {}
    for it in registry.values():
        if it.kind == "area":
            continue
        for n in {it.name, *it.variants}:
            key = _norm(n)
            if key:
                idx.setdefault(key, [])
                if it.id not in idx[key]:
                    idx[key].append(it.id)
    return idx


def build_id_index(registry: dict[str, RegistryItem]) -> dict[str, str]:
    """ref_id (gnis:/wbk:/wsc:/blk:) -> item_id, from every item's `ref_ids`. The bridge that lets a
    curated typed-id override resolve to whatever registry item now owns that FWA id (incl. a lake's
    GNIS_ID_1/2/3).

    SELF-IDENTITY WINS, then first writer. A ref is NOT unique: every side channel that inherited the
    mainstem's name also carries the mainstem's gnis on its name tuples, so `gnis:39325` (the Fraser)
    appears in the ref_ids of all 15 Fraser-channel items. Plain first-writer-wins then handed
    `gnis:39325` to whichever item happened to be built first — Annacis Channel, ONE section — so the
    Fraser overrides pinned the whole river onto a side channel. The item whose OWN id is that ref is
    always the right answer, so it is claimed up front (5 rivers were misdirected this way: Fraser,
    Stave, Columbia, Pitt, Nelson Slough)."""
    idx: dict[str, str] = {iid: iid for iid, it in registry.items() if it.kind != "area"}
    for iid, it in registry.items():
        for r in it.ref_ids:
            idx.setdefault(r, iid)
    return idx


def _item_region_nums(it: RegistryItem) -> set[str]:
    return {m.split("-")[0] for m in it.mus if m}


def _item_mus(it: RegistryItem) -> set[str]:
    return {m for m in it.mus if m}


def load_overrides(path: str | Path | None, *, source: str = "") -> list[dict]:
    """Load the archive-schema overrides (a LIST of override objects). Returns [] if no path/file.

    An entry may declare `"source": "dfo"` to say it answers a question only the DFO SALMON matcher
    asks. Those are EXCLUDED by default — every provincial caller (batch_exporter, reach.covered, the
    matcher CLI) gets the safe set without having to know the field exists — and included only when a
    caller passes `source="dfo"`.

    The alternative was scoping a DFO answer by region so it could not reach a provincial row, and
    that only works while the name happens to be unique in the region. It is not always: Region 5 has
    FIVE gazetted Long Lakes, so a region-scoped "LONG LAKE" entry would bind any future Region-5
    Long Lake row to the Smith Inlet one. The tag says who is asking instead of guessing from scope.
    """
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        return []
    data = json.loads(p.read_text(encoding="utf-8"))
    rows = data if isinstance(data, list) else data.get("overrides", [])
    return [e for e in rows if (e.get("source") or "") in ("", source)]


def build_override_index(overrides: list[dict]) -> dict[str, list[dict]]:
    """normalized name_verbatim -> [override entry]. Several entries can share a name (per region/MU)."""
    idx: dict[str, list[dict]] = defaultdict(list)
    for e in overrides:
        nm = _norm((e.get("criteria") or {}).get("name_verbatim", ""))
        if nm:
            idx[nm].append(e)
    return idx


def _pick_override(entries: list[dict], rn: str, row_mus: set[str]) -> dict | None:
    """The override entry best matching this row's region/MUs. MU overlap > region match > catch-all.

    An override only applies when it POSITIVELY matches the row: MU overlap (3), region match (2), or an
    intentional catch-all with no region/MU scope (1). A scoped override whose region AND MUs both
    conflict with the row (0) is a DIFFERENT water — never apply it (that bound Region-1 'White River'
    to the Region-4 'White River (see also …)' override before this guard)."""
    best, best_score = None, 0
    for e in entries:
        c = e.get("criteria") or {}
        emus = set(c.get("mus", []))
        ereg = re.search(r"\d+", str(c.get("region", "")))
        ereg = ereg.group(0) if ereg else ""
        if emus and emus & row_mus:
            score = 3
        elif ereg and ereg == rn:
            score = 2
        elif not emus and not ereg:
            score = 1
        else:
            score = 0
        if score > best_score:
            best, best_score = e, score
    return best if best_score >= 1 else None


def override_typed_ids(e: dict) -> list[str]:
    """The prefixed FWA ids an override pins (gnis:/wbk:/wsc:/blk:), wsc trimmed to its stable head.
    Poly/fid/ungazetted ids need an FWA lookup and are left to the resolver (not returned here)."""
    ids: list[str] = []
    for g in e.get("gnis_ids", []):
        ids.append(f"gnis:{g}")
    for w in e.get("waterbody_keys", []):
        ids.append(f"wbk:{w}")
    for c in e.get("fwa_watershed_codes", []):
        ids.append(f"wsc:{trim_wsc(str(c))}")
    for b in e.get("blue_line_keys", []):
        ids.append(f"blk:{b}")
    return ids


def _name_resolve(index: int, water: str, lookup: str, via: str, registry, name_index, rn, row_mus) -> MatchResult:
    cands = list(name_index.get(lookup, []))
    if not cands:
        return MatchResult(index, water, None, "unmatched", "no name/variant match", via=via)

    # MU gate (strict): when the reg row names MU(s), a candidate is only a real match if its own MUs
    # share one of them. If EVERY same-named candidate has MUs and none intersect the reg's MU, it's a
    # DIFFERENT water — never guess by region; leave it unmatched so a curator override binds it (e.g.
    # a stream tagged 1-1/1-2/1-3 can't satisfy a reg that names 1-5). Untagged candidates (empty MU
    # column) can't be checked; they're only kept when no MU-conflicting sibling exists, so a lone
    # untagged water still matches. (Haida Gwaii waters printed in Region 1 match by real overlap here
    # because their MU column literally holds their 6-xx MUs.)
    how = ""
    if row_mus:
        overlap = [c for c in cands if _item_mus(registry[c]) & row_mus]
        if overlap:
            cands, how = overlap, f"MU overlap {sorted(row_mus)}"
        elif all(registry[c].mus for c in cands):
            return MatchResult(index, water, None, "unmatched",
                               f"no '{water}' shares MU {sorted(row_mus)} (needs override)", via=via)
        else:
            cands = [c for c in cands if not registry[c].mus]  # keep only the uncheckable ones
            _log.warning("MU gate: '%s' (reg MU %s) matched an item with NO MUs on record %s — "
                         "can't verify; registry MU data likely incomplete, consider an override",
                         water, sorted(row_mus), cands)

    if len(cands) == 1:
        return MatchResult(index, water, cands[0], "matched", reason=(f"disambiguated by {how}" if how else ""), via=via)

    # Still several same-name candidates (or none had MUs to gate on): fall back to region number.
    if rn:
        narrowed = [c for c in cands if rn in _item_region_nums(registry[c]) or not registry[c].mus]
        if len(narrowed) == 1:
            return MatchResult(index, water, narrowed[0], "matched", reason=f"disambiguated by region {rn}", via=via)
        if narrowed:
            cands = narrowed
    return MatchResult(index, water, None, "ambiguous", f"{len(cands)} candidates for '{water}'", tuple(cands), via=via)


def match_row(index: int, row: dict, registry: dict[str, RegistryItem], name_index: dict[str, list[str]],
              id_index: dict[str, str], override_index: dict[str, list[dict]]) -> MatchResult:
    water = row.get("water", "")
    key = _norm(water)
    rn, row_mus = region_num(row), parse_reg_mus(row)

    e = _pick_override(override_index.get(key, []), rn, row_mus)
    if e is not None:
        note = e.get("note", "") or e.get("skip_reason", "")
        if e.get("not_found"):
            # A curator looked and there is no correct registry item for this name in
            # this MU — a PLACEHOLDER, not a guess. It must not fall through to name
            # matching, because the name usually does resolve, just to the wrong water
            # ("REDFERN LAKE" in MU 5-15 finds the 7-42 Redfern Lake). When the right
            # geometry appears, the row gains ids and this branch stops firing.
            return MatchResult(index, water, None, "unmatched",
                               note or "not_found: no registry item for this name",
                               via="override_not_found")
        if e.get("skip"):
            reason = e.get("skip_reason") or note or "override skip"
            var = (e.get("variant_of") or {}).get("name_verbatim")
            if var:
                reason = f"{reason} (variant_of {var})"
            return MatchResult(index, water, None, "skip", reason, via="override_skip")
        # variant_of WITHOUT skip: the curator says this name is a misnomer for another real water.
        var = e.get("variant_of") or {}
        if var.get("name_verbatim") and not override_typed_ids(e):
            return _name_resolve(index, water, _norm(var["name_verbatim"]), "override_alias",
                                 registry, name_index, rn, row_mus)

        typed = override_typed_ids(e)
        admin = tuple(str(a) for a in e.get("admin_targets", []))
        resolved, unresolved = [], []
        for t in typed:
            hit = id_index.get(t)
            if hit and hit not in resolved:
                resolved.append(hit)
            elif not hit:
                unresolved.append(t)
        if resolved:
            return MatchResult(index, water, resolved[0], "override",
                               reason=(note or "override"), also=tuple(resolved[1:]),
                               via="override", unresolved_ids=tuple(unresolved), admin_targets=admin)
        if typed or admin:
            # Nothing resolved to a NAMED item: nameless features (oxbow channels, unnamed lakes),
            # admin/area zones, or poly/fid pins. These are the RESOLVER's job (it has the full graph)
            # — we surface the curated ids, never fall back to a name guess.
            return MatchResult(index, water, None, "feature_pin",
                               reason=(note or "override pins non-named feature(s) -> resolver"),
                               via="override_feature", unresolved_ids=tuple(unresolved or typed),
                               admin_targets=admin)
        # override with only a note (no ids, no skip, no variant): fall through to name matching.

    return _name_resolve(index, water, key, "name", registry, name_index, rn, row_mus)


def match_rows(rows: list[dict], registry: dict[str, RegistryItem],
               overrides: list[dict] | None = None) -> list[MatchResult]:
    overrides = overrides or []
    name_index = build_name_index(registry)
    id_index = build_id_index(registry)
    ov_index = build_override_index(overrides)
    return [match_row(i, r, registry, name_index, id_index, ov_index) for i, r in enumerate(rows)]
