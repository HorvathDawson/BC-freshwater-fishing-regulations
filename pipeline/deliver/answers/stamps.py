"""THE LICENCE DOCUMENTS A FISH NEEDS (rows 31, owner rule 2026-10-21) — which stamp or licence add-on a kind of fish
on the card, or a released fish, needs here, and when: `stamps` [{doc, act, name, when}] on a group tree's kind
(`rows_row.group.tree[..]` node "kind") and on a not-kept line (`rows_row.group.not_kept[]`).

"You need Conservation Surcharge Stamp for Shuswap rainbow trout › only if you keep rainbow trout over 50 cm"
(Shuswap Lake, the rainbow trout kept up to any size); "You need Steelhead Conservation Surcharge Stamp › only if you
fish for steelhead, even to release them" (the Anderson River's released steelhead). This was the prototype's round
UR7 `stampsFor` (owner 4), which read page v36's rendered licence (data-doc / data-act / data-sp / data-min-cm) in the
browser. Which document a fish needs is a licence answer, so under the owner rule it is decided here, once, with THE
PROTOTYPE'S RULE, kept exactly, and the app only draws it:

  * THE DOCUMENTS. The licence answer at the same part key and segment (`licence` frame), for the angler the Keep tab
    answers for — the prototype's: a B.C. resident, 16 or older, not guided, no status (`DEFAULT_PROFILE`). Of its
    `documents`, those needed only to fish for some fish (`when.act` targeting) or to keep some (retaining), with
    their fish (`when.species`, groups expanded) and the size the keeping one starts at (the first `lengths[].min_cm`).
    A document needed to fish at all is the licence tab's, not a fish's.
  * A KIND (a group tree's kind row). A document naming one of its fish: a fish-for document always (the kind may be
    fished); a keep document where the kind keeps any (`n` is not 0) and, where the document names a size, the kind
    may be kept over it — its largest keepable length (`sizes.cm`, the top of its bands; no top: any length) is over
    the document's size.
  * A RELEASED FISH (a not-kept line of status release): its fish-for documents only (it is fished, never kept). A
    closed fish needs none (it may not be fished).
  * THE WORDS are the licence tab's own (page v36 licParts' `name` and `whenTxt`): the document's name with any
    alternative ("… or …"), and "Only if you fish for steelhead, even to release them" / "Only if you keep rainbow
    trout over 50 cm".

A frame shared between keys and segments is never changed: a row whose group gains a stamp is copied.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence

from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import AnswersError

#: The angler the Keep tab answers for (the prototype's `st.licence.adult`): `licence.PROFILE_DIMS` order.
DEFAULT_PROFILE = {"residency": "resident", "age": "16_plus", "guidance": "non_guided", "status": "none"}
#: The acts that tie a document to some fish (the prototype's `data-act`).
FISH_ACTS = ("targeting", "retaining")


def _sp_words(species: Sequence[str]) -> str:
    """page v36 actTxt's fish: the expanded codes' names, else the groups' own names, else "fish"."""
    return common.lc_names(common.expand(species)) \
        or ", ".join(common.sp_name(c).lower() for c in species) or "fish"


def _len_words(lengths: Optional[Sequence[dict]]) -> str:
    if not lengths:
        return ""
    return " " + ", ".join(
        f"{x['min_cm']}–{x['max_cm']} cm" if x.get("min_cm") is not None and x.get("max_cm") is not None
        else f"over {x['min_cm']} cm" if x.get("min_cm") is not None else f"under {x['max_cm']} cm"
        for x in lengths)


def when_words(when: dict) -> str:
    """page v36 licParts whenTxt for a document needed to fish for or keep some fish: "Only if you fish for
    steelhead, even to release them" / "Only if you keep lake trout and Dolly Varden/bull trout over 60 cm"."""
    if when["act"] == "targeting":
        return f"Only if you fish for {_sp_words(when.get('species') or [])}, even to release them"
    if when["act"] == "retaining":
        return f"Only if you keep {_sp_words(when.get('species') or [])}{_len_words(when.get('lengths'))}"
    raise AnswersError(f"stamps: no words for a document needed to {when['act']!r}")


def documents_of(answer: dict) -> List[dict]:
    """One profile's licence answer -> the documents tied to some fish, each {doc, act, sp (expanded codes), min_cm
    (None: any size), or (the alternatives' documents), when}, in the answer's order. A document whose broadest need
    carries a period (`on`) or narrower needs (`also_when`) is refused: no such fish document exists, and its words
    (the licence tab's designation dates) are not this line's."""
    out = []
    for b in answer.get("documents") or []:
        w = b["when"]
        if w["act"] not in FISH_ACTS or not w.get("species"):
            continue
        if w.get("on") or b.get("also_when"):
            raise AnswersError(f"stamps: the document {b['doc']} is needed to {w['act']} some fish on a period "
                               "or for narrower needs too — no words for it on a fish")
        mins = [x["min_cm"] for x in w.get("lengths") or [] if x.get("min_cm") is not None]
        out.append({"doc": b["doc"], "act": w["act"], "sp": common.expand(w["species"]),
                    "min_cm": mins[0] if mins else None, "or": [list(o["need"]) for o in b.get("or") or []],
                    "when": w})
    return out


def pick(docs: Sequence[dict], codes: Sequence[str], *, fish: bool, keep: bool,
         max_cm: Optional[float]) -> List[dict]:
    """The prototype's stampsFor: of `docs`, those naming one of `codes` that the fish needs — a fish-for document
    where it may be fished, a keep document where it may be kept (over the document's size, where it names one: the
    largest keepable length `max_cm` is over it; None: no top)."""
    return [x for x in docs if any(c in codes for c in x["sp"])
            and (fish if x["act"] == "targeting"
                 else keep and (x["min_cm"] is None or max_cm is None or max_cm > x["min_cm"]))]


def stamp_out(x: dict, names: Dict[str, str]) -> dict:
    """One stamp as stored: {doc, act, name, when} — `name` the document's (with its alternatives, "… or …"), `when`
    the licence tab's sentence ("Only if you …"; ". Either one does" after alternatives)."""
    nm = lambda d: names.get(d) or d.replace("_", " ")             # page v36 DOC
    alts = [" + ".join(nm(d) for d in need) for need in x["or"]]
    return {"doc": x["doc"], "act": x["act"], "name": common.cap(" or ".join([nm(x["doc"]), *alts])),
            "when": when_words(x["when"]) + (". Either one does" if alts else "")}


def kind_max_cm(kind: dict) -> Optional[float]:
    """The largest length a tree kind may be kept at (`sizes.cm` band tops): None where a band has no top; 0 where it
    has no band (the prototype's `Math.max(0, …)`)."""
    cm = (kind.get("sizes") or {}).get("cm") or []
    if any(hi is None for _lo, hi in cm):
        return None
    return max([0, *(hi for _lo, hi in cm)])


def _tree(nodes: Sequence[dict], docs: Sequence[dict], names: Dict[str, str]) -> tuple:
    out, moved = [], False
    for n in nodes:
        if n.get("node") == "tier":
            kids, m = _tree(n.get("children") or [], docs, names)
            out.append({**n, "children": kids} if m else n)
            moved |= m
        elif n.get("node") == "kind":
            got = pick(docs, n["species"], fish=True, keep=n.get("n") != 0, max_cm=kind_max_cm(n))
            if got:
                if "stamps" in n:
                    raise AnswersError("stamps: a kind already carries its stamps")
                out.append({**n, "stamps": [stamp_out(x, names) for x in got]})
                moved = True
            else:
                out.append(n)
        else:
            out.append(n)
    return out, moved


def group_stamps(group: dict, docs: Sequence[dict], names: Dict[str, str]) -> dict:
    """A group with `stamps` on every tree kind and released not-kept line that needs a document (a new dict where
    one does; the same group where none does)."""
    tree, moved = _tree(group.get("tree") or [], docs, names)
    nk = []
    for x in group.get("not_kept") or []:
        got = pick(docs, x["species"], fish=True, keep=False, max_cm=None) if x["status"] == "release" else []
        if got:
            if "stamps" in x:
                raise AnswersError("stamps: a not-kept line already carries its stamps")
            nk.append({**x, "stamps": [stamp_out(y, names) for y in got]})
            moved = True
        else:
            nk.append(x)
    if not moved:
        return group
    return {**group, "tree": tree, **({"not_kept": nk} if "not_kept" in group else {})}


def stamp_licence_documents(rows_frames: Sequence[dict], licence_frames: Sequence[dict], names: Dict[str, str],
                            profile_ix: int) -> List[dict]:
    """One key's rows frames and licence frames (one per segment, aligned) -> rows frames whose groups carry their
    stamps. The inputs are not changed (frames are shared between keys and segments)."""
    if len(rows_frames) != len(licence_frames):
        raise AnswersError(f"stamps: {len(rows_frames)} rows frames for {len(licence_frames)} licence frames")
    out = []
    for f, lic in zip(rows_frames, licence_frames):
        profiles = lic.get("profiles") or []
        if profile_ix >= len(profiles):
            raise AnswersError(f"stamps: the licence frame has no answer for profile {profile_ix}")
        docs = documents_of(profiles[profile_ix])
        if not docs:
            out.append(f)
            continue
        rows, moved = [], False
        for r in f.get("rows") or []:
            g = r.get("group")
            g2 = group_stamps(g, docs, names) if g else g
            rows.append(r if g2 is g else {**r, "group": g2})
            moved |= g2 is not g
        out.append({**f, "rows": rows} if moved else f)
    return out
