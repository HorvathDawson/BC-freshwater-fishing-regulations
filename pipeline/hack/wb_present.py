"""Render ONE waterbody (reg entry) as chat markdown for manual review: the FULL verbatim regulation
with each boundary phrase bolded + numbered, then a numbered list of every locator's endpoint rows
with coord, offset (if any), and click-through satellite/OSM links. Drives off the grouped card
(reg_text + locators) so context is never lost.

    wb_present.py "NAME" [mu ...]     # one waterbody (matches card water + first mu)
    wb_present.py --todo              # list incomplete waterbodies, most todo-locators first
"""
import sys, json, re
from collections import Counter, defaultdict

GROUPED = "pipeline/docs/waterbody-splits.json"
DONE = {"curated", "manual"}
EMO = {"curated": "✅", "manual": "✅", "not_applicable": "⬜", "deferred": "🟡", "todo": "🟥"}


def cards():
    return json.load(open(GROUPED))["cards"]


def sat(lon, lat):
    return f"https://maps.google.com/?q={lat},{lon}&t=k"


def osm(lon, lat):
    return f"https://www.openstreetmap.org/?mlat={lat}&mlon={lon}#map=14/{lat}/{lon}"


def number_reg(reg, phrases):
    """Bold each boundary phrase in the verbatim reg and tag it with its number (longest-first, once)."""
    disp = re.sub(r"[ \t]+", " ", (reg or "").replace("*", ""))
    spans = []
    for n, txt in phrases:
        pd = re.sub(r"\s+", " ", txt or "").strip()
        for cut in (pd, pd[:44]):
            if len(cut) < 4:
                continue
            m = re.search(re.escape(cut).replace(r"\ ", r"\s+"), disp, re.I)
            if not m:
                continue
            if any(not (m.end() <= s or m.start() >= e) for s, e, _ in spans):
                continue
            spans.append((m.start(), m.end(), n)); break
    spans.sort()
    out, cur = [], 0
    for s, e, n in spans:
        out.append(disp[cur:s]); out.append(f"**{disp[s:e].strip()}**{_sup(n)}"); cur = e
    out.append(disp[cur:])
    return "".join(out).strip()


def _sup(n):
    return "".join("⁰¹²³⁴⁵⁶⁷⁸⁹"[int(d)] for d in str(n))


def render(card):
    reg = card["reg_text"]
    locs = [l for l in card["locators"] if l["src"] != "name"]
    phrases = [(i + 1, l["text"]) for i, l in enumerate(locs)]
    L = [f"## {card['water']} · MU {', '.join(card['mu'])} · p{card.get('page') or '?'}  [{card['completeness']}]", ""]
    L.append("> " + number_reg(reg, phrases).replace("\n", "\n> "))
    L.append("")
    for i, l in enumerate(locs, 1):
        rows = l["rows"]
        st = "todo" if not rows else ("todo" if any(r["status"] == "todo" for r in rows)
                                      else ("not_applicable" if all(r["status"] == "not_applicable" for r in rows)
                                            else "curated"))
        head = f"**{i}.** {EMO[st]} _{l['text'][:80]}_"
        L.append(head)
        for r in rows:
            c = r.get("coord")
            bit = f"   - `{r['id'].split('-')[-1]}` [{r['status']}] {r['anchor_kind']}"
            if c:
                lon, lat = c
                bit += f" — `{lat:.5f}, {lon:.5f}` [🛰]({sat(lon,lat)}) [🗺]({osm(lon,lat)})"
            if r.get("offset"):
                o = r["offset"]
                bit += f"  ·  offset {o['m']} m {o['dir']} of {o['anchor_label']}"
            L.append(bit)
        if not rows:
            L.append("   - **needs coord**")
    return "\n".join(L)


def main():
    if sys.argv[1:2] == ["--todo"]:
        C = cards()
        inc = defaultdict(int)
        keys = {}
        for c in C.values():
            nt = sum(1 for l in c["locators"] for r in l["rows"] if r["status"] == "todo")
            if nt:
                k = (c["water"], tuple(c["mu"]))
                inc[k] = nt; keys[k] = c["completeness"]
        for k, nt in sorted(inc.items(), key=lambda kv: -kv[1]):
            print(f"{nt:2d} todo  [{keys[k]}]  {k[0]}  {list(k[1])}")
        return
    name = sys.argv[1].upper()
    mu = sys.argv[2] if len(sys.argv) > 2 else None
    for c in cards().values():
        if c["water"].upper().startswith(name) and (mu is None or mu in c["mu"]):
            print(render(c)); return
    print("no card for", name, mu)


if __name__ == "__main__":
    main()
