"""Compare the Gemini parse against an agent parse of the synopsis.

Loads two session checkpoints — Gemini's shared session and an agent-parsed
session — and reports, per row, where the two engines disagree: rule count,
tributary flags, restriction-type mix, dates, and the verbatim echo. Only rows
that BOTH engines parsed are compared; coverage gaps are reported separately.

Usage
-----
    # Compare the live Gemini session against the full agent session
    python -m pipeline.agent_parsing.compare \
        --agent-session-dir output/pipeline/agent_parsing_full

    # Only show rows whose rule count differs
    python -m pipeline.agent_parsing.compare \
        --agent-session-dir output/pipeline/agent_parsing_full \
        --category rule_count

    # Machine-readable report
    python -m pipeline.agent_parsing.compare \
        --agent-session-dir output/pipeline/agent_parsing_full --json
"""

from __future__ import annotations

import argparse
import json
import textwrap
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

from pipeline.parsing.rows import load_synopsis_rows

from .common import default_agent_dir, load_session_state, resolve_parsing_dir

# Categories compared, in display order.
CATEGORIES = (
    "echo",
    "includes_tributaries",
    "tributary_only",
    "rule_count",
    "restriction_types",
    "dates",
)


def _norm(s: str) -> str:
    return " ".join((s or "").replace("**", "").replace("\n", " ").split()).lower()


def _types(entry: Dict[str, Any]) -> Counter:
    return Counter(r["restriction_type"] for r in entry["rules"])


def _dates(entry: Dict[str, Any]) -> List[str]:
    return sorted({d for r in entry["rules"] for d in r["dates"]})


def compare_entry(g: Dict[str, Any], a: Dict[str, Any]) -> Dict[str, Tuple[Any, Any]]:
    """Return the categories that differ between two entries → (gemini, agent)."""
    diffs: Dict[str, Tuple[Any, Any]] = {}
    if _norm(g["regs_verbatim"]) != _norm(a["regs_verbatim"]):
        diffs["echo"] = (g["regs_verbatim"], a["regs_verbatim"])
    if g["includes_tributaries"] != a["includes_tributaries"]:
        diffs["includes_tributaries"] = (
            g["includes_tributaries"],
            a["includes_tributaries"],
        )
    if g["tributary_only"] != a["tributary_only"]:
        diffs["tributary_only"] = (g["tributary_only"], a["tributary_only"])
    if len(g["rules"]) != len(a["rules"]):
        diffs["rule_count"] = (len(g["rules"]), len(a["rules"]))
    tg, ta = _types(g), _types(a)
    if tg != ta:
        diffs["restriction_types"] = (dict(tg), dict(ta))
    dg, da = _dates(g), _dates(a)
    if dg != da:
        diffs["dates"] = (dg, da)
    return diffs


def compare_sessions(
    gemini_results: List[Any],
    agent_results: List[Any],
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Compare two results lists (index-aligned) against the row list.

    Rows are bucketed into four disjoint groups: parsed by *both* engines
    (the only ones actually diffed), parsed by *only* one engine, or by
    neither.  The single-engine rows are recorded as lists so the report can
    surface them in their own section rather than silently hiding them.
    """
    total = len(rows)
    report: Dict[str, Any] = {
        "total": total,
        "both_parsed": 0,
        "only_gemini": 0,
        "only_agent": 0,
        "neither": 0,
        "identical": 0,
        "category_counts": Counter(),
        "only_gemini_rows": [],
        "only_agent_rows": [],
        "rows": [],
    }
    for i in range(total):
        g = gemini_results[i] if i < len(gemini_results) else None
        a = agent_results[i] if i < len(agent_results) else None
        water = rows[i].get("water", "")
        if g is None and a is None:
            report["neither"] += 1
            continue
        if g is None:
            report["only_agent"] += 1
            report["only_agent_rows"].append({"index": i, "water": water})
            continue
        if a is None:
            report["only_gemini"] += 1
            report["only_gemini_rows"].append({"index": i, "water": water})
            continue
        report["both_parsed"] += 1
        diffs = compare_entry(g, a)
        if not diffs:
            report["identical"] += 1
            continue
        for cat in diffs:
            report["category_counts"][cat] += 1
        report["rows"].append(
            {
                "index": i,
                "water": water,
                "raw": rows[i].get("raw_regs", ""),
                "diffs": diffs,
                "gemini": g,
                "agent": a,
            }
        )
    return report


# --- pretty-printing ------------------------------------------------------

RULE = "─" * 74


def _section(title: str) -> None:
    print(f"\n{RULE}\n  {title}\n{RULE}")


def _pct(n: int, total: int) -> str:
    return f"{100 * n / total:.0f}%" if total else "—"


def _wrap(text: str, indent: str) -> str:
    body = " ".join((text or "").split())
    lines = textwrap.wrap(body, width=72 - len(indent)) or [""]
    return "\n".join(indent + ln for ln in lines)


def _render_rules(entry: Dict[str, Any]) -> List[str]:
    """Compact, human-readable rendering of an entry's rules."""
    out: List[str] = []
    for r in entry["rules"]:
        dates = f"  [{', '.join(r['dates'])}]" if r.get("dates") else ""
        loc = f"  @ {r['location_text']}" if r.get("location_text") else ""
        out.append(f"      • ({r['restriction_type']}) {r['details']}{loc}{dates}")
    return out


def _format_diff(cat: str, gv: Any, av: Any) -> str:
    if cat == "echo":
        return "      echo         : regs_verbatim text differs"
    label = cat.replace("_", " ")
    return f"      {label:<13}: gemini={gv}  |  agent={av}"


def _print_row_diff(r: Dict[str, Any], categories: set) -> None:
    """Print one differing row, restricted to the given diff categories."""
    show = [c for c in CATEGORIES if c in categories and c in r["diffs"]]
    print(f"\n  [{r['index']}] {r['water']}")
    print(_wrap(r["raw"], "      raw: "))
    print(f"      differs on: {', '.join(show)}")
    for cat in show:
        gv, av = r["diffs"][cat]
        print(_format_diff(cat, gv, av))
    print(f"      Gemini ({len(r['gemini']['rules'])} rules):")
    for line in _render_rules(r["gemini"]):
        print(line)
    print(f"      Agent  ({len(r['agent']['rules'])} rules):")
    for line in _render_rules(r["agent"]):
        print(line)


def _print_single_engine(report: Dict[str, Any], limit: int) -> None:
    """List rows only one engine parsed — these are NOT compared."""
    _section("Parsed by only ONE engine (not compared)")
    for label, key in (("Only Gemini parsed", "only_gemini_rows"),
                       ("Only agent parsed", "only_agent_rows")):
        rows = report[key]
        print(f"\n  {label} ({len(rows)}):")
        if not rows:
            print("    (none)")
            continue
        shown = rows if limit <= 0 else rows[:limit]
        for r in shown:
            print(f"    [{r['index']:>4}] {r['water']}")
        if limit > 0 and len(rows) > limit:
            print(f"    ... {len(rows) - limit} more (use --limit 0 to see all)")


def _print_report(report: Dict[str, Any], category: str, limit: int) -> None:
    both = report["both_parsed"]
    total = report["total"]
    differ = len(report["rows"])

    _section("Coverage")
    print(f"  total rows            {total:>5}")
    print(f"  parsed by both        {both:>5}   ({_pct(both, total)})  "
          f"← compared below")
    print(f"  only Gemini parsed    {report['only_gemini']:>5}   "
          f"({_pct(report['only_gemini'], total)})")
    print(f"  only agent parsed     {report['only_agent']:>5}   "
          f"({_pct(report['only_agent'], total)})")
    print(f"  parsed by neither     {report['neither']:>5}   "
          f"({_pct(report['neither'], total)})")

    _section("Agreement (rows parsed by BOTH)")
    print(f"  identical             {report['identical']:>5} / {both}   "
          f"({_pct(report['identical'], both)})")
    print(f"  differ                {differ:>5} / {both}   "
          f"({_pct(differ, both)})")

    _section("Differences by category (rows parsed by BOTH)")
    if not report["category_counts"]:
        print("  (no differences)")
    for cat in CATEGORIES:
        n = report["category_counts"].get(cat, 0)
        if n:
            label = cat.replace("_", " ")
            print(f"  {label:<20} {n:>5}   ({_pct(n, both)} of compared)")

    # Single-engine coverage gaps get their own section.
    _print_single_engine(report, limit)

    # Row-by-row differences, grouped under each category they differ on.
    # A row that differs in several categories appears under each one.
    cats = [category] if category else list(CATEGORIES)
    print("\n  (raw = source text / ground truth — compare each engine's rules to it)")
    for cat in cats:
        crows = [r for r in report["rows"] if cat in r["diffs"]]
        if not crows:
            continue
        shown = crows if limit <= 0 else crows[:limit]
        label = cat.replace("_", " ").upper()
        _section(f"Differences — {label}   (showing {len(shown)} of {len(crows)})")
        for r in shown:
            _print_row_diff(r, {cat})
        if limit > 0 and len(crows) > limit:
            print(f"\n  ... {len(crows) - limit} more differing on "
                  f"{cat.replace('_', ' ')} (raise --limit or use --json)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare the Gemini and agent parses of the synopsis."
    )
    parser.add_argument(
        "--gemini-session-dir",
        help="Gemini session dir (default: the shared parsing output dir).",
    )
    parser.add_argument(
        "--agent-session-dir",
        help="Agent session dir (default: output/pipeline/agent_parsing).",
    )
    parser.add_argument("--raw", help="Path to synopsis_raw_data.json (optional).")
    parser.add_argument(
        "--category",
        choices=CATEGORIES,
        help="Only list rows differing in this category.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=40,
        help="Max differing rows to print (0 = all). Default 40.",
    )
    parser.add_argument(
        "--json", action="store_true", help="Emit the full report as JSON."
    )
    args = parser.parse_args()

    rows = load_synopsis_rows(Path(args.raw) if args.raw else None)

    gemini_dir = (
        Path(args.gemini_session_dir)
        if args.gemini_session_dir
        else resolve_parsing_dir()
    )
    agent_dir = (
        Path(args.agent_session_dir)
        if args.agent_session_dir
        else default_agent_dir()
    )

    gemini = load_session_state(gemini_dir, expected_total=len(rows))
    agent = load_session_state(agent_dir, expected_total=len(rows))

    report = compare_sessions(gemini["results"], agent["results"], rows)

    if args.json:
        # Counter → dict for JSON.
        out = dict(report)
        out["category_counts"] = dict(report["category_counts"])
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    print(RULE)
    print("  SYNOPSIS PARSE COMPARISON  \u2014  Gemini vs Agent")
    print(RULE)
    print(f"  gemini session: {gemini_dir}")
    print(f"  agent  session: {agent_dir}")
    _print_report(report, args.category, args.limit)


if __name__ == "__main__":
    main()
