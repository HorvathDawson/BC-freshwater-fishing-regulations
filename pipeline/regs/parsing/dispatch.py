"""Automated dispatch — run each exported batch through the Claude CLI (headless) and save the
response, so parsing doesn't need a human to paste prompts.

⛔ HUMAN-ONLY: this spends the user's credits (it spawns `claude -p` workers). Claude/agents must NOT
run this module — only hand the user the command to run in their own terminal. See CLAUDE.md.

This is the automation layer over the tested export -> (parse) -> ingest flow. It shells out to the
`claude` CLI in print mode with the batch's self-contained prompt; the CLI agent has Bash access, so it
can run `pipeline.regs.parsing.validate_catalogue` and `pipeline.regs.parsing.species` to self-correct BEFORE emitting its
JSON (exactly the "the chat can use the python functions" intent). Requires the `claude` CLI installed +
authenticated (set CLAUDE_BIN or --claude-bin if it isn't on PATH). If you're parsing inside a chat
already, skip this and paste batches to a subagent by hand — same downstream ingest.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.dispatch --model opus
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.dispatch --review     # second-pass review
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from pipeline.regs.parsing import io
from pipeline.common.curated import REPO_ROOT


class CreditExhausted(RuntimeError):
    """The CLI stopped because the account is out of credits / hit a usage limit — a clean, resumable
    stop (the batch wrote no response, so a rerun picks it up), NOT a per-batch parse failure."""


# Substrings (lowercased) that mark a usage-limit / billing stop in the CLI's stderr or JSON envelope.
# Kept broad on purpose: a false positive only makes us stop early (rerun resumes), never corrupts data.
_CREDIT_MARKERS = (
    "usage limit", "rate limit", "rate_limit", "credit balance", "insufficient credit",
    "quota", "out of credits", "billing", "payment required", "429", "overloaded",
    "insufficient_quota", "too many requests",
)


def _is_credit_error(text: str) -> bool:
    low = (text or "").lower()
    return any(m in low for m in _CREDIT_MARKERS)


_extract_json_array = io.parse_response                  # CLI envelope + fences -> [{index, entry}] (io)


def _cli_flags(model: str, permission_mode: str, allowed_tools: str, skip_perms: bool) -> list[str]:
    """Flags shared by the parse + review invocations. Default is SINGLE-SHOT: no tools, the subagent
    just emits JSON (cheapest — validation happens downstream at ingest, failures are re-dispatched).
    Opt into an agentic run (in-agent self-validation) with --dangerously-skip-permissions, or scope
    it with an --allowed-tools allowlist (e.g. 'Bash Read')."""
    flags = ["-p", "--output-format", "json", "--model", model]
    if skip_perms:
        flags.append("--dangerously-skip-permissions")
    elif allowed_tools:
        flags += ["--permission-mode", permission_mode, "--allowed-tools", allowed_tools]
    # else: single-shot — no tools, pure JSON generation (the default)
    return flags


def dispatch_prompt(prompt_path: Path, response_path: Path, *, claude_bin: str, cli_flags: list[str],
                    cwd: Path, timeout: int) -> list[dict]:
    """Run one prompt through the CLI, write the parsed JSON array to `response_path`, return it."""
    prompt = prompt_path.read_text(encoding="utf-8")
    cmd = [claude_bin, *cli_flags]
    proc = subprocess.run(cmd, input=prompt, capture_output=True, text=True, cwd=str(cwd), timeout=timeout)
    if proc.returncode != 0:
        detail = f"{proc.stderr}\n{proc.stdout}"[:1000]
        if _is_credit_error(detail):
            raise CreditExhausted(f"credit/usage limit hit on {prompt_path.name}: {proc.stderr[:300]}")
        raise RuntimeError(f"claude CLI failed ({proc.returncode}) for {prompt_path.name}: {proc.stderr[:500]}")
    result = _extract_json_array(proc.stdout)
    response_path.parent.mkdir(parents=True, exist_ok=True)
    response_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def _dispatch_reviews(batches_dir: Path, responses_dir: Path, reviews_dir: Path, bids: list[int], *,
                      claude_bin: str, cli_flags: list[str], cwd: Path, timeout: int,
                      concurrency: int, force: bool, review_model: str = "") -> None:
    """Second pass: an INDEPENDENT reviewer subagent per parsed batch (render + dispatch)."""
    from pipeline.regs.parsing.review_exporter import render_from_files
    reviews_dir.mkdir(parents=True, exist_ok=True)

    def _one(bid: int):
        response_path = responses_dir / f"batch_{bid:03d}.json"
        if not response_path.exists():
            return bid, "no response to review"
        prompt_path = reviews_dir / f"batch_{bid:03d}.review.prompt.txt"
        out_path = reviews_dir / f"batch_{bid:03d}.review.json"
        if out_path.exists() and not force:
            return bid, "review exists (skipped)"
        prompt_path.write_text(render_from_files(batches_dir / f"batch_{bid:03d}.json", response_path),
                               encoding="utf-8")
        prompt = prompt_path.read_text(encoding="utf-8")
        cmd = [claude_bin, *cli_flags]
        proc = subprocess.run(cmd, input=prompt, capture_output=True, text=True, cwd=str(cwd), timeout=timeout)
        if proc.returncode != 0:
            if _is_credit_error(f"{proc.stderr}\n{proc.stdout}"):
                raise CreditExhausted(f"credit/usage limit hit reviewing batch {bid:03d}")
            return bid, f"CLI failed: {proc.stderr[:200]}"
        # A reply that is not a verdict is a FAILED review, recorded as one. It used to be
        # normalised to `{verdict: "", issues: []}`, which every reader took for a clean pass.
        obj = _extract_json_obj(proc.stdout)
        if obj.get("verdict") not in ("pass", "changes_requested") \
                or not isinstance(obj.get("issues", []), list):
            obj = {"error": io.REVIEW_FAILED, "raw": proc.stdout[:4000]}
        obj["model"] = review_model
        obj["reviewed_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        out_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        if obj.get("error"):
            return bid, f"✗ REVIEW FAILED (unreadable reply) -> {out_path.name}; rerun with --rereview"
        return bid, f"reviewed -> {out_path.name}"

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futs = {pool.submit(_one, b): b for b in bids}
        try:
            for fut in as_completed(futs):
                bid, msg = fut.result()
                print(f"  review batch {bid:03d}: {msg}")
        except CreditExhausted:
            for f in futs:
                f.cancel()
            print("  ⚠ CREDIT/USAGE LIMIT during review — stopping. Re-run to resume "
                  "(existing reviews are skipped).")


def _write_run_state(path: Path, model: str, all_bids: list[int], responses_dir: Path,
                     statuses: dict[int, dict], credit_stop: bool) -> None:
    """Persist a resume-handoff: per-batch status so a rerun (or a human) sees exactly where a run
    stopped. `done` is derived from the response file on disk (the real resume signal), so this file
    is advisory — deleting it never loses work. Written after every batch for crash-safety."""
    batches: dict[str, dict] = {}
    for bid in all_bids:
        if (responses_dir / f"batch_{bid:03d}.json").exists():
            batches[str(bid)] = statuses.get(bid, {"status": "done"})
        else:
            batches[str(bid)] = statuses.get(bid, {"status": "pending"})
    done = sum(1 for v in batches.values() if v["status"] in ("done", "ingested"))
    payload = {
        "updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model": model,
        "total_batches": len(all_bids),
        "done": done,
        "remaining": len(all_bids) - done,
        "credit_stop": credit_stop,
        "batches": batches,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


_extract_json_obj = io.extract_json_obj                  # CLI envelope + fences -> {verdict, issues} (io)


def _covered_batch_ids(manifest: dict, batches_dir: Path, entries_dir: Path) -> set[int]:
    """Batch ids every one of whose rows is ALREADY present in the catalogue region files (parsed
    and ingested on a prior run). These are skipped on a normal run so a resume never re-parses finished
    work — and because the batch layout is stable, skipping here never renumbers anything (unlike the
    old export-time skip that caused the desync)."""
    from pipeline.regs.parsing.batch_exporter import load_existing_entry_ids
    done_ids = load_existing_entry_ids(entries_dir)
    if not done_ids:
        return set()
    covered: set[int] = set()
    for b in manifest["batches"]:
        bf = batches_dir / f"batch_{b['id']:03d}.json"
        if not bf.exists():
            continue
        items = json.loads(bf.read_text(encoding="utf-8")).get("items", [])
        if items and all(it.get("entry_id") in done_ids for it in items):
            covered.add(b["id"])
    return covered


def _invalid_batch_ids(manifest: dict, batches_dir: Path, responses_dir: Path) -> list[int]:
    """Batch ids whose EXISTING response fails ingest validation — a response was written but ≥1 of its
    entries is invalid, unparseable, or missing. A normal resume skips these (the file exists), so they
    must be targeted explicitly to re-parse (e.g. on a stronger model)."""
    # JUDGED BY THE INGEST THAT WRITES. This called the retired prose ingest, which refuses every
    # catalogue entry, so every response looked invalid and `--redo-invalid` re-bought them all.
    from pipeline.regs.parsing.ingest_catalogue import ingest, load_batch, response_rows
    batch = load_batch([str(p) for p in sorted(batches_dir.glob("batch_*.json"))])
    bad_indices: set[int] = set()
    for b in manifest["batches"]:
        rp = responses_dir / f"batch_{b['id']:03d}.json"
        if not rp.exists():
            continue
        try:
            accepted, _ = ingest(response_rows(rp), batch)
        except Exception:                                 # noqa: BLE001 — garbage response: redo it
            bad_indices.update(b["indices"])
            continue
        good = {batch[eid].get("index") for eid in accepted if eid in batch}
        bad_indices.update(set(b["indices"]) - good)      # rejected, or never returned
    return sorted({b["id"] for b in manifest["batches"] if set(b["indices"]) & bad_indices})


def main() -> None:
    ap = argparse.ArgumentParser(description="Dispatch exported batches through the Claude CLI (parallel subagents).")
    ap.add_argument("--work-dir", help="parse working dir (default: <out>/parse)")
    ap.add_argument("--entries-dir", help="catalogue region files dir (default: "
                    "data/curated/regulations/entries/catalogue) — batches fully covered by these "
                    "are skipped as already ingested")
    ap.add_argument("--model", default="sonnet", help="CLI model alias (default: sonnet — parsing is "
                    "mechanical; reserve opus for re-dispatching failures)")
    ap.add_argument("--claude-bin", default=os.environ.get("CLAUDE_BIN", "claude"))
    ap.add_argument("--permission-mode", default="acceptEdits",
                    help="CLI permission mode (only used with --allowed-tools)")
    ap.add_argument("--allowed-tools", default="",
                    help="tools the subagent may use; empty (default) = single-shot, no tools")
    ap.add_argument("--dangerously-skip-permissions", action="store_true",
                    help="agentic mode: give the subagent full tool access for in-agent self-validation "
                    "(much more expensive — default is single-shot)")
    ap.add_argument("--concurrency", type=int, default=3, help="parallel batch subagents (default: 3)")
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--only", help="dispatch only these batch id(s), comma-separated (e.g. '3' or '3,5,7')")
    ap.add_argument("--force", action="store_true", help="re-dispatch batches with an existing response")
    ap.add_argument("--review", action="store_true", help="also run an independent reviewer subagent per batch")
    ap.add_argument("--rereview", action="store_true",
                    help="re-review even batches that already have a review file (refresh the verdict), "
                    "WITHOUT --force re-parsing. Use to re-review everything after a prompt change.")
    ap.add_argument("--review-model", default="", help="model for the review pass (default: same as --model)")
    ap.add_argument("--redo-invalid", action="store_true",
                    help="re-parse batches whose existing response fails validation (schema/split-id/"
                    "incomplete) — escalation tier, e.g. with --model opus")
    args = ap.parse_args()

    if args.work_dir:
        work = Path(args.work_dir)
    else:
        from pipeline.regs.parsing.batch_exporter import default_work_dir
        work = default_work_dir()
    batches_dir, responses_dir, reviews_dir = work / "batches", work / "responses", work / "reviews"
    manifest = json.loads((work / "manifest.json").read_text(encoding="utf-8"))
    project_root = REPO_ROOT

    cli_flags = _cli_flags(args.model, args.permission_mode, args.allowed_tools,
                           args.dangerously_skip_permissions)

    only = None if not args.only else {int(x) for x in str(args.only).split(",") if x.strip()}
    bids = [b["id"] for b in manifest["batches"] if only is None or b["id"] in only]
    covered: set[int] = set()

    if args.redo_invalid:
        # Escalation tier: target only the hard cases (+ any still-missing responses), re-parse them
        # on this run's (stronger) model, overwriting their old responses.
        target: set[int] = {b for b in bids if not (responses_dir / f"batch_{b:03d}.json").exists()}
        inv = set(_invalid_batch_ids(manifest, batches_dir, responses_dir)) & set(bids)
        target |= inv
        print(f"  redo-invalid: {len(inv)} batch(es) with invalid/incomplete responses")
        todo = sorted(target)
        for b in todo:                                    # a re-parse invalidates the old review
            rev = reviews_dir / f"batch_{b:03d}.review.json"
            if rev.exists():
                rev.unlink()
    else:
        entries_dir = Path(args.entries_dir) if args.entries_dir else \
            io.entries_dir()
        covered = set() if args.force else _covered_batch_ids(manifest, batches_dir, entries_dir)
        todo = [b for b in bids if b not in covered
                and (args.force or not (responses_dir / f"batch_{b:03d}.json").exists())]
        n_ingested = len(covered & set(bids))
        n_response = len(bids) - len(todo) - n_ingested
        if n_ingested:
            print(f"  skipping {n_ingested} batch(es) already ingested into the catalogue")
        if n_response:
            print(f"  skipping {n_response} batch(es) with an existing response (use --force)")

    run_state_path = work / "run_state.json"
    statuses: dict[int, dict] = {b: {"status": "done"} for b in bids
                                 if (responses_dir / f"batch_{b:03d}.json").exists()}
    if not args.redo_invalid:
        for b in covered & set(bids):                     # already in the catalogue (may have no response)
            statuses.setdefault(b, {"status": "ingested"})

    def _parse_one(bid: int):
        result = dispatch_prompt(batches_dir / f"batch_{bid:03d}.prompt.txt",
                                 responses_dir / f"batch_{bid:03d}.json",
                                 claude_bin=args.claude_bin, cli_flags=cli_flags, cwd=project_root,
                                 timeout=args.timeout)
        return bid, len(result)

    # fan out: each batch is an independent Claude subagent, run `--concurrency` at a time. A per-batch
    # failure is recorded (not fatal) so the rest of the run continues; a credit/usage-limit stop is a
    # CLEAN halt — cancel not-yet-started batches, keep every completed response, and leave a resume note.
    credit_stop = False
    failed = 0
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futs = {pool.submit(_parse_one, b): b for b in todo}
        try:
            for fut in as_completed(futs):
                bid = futs[fut]
                try:
                    _, n = fut.result()
                    statuses[bid] = {"status": "done", "entries": n}
                    print(f"  batch {bid:03d}: {n} entr(ies) saved")
                except CreditExhausted as e:
                    credit_stop = True
                    statuses[bid] = {"status": "credit_stopped", "error": str(e)[:300]}
                    print(f"  batch {bid:03d}: ⚠ CREDIT/USAGE LIMIT — stopping cleanly")
                    for f in futs:                       # don't start any batch that hasn't begun
                        f.cancel()
                    break
                except Exception as e:                   # noqa: BLE001 — record and keep going
                    failed += 1
                    statuses[bid] = {"status": "failed", "error": str(e)[:300]}
                    print(f"  batch {bid:03d}: ✗ FAILED — {str(e)[:160]}")
                finally:
                    _write_run_state(run_state_path, args.model, bids, responses_dir, statuses, credit_stop)
        except BaseException:                            # crash OR Ctrl-C: cancel queued batches so the
            for f in futs:                               # pool's shutdown(wait=True) can't drain them and
                f.cancel()                               # keep burning credits (the original run's bug)
            raise
        finally:
            _write_run_state(run_state_path, args.model, bids, responses_dir, statuses, credit_stop)

    done = sum(1 for b in bids if (responses_dir / f"batch_{b:03d}.json").exists() or b in covered)
    remaining = len(bids) - done
    print(f"\nParse: {done}/{len(bids)} batches done" + (f", {failed} failed this run" if failed else "")
          + f". run_state: {run_state_path}")

    if credit_stop or remaining:
        reason = "credit/usage limit" if credit_stop else "incomplete batches"
        print(f"  ⚠ stopped early ({reason}). {remaining} batch(es) remaining — RESUME by re-running the "
              f"SAME command; completed responses are skipped automatically.")
        return                                            # don't review/finish a partial run

    if args.review or args.rereview:
        review_model = args.review_model or args.model
        review_flags = _cli_flags(review_model, args.permission_mode, args.allowed_tools,
                                  args.dangerously_skip_permissions)
        print(f"\nReview pass ({review_model}){' — rereview (force)' if args.rereview else ''}:")
        _dispatch_reviews(batches_dir, responses_dir, reviews_dir, bids, claude_bin=args.claude_bin,
                          cli_flags=review_flags, cwd=project_root, timeout=args.timeout,
                          concurrency=args.concurrency, force=(args.force or args.rereview),
                          review_model=review_model)

    print(f"Done. Next: python -m pipeline.regs.parsing.ingest_catalogue --batch {responses_dir}/../batches/batch_*.json --response {responses_dir}/batch_*.json --out data/curated/regulations/entries/catalogue --dry-run")


if __name__ == "__main__":
    main()
