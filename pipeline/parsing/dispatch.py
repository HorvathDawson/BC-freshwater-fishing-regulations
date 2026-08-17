"""Automated dispatch — run each exported batch through the Claude CLI (headless Opus) and save the
response, so parsing doesn't need a human to paste prompts.

This is the automation layer over the tested export -> (parse) -> ingest flow. It shells out to the
`claude` CLI in print mode with the batch's self-contained prompt; the CLI agent has Bash access, so it
can run `pipeline.parsing.validate` and `pipeline.parsing.species` to self-correct BEFORE emitting its
JSON (exactly the "the chat can use the python functions" intent). Requires the `claude` CLI installed +
authenticated (set CLAUDE_BIN or --claude-bin if it isn't on PATH). If you're parsing inside a chat
already, skip this and paste batches to a subagent by hand — same downstream ingest.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.dispatch --model opus
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.dispatch --review     # second-pass review
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from pipeline.parsing.ingest import _parse_response


def _extract_json_array(stdout: str) -> list[dict]:
    """The CLI (`--output-format json`) wraps the reply in an envelope with a `result` string; older/raw
    modes print the reply directly. Handle both, then reuse the ingest fence-stripping parser."""
    text = stdout.strip()
    try:
        env = json.loads(text)
        if isinstance(env, dict) and "result" in env:
            text = env["result"]
    except json.JSONDecodeError:
        pass
    return _parse_response(text)


def _cli_flags(model: str, permission_mode: str, allowed_tools: str, skip_perms: bool) -> list[str]:
    """Flags shared by the parse + review invocations. `skip_perms` (--dangerously-skip-permissions) is
    the reliable way to let a headless `-p` subagent run its Bash self-checks; otherwise pass an
    --allowed-tools allowlist (e.g. 'Bash Read') or a permission-mode."""
    flags = ["-p", "--output-format", "json", "--model", model]
    if skip_perms:
        flags.append("--dangerously-skip-permissions")
    else:
        flags += ["--permission-mode", permission_mode]
        if allowed_tools:
            flags += ["--allowed-tools", allowed_tools]
    return flags


def dispatch_prompt(prompt_path: Path, response_path: Path, *, claude_bin: str, cli_flags: list[str],
                    cwd: Path, timeout: int) -> list[dict]:
    """Run one prompt through the CLI, write the parsed JSON array to `response_path`, return it."""
    prompt = prompt_path.read_text(encoding="utf-8")
    cmd = [claude_bin, *cli_flags]
    proc = subprocess.run(cmd, input=prompt, capture_output=True, text=True, cwd=str(cwd), timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(f"claude CLI failed ({proc.returncode}) for {prompt_path.name}: {proc.stderr[:500]}")
    result = _extract_json_array(proc.stdout)
    response_path.parent.mkdir(parents=True, exist_ok=True)
    response_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def _dispatch_reviews(batches_dir: Path, responses_dir: Path, reviews_dir: Path, bids: list[int], *,
                      claude_bin: str, cli_flags: list[str], cwd: Path, timeout: int,
                      concurrency: int, force: bool) -> None:
    """Second pass: an INDEPENDENT reviewer subagent per parsed batch (render + dispatch)."""
    from pipeline.parsing.review_exporter import render_from_files
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
            return bid, f"CLI failed: {proc.stderr[:200]}"
        out_path.write_text(proc.stdout, encoding="utf-8")
        return bid, f"reviewed -> {out_path.name}"

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for fut in as_completed([pool.submit(_one, b) for b in bids]):
            bid, msg = fut.result()
            print(f"  review batch {bid:03d}: {msg}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Dispatch exported batches through the Claude CLI (parallel subagents).")
    ap.add_argument("--work-dir", help="parse working dir (default: <out>/parse)")
    ap.add_argument("--model", default="opus", help="CLI model alias (default: opus)")
    ap.add_argument("--claude-bin", default=os.environ.get("CLAUDE_BIN", "claude"))
    ap.add_argument("--permission-mode", default="acceptEdits",
                    help="CLI permission mode (used unless --dangerously-skip-permissions)")
    ap.add_argument("--allowed-tools", default="Bash Read",
                    help="tools the subagent may use non-interactively so it can self-validate (default: 'Bash Read')")
    ap.add_argument("--dangerously-skip-permissions", action="store_true",
                    help="reliable headless mode: let the subagent run its Bash self-checks with no prompts")
    ap.add_argument("--concurrency", type=int, default=3, help="parallel batch subagents (default: 3)")
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--only", type=int, help="dispatch a single batch id")
    ap.add_argument("--force", action="store_true", help="re-dispatch batches with an existing response")
    ap.add_argument("--review", action="store_true", help="also run an independent reviewer subagent per batch")
    args = ap.parse_args()

    if args.work_dir:
        work = Path(args.work_dir)
    else:
        from pipeline.parsing.batch_exporter import default_work_dir
        work = default_work_dir()
    batches_dir, responses_dir, reviews_dir = work / "batches", work / "responses", work / "reviews"
    manifest = json.loads((work / "manifest.json").read_text(encoding="utf-8"))
    project_root = Path(__file__).resolve().parents[2]

    cli_flags = _cli_flags(args.model, args.permission_mode, args.allowed_tools,
                           args.dangerously_skip_permissions)

    bids = [b["id"] for b in manifest["batches"] if args.only is None or b["id"] == args.only]
    todo = [b for b in bids if args.force or not (responses_dir / f"batch_{b:03d}.json").exists()]
    if len(todo) < len(bids):
        print(f"  skipping {len(bids) - len(todo)} batch(es) with an existing response (use --force)")

    def _parse_one(bid: int):
        result = dispatch_prompt(batches_dir / f"batch_{bid:03d}.prompt.txt",
                                 responses_dir / f"batch_{bid:03d}.json",
                                 claude_bin=args.claude_bin, cli_flags=cli_flags, cwd=project_root,
                                 timeout=args.timeout)
        return bid, len(result)

    # fan out: each batch is an independent Claude subagent, run `--concurrency` at a time
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        for fut in as_completed([pool.submit(_parse_one, b) for b in todo]):
            bid, n = fut.result()
            print(f"  batch {bid:03d}: {n} entr(ies) saved")

    if args.review:
        _dispatch_reviews(batches_dir, responses_dir, reviews_dir, bids, claude_bin=args.claude_bin,
                          cli_flags=cli_flags, cwd=project_root, timeout=args.timeout,
                          concurrency=args.concurrency, force=args.force)

    print(f"Done. Next: python -m pipeline.parsing.ingest {responses_dir}/*.json --dry-run")


if __name__ == "__main__":
    main()
