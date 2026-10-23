"""The review screen IN A BROWSER (T142): what only a real page can show.

    draft guard   fix mode: an unsaved edit is never left by one key — `j` asks first; "cancel"
                  stays on the entry with the edit, "OK" moves on (T134 adversarial review, which
                  had no browser test because the frontend has no test runner)
    blur          clicking an answer button gives the focus back, so the next Enter RECORDS the
                  mark instead of re-clicking that button (T134 left-over)

Runs the real backend (uvicorn, FIX MODE, over temp copies of the catalogue, splits.json and the
marks store; an empty reach dir, so every entry awaits its build and only T and R are answered) and
the real Vite dev server, each on a FREE PORT of its own (never the owner's 39217 / 39218 / 8787 /
5173), and drives them with Playwright's Chromium. Slow (a cold backend loads the registry): `-m slow`.

Mutation pins, run by hand on 2026-10-23 (lane-T135): the blur removed from `Checklist.tsx` ->
`test_enter_after_clicking_an_answer_records_the_mark` fails (no mark: Enter re-clicked "R ?");
`go` in `App.tsx` without its `window.confirm` -> `test_the_draft_guard_asks_before_j_leaves_an_edit`
fails (no dialog).
"""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED, GENERATED
from pipeline.tests.conftest import ATLAS_HINT, need

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
FRONTEND = REPO / "curation-review" / "frontend"
ADAM = "r1:adam_river_except_eve_river@1-10"
OWNER_PORTS = {39217, 39218, 8787, 5173}

pytestmark = [pytest.mark.slow, pytest.mark.needs_atlas, pytest.mark.needs_source]


def _free_port() -> int:
    while True:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        if port not in OWNER_PORTS:
            return port


def _wait(url: str, seconds: float) -> None:
    end = time.time() + seconds
    while time.time() < end:
        try:
            with urllib.request.urlopen(url, timeout=5) as r:
                if r.status < 500:
                    return
        except OSError:
            time.sleep(0.5)
    raise TimeoutError(f"{url} did not answer in {seconds:.0f} s")


@pytest.fixture(scope="module")
def site(request, tmp_path_factory):
    need(request, "atlas", GENERATED.build() / "registry.json", ATLAS_HINT)
    vite = FRONTEND / "node_modules" / ".bin" / "vite"
    if not vite.exists():
        pytest.fail(f"missing {vite} — run `npm install` in {FRONTEND}", pytrace=False)
    root = tmp_path_factory.mktemp("browser")
    cat = root / "catalogue"
    cat.mkdir()
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        shutil.copy2(p, cat / p.name)
    shutil.copy2(CURATED.waters.splits, root / "splits.json")
    (root / "marks.jsonl").write_text("")
    (root / "run").mkdir()
    api_port, vite_port = _free_port(), _free_port()
    env = {**os.environ, "CURATION_FIX": "1", "CURATION_ENTRIES_DIR": str(cat),
           "CURATION_SPLITS_JSON": str(root / "splits.json"), "CURATION_MARKS": str(root / "marks.jsonl"),
           "CURATION_BACKUP_DIR": str(root / "backups"), "CURATION_REACH_DIR": str(root / "run"),
           "CURATION_VITE_PORT": str(vite_port), "CURATION_API": f"http://127.0.0.1:{api_port}",
           "PYTHONPATH": str(REPO)}
    procs = [subprocess.Popen([sys.executable, "-m", "uvicorn", "app:app", "--host", "127.0.0.1",
                               "--port", str(api_port)], cwd=BACKEND, env=env,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL),
             subprocess.Popen([str(vite)], cwd=FRONTEND, env=env, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL)]
    try:
        _wait(f"http://127.0.0.1:{api_port}/api/mode", 120)
        _wait(f"http://127.0.0.1:{vite_port}/", 60)
        # warm the backend (registry, graph) so a page's first entry is not a 20 s cold load
        _wait(f"http://127.0.0.1:{api_port}/api/entries/{ADAM}", 300)
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            yield {"browser": browser, "url": f"http://127.0.0.1:{vite_port}/", "root": root}
            browser.close()
    finally:
        for p in procs:              # our own two processes, by handle — nothing else is touched
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                p.kill()


def _open(site, entry_id: str):
    page = site["browser"].new_page()
    page.goto(site["url"] + "#" + entry_id)
    page.wait_for_selector('[data-testid="entry-review"]', timeout=180_000)
    page.wait_for_selector('[data-testid="mode"]:has-text("FIX MODE")', timeout=30_000)
    return page


def _hash(page) -> str:
    return page.evaluate("decodeURIComponent(window.location.hash.slice(1))")


def test_the_draft_guard_asks_before_j_leaves_an_edit(site):
    page = _open(site, ADAM)
    page.fill('input[aria-label="scope_note"]', "an unsaved edit")
    page.locator('[data-testid="raw-name"]').click()              # the focus leaves the field
    asked: list[str] = []

    def dismiss(d):
        asked.append(d.message)
        d.dismiss()
    page.once("dialog", dismiss)
    page.keyboard.press("j")
    page.wait_for_timeout(500)
    assert asked == ["This entry has unsaved edits. Leave it and discard them?"]
    assert _hash(page) == ADAM                                    # cancel: still here
    assert page.input_value('input[aria-label="scope_note"]') == "an unsaved edit"

    page.once("dialog", lambda d: d.accept())
    page.keyboard.press("j")
    page.wait_for_function(f"decodeURIComponent(location.hash.slice(1)) !== {json.dumps(ADAM)}",
                           timeout=60_000)
    assert _hash(page) != ADAM                                    # OK: moved on, edit dropped
    page.close()


def test_enter_after_clicking_an_answer_records_the_mark(site):
    page = _open(site, ADAM)
    page.fill('input[aria-label="reviewer"]', "tester")
    page.locator('[data-testid="raw-name"]').click()
    first_rule = page.locator('[data-testid="zone-rules"] [data-rule-pick]')
    if first_rule.count():                                        # a zone entry: one rule
        first_rule.first.click()
    page.click('button[aria-label="T y"]')
    page.click('button[aria-label="R ?"]')
    assert page.evaluate("document.activeElement === document.body")
    page.keyboard.press("Enter")
    store = site["root"] / "marks.jsonl"
    end = time.time() + 60                    # the mark is written, then the screen moves on
    while time.time() < end and not store.read_text().strip():
        if page.locator('[data-testid="mark-errors"]').count():
            pytest.fail("refused: " + page.inner_text('[data-testid="mark-errors"]'))
        page.wait_for_timeout(250)
    lines = [json.loads(x) for x in store.read_text().splitlines() if x]
    assert [(m["target"], m["verdict"], m["answers"]) for m in lines] == [
        (ADAM, "unsure", {"T": "y", "R": "?"})]
    page.close()
