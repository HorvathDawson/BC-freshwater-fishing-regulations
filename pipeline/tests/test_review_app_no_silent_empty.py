"""T434 (audit 2026-11-05): the review app's backend turns no error into an empty answer.

  applied keys   a failure in `proposals.applied_keys` (T421: no build) reaches the request as a 503 naming it — it was
                 caught into an empty set, and every landed fix read "binding moved"
  shutdown       the write-through drains (`wait_idle`) when the app stops
  splits         no "splits" review domain: a split mark (history, T184) applies nothing
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from pipeline.regs.review import proposals as P
from pipeline.tests.conftest import curated_root

BACKEND = Path(__file__).resolve().parents[2] / "curation-review" / "backend"


@pytest.fixture()
def app(tmp_path, monkeypatch):
    curated_root(monkeypatch, tmp_path / "curated")
    monkeypatch.setenv("CURATION_BACKUP_DIR", str(tmp_path / "backups"))
    monkeypatch.syspath_prepend(str(BACKEND))
    for mod in ("app", "reuse", "writes", "review", "write_through"):
        sys.modules.pop(mod, None)
    import app as app_mod                                                  # noqa: E402
    yield app_mod
    for mod in ("app", "reuse", "writes", "review", "write_through"):
        sys.modules.pop(mod, None)


def test_a_failed_applied_keys_is_a_503_that_says_why(app, monkeypatch):
    from fastapi.testclient import TestClient

    def boom(*_a, **_k):
        raise FileNotFoundError("applied_keys: no registry at /x (build the atlas)")
    monkeypatch.setattr(P, "applied_keys", boom)
    import reuse
    # the entries and the fingerprint context read the built registry BEFORE applied keys run; without a build (CI)
    # their own FileNotFoundError answered first and the failure under test never ran — neither is under test
    ctx = object()
    monkeypatch.setattr(reuse, "_review_context", lambda: ctx)
    monkeypatch.setattr(reuse, "_all_entries", lambda: [])
    reuse._applied_keys_for.cache_clear()
    with pytest.raises(FileNotFoundError):
        reuse.applied_keys()
    r = TestClient(app.app, raise_server_exceptions=False).get("/api/marks/health")
    assert r.status_code == 503
    assert "no registry at /x" in r.json()["detail"]


def test_shutdown_drains_the_write_through(app, monkeypatch):
    from fastapi.testclient import TestClient
    waited = []
    monkeypatch.setattr(app.WRITE_THROUGH, "wait_idle", lambda timeout=60.0: waited.append(timeout) or True)
    with TestClient(app.app):
        pass
    assert waited == [30]


def test_no_splits_domain():
    from pipeline.regs.review import apply as AP
    from pipeline.regs.review import export as E
    assert "splits" not in E.DOMAINS and "splits" not in AP.PROPOSAL_DOMAINS
    assert AP.domain_of("split") is None
