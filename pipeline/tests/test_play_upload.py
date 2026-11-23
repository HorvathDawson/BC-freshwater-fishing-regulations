"""(T1003) deploy/staging/play-upload.py sends R8's mapping with every AAB, in the same edit, and a missing one stops it.

R8 is on for every Android release build, so Play needs each build's mapping.txt (Android vitals and Play's crash
reports read the renamed Java/Kotlin frames back with it). The upload order is pinned against a stand-in Play session:
edits.insert -> bundles.upload -> deobfuscationfiles.upload (type proguard, that versionCode) -> tracks.update -> commit;
a missing, empty or unnamed mapping stops before any Play call (before the service-account key is even read).
"""
from __future__ import annotations

import importlib.util
import subprocess
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "deploy/staging/play-upload.py"


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True, timeout=60)


@pytest.mark.parametrize("mapping", [None, "missing", "empty"])
def test_a_missing_mapping_stops_the_upload_before_play(tmp_path, mapping):
    aab = tmp_path / "app.aab"
    aab.write_bytes(b"PK")
    extra: list[str] = []
    if mapping == "missing":
        extra = ["--mapping", str(tmp_path / "nope.txt")]
    elif mapping == "empty":
        (tmp_path / "m.txt").write_text("")
        extra = ["--mapping", str(tmp_path / "m.txt")]
    # the key does not exist either: the mapping check must come first (no Play call, no credentials read)
    r = _run("--aab", str(aab), "--package", "x.y", "--key", str(tmp_path / "no-key.json"), *extra)
    assert r.returncode != 0
    if mapping is None:
        assert "--mapping <r8-mapping.txt> is required with --aab" in r.stderr
    else:
        assert "(missing or empty); nothing uploaded" in r.stderr
    assert "service-account key" not in r.stderr


class _Resp:
    def __init__(self, status: int = 200, body: dict | None = None):
        self.status_code, self._body = status, body or {}
        self.content = b"x" if body else b""
        self.text = str(body)

    def json(self):
        return self._body


class _Session:
    """Play stand-in: records every call; the edit is "e1", the bundle versionCode 77."""
    calls: list[tuple[str, str]] = []
    fail_mapping = False

    def __init__(self, _creds):
        pass

    def post(self, url, **kw):
        if "data" in kw and hasattr(kw["data"], "read"):
            kw["data"].read()
        _Session.calls.append(("POST", url))
        if url.endswith("/edits"):
            return _Resp(body={"id": "e1"})
        if url.endswith("/edits/e1/bundles"):
            return _Resp(body={"versionCode": 77})
        if "/deobfuscationFiles/" in url:
            return _Resp(500, {"error": "no"}) if _Session.fail_mapping else _Resp(body={"deobfuscationFile": {}})
        return _Resp(body={"ok": 1})

    def put(self, url, **kw):
        _Session.calls.append(("PUT", url))
        return _Resp(body={"ok": 1})

    def get(self, url, **kw):
        _Session.calls.append(("GET", url))
        return _Resp(body={})

    def delete(self, url, **kw):
        _Session.calls.append(("DELETE", url))
        return _Resp()


def _module(monkeypatch):
    """the script with the Google libraries replaced by the stand-in (CI's pipeline venv has no google-auth)"""
    sa = types.SimpleNamespace(Credentials=types.SimpleNamespace(from_service_account_file=lambda *a, **k: None))
    fakes = {"google": types.ModuleType("google"), "google.auth": types.ModuleType("google.auth"),
             "google.auth.transport": types.ModuleType("google.auth.transport"),
             "google.auth.transport.requests": types.SimpleNamespace(AuthorizedSession=_Session),
             "google.oauth2": types.SimpleNamespace(service_account=sa), "google.oauth2.service_account": sa}
    for name, m in fakes.items():
        monkeypatch.setitem(sys.modules, name, m)
    spec = importlib.util.spec_from_file_location("play_upload", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _Session.calls = []
    return mod


def _argv(tmp_path):
    (tmp_path / "app.aab").write_bytes(b"PK")
    (tmp_path / "r8-mapping.txt").write_text("a.B -> c:\n")
    (tmp_path / "key.json").write_text("{}")
    return ["play-upload.py", "--aab", str(tmp_path / "app.aab"), "--mapping", str(tmp_path / "r8-mapping.txt"),
            "--package", "x.y", "--key", str(tmp_path / "key.json")]


def test_the_mapping_goes_in_the_same_edit_after_the_bundle_and_before_the_track(tmp_path, monkeypatch):
    mod = _module(monkeypatch)
    _Session.fail_mapping = False
    monkeypatch.setattr(sys, "argv", _argv(tmp_path))
    mod.main()
    urls = [u.split("/applications/x.y")[-1] for _, u in _Session.calls]
    assert urls == ["/edits", "/edits/e1/bundles", "/edits/e1/apks/77/deobfuscationFiles/proguard",
                    "/edits/e1/tracks/internal", "/edits/e1:commit"]


def test_a_refused_mapping_stops_before_the_track_and_the_commit(tmp_path, monkeypatch):
    mod = _module(monkeypatch)
    _Session.fail_mapping = True
    monkeypatch.setattr(sys, "argv", _argv(tmp_path))
    with pytest.raises(SystemExit) as e:
        mod.main()
    assert "deobfuscationfiles.upload" in str(e.value)
    assert not any("/tracks/" in u or ":commit" in u for _, u in _Session.calls)
