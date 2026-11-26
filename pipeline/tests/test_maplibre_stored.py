"""(T1170) The stored MapLibre Android library: app/tools/maplibre-android/stored.py, the consumer build.sh runs
without --compile. A fake downloader (bytes in memory) and a local file URL stand in for the GitHub Release; nothing
here downloads or builds anything.

Each refusal is one test: the right file is unpacked; a wrong sha256, a wrong size, another key (the pin moved), an
unpublished record, a failed download, a record naming another url, a non-empty --out and a tar reaching outside --out
are each refused with their cause, and nothing is unpacked."""
from __future__ import annotations

import importlib.util
import io
import json
import re
import shutil
import subprocess
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
TOOL = ROOT / "app" / "tools" / "maplibre-android"


def _load():
    spec = importlib.util.spec_from_file_location("mln_stored", TOOL / "stored.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


S = _load()


def _here(tmp: Path) -> Path:
    """a copy of the tool folder's key inputs (pin.env, build.sh, jobpool.init.gradle), free to change"""
    h = tmp / "tool"
    h.mkdir()
    for f in ("pin.env", "build.sh", "jobpool.init.gradle"):
        shutil.copy(TOOL / f, h / f)
    return h


def _maven(parent: Path, here: Path) -> Path:
    """a stand-in for build.sh's maven folder: the one AAR named for the pin and SOURCE.txt naming its commit"""
    p = S.pin(here)
    repo = parent / "maven"
    aar = repo / f"org/maplibre/gl/android-sdk-vulkan/{p['MLN_VERSION']}/android-sdk-vulkan-{p['MLN_VERSION']}.aar"
    aar.parent.mkdir(parents=True)
    with zipfile.ZipFile(aar, "w") as z:
        z.writestr("jni/arm64-v8a/libmaplibre.so", "so")
    (repo / "SOURCE.txt").write_text(f"maplibre-native {p['MLN_COMMIT']} (upstream, no patch), version "
                                     f"{p['MLN_VERSION']}, built 2026-11-26T00:00:00Z from /src\n")
    return repo


def _stored(tmp: Path, **change):
    """(here, lock path, the tar's bytes): a packed stand-in and its lock record, published unless changed"""
    here = _here(tmp)
    inputs = S.key_inputs(here, env={})
    key = S.key_of(inputs)
    n = S.names(inputs, key)
    tar = tmp / n["file"]
    sha, size = S.pack(_maven(tmp, here), tar)
    rec = {"key": key, "inputs": inputs, **n, "sha256": sha, "size": size,
           "run": "https://github.com/canifishthis/canifishthis/actions/runs/1", "published": True, **change}
    lock = tmp / "maplibre-android.lock.json"
    lock.write_text(json.dumps(rec))
    return here, lock, tar.read_bytes()


def _serve(data: bytes, seen: list | None = None):
    def opener(url):
        if seen is not None:
            seen.append(url)
        return io.BytesIO(data)
    return opener


def _fetch(tmp, here, lock, opener):
    return S.fetch(tmp / "work", tmp / "out", lock=lock, here=here, opener=opener, env={})


def test_the_right_file_is_verified_and_unpacked(tmp_path):
    here, lock, data = _stored(tmp_path)
    seen: list = []
    aar = _fetch(tmp_path, here, lock, _serve(data, seen))
    rec = json.loads(lock.read_text())
    assert seen == [rec["url"]] and rec["url"].startswith(
        "https://github.com/canifishthis/canifishthis/releases/download/maplibre-android-")
    assert aar.is_file() and aar.name == f"android-sdk-vulkan-{S.pin(here)['MLN_VERSION']}.aar"
    out = tmp_path / "out"
    assert (out / "SOURCE.txt").read_text().split()[1] == S.pin(here)["MLN_COMMIT"]
    assert rec["key"] in (out / "STORED.txt").read_text()
    assert not list((tmp_path / "work").iterdir())          # the download is removed once unpacked


def test_a_local_file_url_serves_it_too(tmp_path):
    here, lock, data = _stored(tmp_path)
    f = tmp_path / "served.tar"
    f.write_bytes(data)
    _fetch(tmp_path, here, lock, lambda url: urllib.request.urlopen(f.as_uri()))
    assert (tmp_path / "out" / "SOURCE.txt").is_file()


def test_a_wrong_sha256_is_refused_naming_the_url_and_both_hashes(tmp_path):
    here, lock, data = _stored(tmp_path)
    bad = data[:-1] + bytes([data[-1] ^ 1])                 # same size, one bit flipped
    with pytest.raises(S.Refused) as e:
        _fetch(tmp_path, here, lock, _serve(bad))
    rec = json.loads(lock.read_text())
    msg = str(e.value)
    assert rec["url"] in msg and rec["sha256"] in msg and S._sha(bad) in msg
    assert not (tmp_path / "out").exists()


def test_a_wrong_size_is_refused(tmp_path):
    here, lock, data = _stored(tmp_path)
    rec = json.loads(lock.read_text())
    rec["size"] += 1                                         # the hash still matches: only the size check refuses
    lock.write_text(json.dumps(rec))
    with pytest.raises(S.Refused, match=f"size {rec['size']}"):
        _fetch(tmp_path, here, lock, _serve(data))
    assert not (tmp_path / "out").exists()


def test_the_pin_moved_so_the_stored_file_is_refused(tmp_path):
    here, lock, data = _stored(tmp_path)
    pin = (here / "pin.env").read_text()
    (here / "pin.env").write_text(re.sub(r"^MLN_COMMIT=\w+$", "MLN_COMMIT=" + "e" * 40, pin, flags=re.M))
    with pytest.raises(S.Refused, match=r"changed: mln_commit\)"):
        _fetch(tmp_path, here, lock, _serve(data))


def test_a_build_setting_moved_so_the_stored_file_is_refused(tmp_path):
    here, lock, data = _stored(tmp_path)
    sh = (here / "build.sh").read_text()
    (here / "build.sh").write_text(sh.replace("\nJDK=21", "\nJDK=17", 1))
    with pytest.raises(S.Refused, match=r"changed: build_sh, jdk\)"):
        _fetch(tmp_path, here, lock, _serve(data))
    (here / "build.sh").write_text(sh)
    with pytest.raises(S.Refused, match=r"changed: abis\)"):    # an ABI list from the environment is a key input too
        S.fetch(tmp_path / "work", tmp_path / "out", lock=lock, here=here, opener=_serve(data),
                env={"MLN_ABIS": "arm64-v8a"})


def test_an_unpublished_record_is_not_used(tmp_path):
    here, lock, data = _stored(tmp_path, published=False)
    seen: list = []
    with pytest.raises(S.Refused, match="is not published"):
        _fetch(tmp_path, here, lock, _serve(data, seen))
    assert seen == []                                        # not even downloaded


def test_a_record_naming_another_url_is_refused(tmp_path):
    here, lock, data = _stored(tmp_path, url="https://example.com/x.tar")
    with pytest.raises(S.Refused, match="not this key's release asset"):
        _fetch(tmp_path, here, lock, _serve(data))


def test_a_failed_download_is_refused_naming_the_url(tmp_path):
    here, lock, _ = _stored(tmp_path)

    def gone(url):
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
    with pytest.raises(S.Refused, match=r"download failed: https://github\.com/.*404"):
        _fetch(tmp_path, here, lock, gone)


def test_an_out_that_holds_files_is_never_mixed(tmp_path):
    here, lock, data = _stored(tmp_path)
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "old.aar").write_text("x")
    with pytest.raises(S.Refused, match="already holds files"):
        _fetch(tmp_path, here, lock, _serve(data))


def test_a_tar_reaching_outside_out_is_refused(tmp_path):
    import tarfile
    here, lock, _ = _stored(tmp_path)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as t:
        ti = tarfile.TarInfo("../escape.txt")
        ti.size = 1
        t.addfile(ti, io.BytesIO(b"x"))
    data = buf.getvalue()
    rec = json.loads(lock.read_text())
    rec.update(sha256=S._sha(data), size=len(data))
    lock.write_text(json.dumps(rec))
    with pytest.raises(S.Refused, match="not a plain relative file"):
        _fetch(tmp_path, here, lock, _serve(data))
    assert not (tmp_path / "escape.txt").exists()


def test_the_key_ignores_comments_and_names_every_setting(tmp_path):
    here = _here(tmp_path)
    a = S.key_inputs(here, env={})
    assert set(a) == {"mln_commit", "mln_version", "build_sh", "jobpool_init_gradle", "ndk", "cmake", "gradle", "jdk",
                      "abis", "pack_format"}
    assert (a["ndk"], a["cmake"], a["gradle"], a["jdk"], a["abis"]) == ("28.2.13676358", "3.31.6", "9.5.1", "21",
                                                                         "arm64-v8a x86_64")
    (here / "build.sh").write_text((here / "build.sh").read_text() + "\n# a reworded comment\n\n")
    assert S.key_of(S.key_inputs(here, env={})) == S.key_of(a)
    (here / "build.sh").write_text((here / "build.sh").read_text() + "echo a code line\n")
    assert S.key_of(S.key_inputs(here, env={})) != S.key_of(a)


def test_pack_is_deterministic_and_its_bytes_are_pinned(tmp_path):
    """a change to pack()'s bytes fails here: bump PACK_FORMAT with it (the key then names the new format)"""
    src = tmp_path / "m"
    (src / "a" / "b").mkdir(parents=True)
    (src / "a" / "b" / "f.txt").write_text("one\n")
    (src / "SOURCE.txt").write_text("two\n")
    one = S.pack(src, tmp_path / "1.tar")
    (src / "SOURCE.txt").touch()                              # a new mtime changes nothing
    assert S.pack(src, tmp_path / "2.tar") == one
    assert S.PACK_FORMAT == 1
    assert one == ("e0e92e5acb31620ffceb4a2e90cceba8438c9e5bd28bb877dc0b3d83f44aa9b0", 10240), one


def test_build_sh_without_compile_only_fetches_and_says_why_it_refuses(tmp_path):
    """the real build.sh, no --compile: it execs stored.py fetch (no clone, no gradle) and a lock that is not a
    published record for this key stops it, said"""
    r = subprocess.run(["bash", str(TOOL / "build.sh"), str(tmp_path / "work")], capture_output=True, text=True,
                       env={"PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin", "MLN_OUT": str(tmp_path / "out")},
                       timeout=60)
    assert r.returncode == 1, (r.stdout, r.stderr)
    assert "stored.py fetch: REFUSED:" in r.stderr, r.stderr
    assert not (tmp_path / "work" / "src").exists() and not (tmp_path / "out").exists()
    sh = (TOOL / "build.sh").read_text()
    assert 'if [[ -z $COMPILE ]]; then exec python3 "$HERE/stored.py" fetch --work "$WORK" --out "$OUT"; fi' in sh
    assert sh.index("exec python3") < sh.index("git clone")
