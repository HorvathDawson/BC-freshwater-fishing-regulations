#!/usr/bin/env python3
"""(T1170) THE PINNED MAPLIBRE ANDROID LIBRARY AS A STORED BUILD: built once per KEY on a GitHub runner
(.github/workflows/maplibre-android.yml), kept as a GitHub Release asset, recorded in maplibre-android.lock.json
beside pin.env, and downloaded + verified by build.sh instead of compiling it here.

  stored.py key [--github-output FILE]     the key and the inputs it is made of (JSON on stdout)
  stored.py pack <maven dir> <file.tar>    build.sh's output folder as one deterministic tar; prints sha256 and size
  stored.py record <file.tar> <run url>    the lock record for that file (published: false) on stdout
  stored.py fetch --work DIR --out DIR     what build.sh runs without --compile: download the lock's file, verify
                                           sha256 AND size, unpack it to --out. Any refusal exits 1 and says why.

THE KEY is the sha256 of every input that changes the stored file: the pin (MLN_COMMIT, MLN_VERSION), build.sh's and
jobpool.init.gradle's code (comment and blank lines dropped: a reworded comment is not a rebuild), the NDK, CMake,
Gradle and JDK build.sh names and checks, the ABI list (MLN_ABIS, else build.sh's default) and PACK_FORMAT. MLN_JOBS is
not in it: the job pool changes nothing in the AAR (jobpool.init.gradle). The pin moving, or any of those, makes a new
key; a lock recording another key is refused (never a stored file built from other inputs).

NO SILENT FALLBACK: a record that is unpublished, for another key, a failed download, a wrong sha256 or size, or an
--out that already holds something, each stops the build with its cause. build.sh compiles locally ONLY with its
explicit --compile flag (the runner's build, or a pin move before the workflow has stored it). Python 3.9+, stdlib only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tarfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import BinaryIO, Callable, Optional

HERE = Path(__file__).resolve().parent
LOCK = HERE / "maplibre-android.lock.json"
REPO = "canifishthis/canifishthis"
#: bump when pack() changes its bytes (test_maplibre_stored.py pins pack()'s output for a fixed tree)
PACK_FORMAT = 1
ARTIFACT = "android-sdk-vulkan"


class Refused(Exception):
    """A stored build that must not be used, with the reason a person reads."""


def _code(text: str, comment: str) -> str:
    """the code lines of a script: full-line comments and blank lines dropped"""
    return "".join(ln + "\n" for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith(comment))


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _setting(sh: str, name: str) -> str:
    m = re.search(rf"^{name}=(\S+)", sh, re.M)
    if not m:
        raise Refused(f"build.sh has no {name}= line: the key cannot name it")
    return m.group(1)


def pin(here: Path = HERE) -> dict[str, str]:
    out = {}
    for line in (here / "pin.env").read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            k, v = line.split("=", 1)
            out[k] = v
    return out


def key_inputs(here: Path = HERE, env: Optional[dict] = None) -> dict:
    env = os.environ if env is None else env
    sh = (here / "build.sh").read_text()
    p = pin(here)
    m = re.search(r'^ABIS=\$\{MLN_ABIS:-"([^"]+)"\}', sh, re.M)
    if not m:
        raise Refused("build.sh has no ABIS=${MLN_ABIS:-\"...\"} line: the key cannot name the ABIs")
    return {
        "mln_commit": p["MLN_COMMIT"],
        "mln_version": p["MLN_VERSION"],
        "build_sh": _sha(_code(sh, "#").encode()),
        "jobpool_init_gradle": _sha(_code((here / "jobpool.init.gradle").read_text(), "//").encode()),
        "ndk": _setting(sh, "NDK"),
        "cmake": _setting(sh, "CMAKE"),
        "gradle": _setting(sh, "GRADLE"),
        "jdk": _setting(sh, "JDK"),
        "abis": env.get("MLN_ABIS") or m.group(1),
        "pack_format": PACK_FORMAT,
    }


def key_of(inputs: dict) -> str:
    return _sha(json.dumps(inputs, sort_keys=True, separators=(",", ":")).encode())


def names(inputs: dict, key: str) -> dict[str, str]:
    """the release tag (never a v* tag: those are the app's versions) and the file name for a key"""
    tag = f"maplibre-android-{key[:16]}"
    file = f"maplibre-android-{inputs['mln_version']}-{key[:16]}.tar"
    return {"tag": tag, "file": file, "url": f"https://github.com/{REPO}/releases/download/{tag}/{file}"}


# ── pack: one deterministic tar of the maven folder (sorted, no owners, no times, fixed modes; uncompressed: the AAR
#    is a zip already, and no zlib version can change the bytes)
def pack(src: Path, dest: Path) -> tuple[str, int]:
    src = Path(src)
    paths = sorted(p for p in src.rglob("*") if not p.is_symlink())
    with dest.open("wb") as f, tarfile.open(fileobj=f, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for p in paths:
            rel = p.relative_to(src).as_posix()
            ti = tarfile.TarInfo(rel)
            ti.mtime, ti.uid, ti.gid, ti.uname, ti.gname = 0, 0, 0, "", ""
            if p.is_dir():
                ti.type, ti.mode = tarfile.DIRTYPE, 0o755
                tar.addfile(ti)
            else:
                ti.mode, ti.size = 0o644, p.stat().st_size
                with p.open("rb") as fh:
                    tar.addfile(ti, fh)
    data = dest.read_bytes()
    return _sha(data), len(data)


def record(tar: Path, run_url: str, here: Path = HERE) -> dict:
    inputs = key_inputs(here)
    key = key_of(inputs)
    n = names(inputs, key)
    if Path(tar).name != n["file"]:
        raise Refused(f"{tar} is not named {n['file']} (the key's file name)")
    data = Path(tar).read_bytes()
    return {"key": key, "inputs": inputs, **n, "sha256": _sha(data), "size": len(data), "run": run_url,
            "published": False}


# ── fetch
Opener = Callable[[str], BinaryIO]


def _open(url: str) -> BinaryIO:
    return urllib.request.urlopen(url, timeout=120)   # noqa: S310 (the url is the lock's https release url)


def _check_unpacked(out: Path, p: dict[str, str]) -> Path:
    """build-links.sh's rule (mln_pinned_aar): exactly one android-sdk-vulkan AAR, named for MLN_VERSION, and
    SOURCE.txt naming MLN_COMMIT"""
    aars = sorted(out.rglob(f"{ARTIFACT}-*.aar"))
    want = f"{ARTIFACT}-{p['MLN_VERSION']}.aar"
    if len(aars) != 1 or aars[0].name != want:
        raise Refused(f"the unpacked stored build in {out} holds {[a.name for a in aars]}, not exactly {want}")
    src = (out / "SOURCE.txt").read_text().split() if (out / "SOURCE.txt").is_file() else []
    if len(src) < 2 or src[1] != p["MLN_COMMIT"]:
        raise Refused(f"the unpacked stored build's SOURCE.txt does not name the pin {p['MLN_COMMIT']}")
    return aars[0]


def fetch(work: Path, out: Path, lock: Path = LOCK, here: Path = HERE, opener: Opener = _open,
          env: Optional[dict] = None) -> Path:
    """download the lock's file, verify it, unpack it to `out`; returns the AAR. Raises Refused with the cause."""
    if not lock.is_file():
        raise Refused(f"no {lock}: no stored build is recorded")
    rec = json.loads(lock.read_text())
    inputs = key_inputs(here, env)
    key = key_of(inputs)
    if rec.get("key") != key:
        was = rec.get("inputs") or {}
        moved = sorted(k for k in set(inputs) | set(was) if inputs.get(k) != was.get(k))
        raise Refused(f"{lock.name} records key {str(rec.get('key'))[:16]}, the current key is {key[:16]} "
                      f"(changed: {', '.join(moved) or 'unknown'}): the stored file was built from other inputs. "
                      "Run the maplibre-android workflow for this commit and record its file")
    if rec.get("published") is not True:
        raise Refused(f"{lock.name}'s record for key {key[:16]} is not published (published: "
                      f"{json.dumps(rec.get('published'))}): its file is not a release asset the owner approved")
    want_url = names(inputs, key)["url"]
    if rec.get("url") != want_url:
        raise Refused(f"{lock.name} names url {rec.get('url')}, not this key's release asset {want_url}")
    out = Path(out)
    if out.exists() and any(out.iterdir()):
        raise Refused(f"{out} already holds files: never mixed with a stored build (remove it, or set MLN_OUT)")
    work = Path(work)
    work.mkdir(parents=True, exist_ok=True)
    part = work / (rec["file"] + ".part")
    h, size = hashlib.sha256(), 0
    try:
        with opener(want_url) as r, part.open("wb") as f:
            for chunk in iter(lambda: r.read(1 << 20), b""):
                h.update(chunk)
                size += len(chunk)
                f.write(chunk)
    except (urllib.error.URLError, OSError) as e:
        raise Refused(f"download failed: {want_url}: {e}") from e
    got = h.hexdigest()
    if got != rec["sha256"] or size != rec["size"]:
        raise Refused(f"the downloaded file does not match the lock: {want_url}\n"
                      f"  lock     sha256 {rec['sha256']} size {rec['size']}\n"
                      f"  download sha256 {got} size {size}\n"
                      f"  (kept for a look: {part})")
    tmp = out.parent / (out.name + ".unpacking")
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    with tarfile.open(part, mode="r:") as tar:
        members = tar.getmembers()
        for m in members:
            if not (m.isfile() or m.isdir()) or m.name.startswith("/") or ".." in Path(m.name).parts:
                raise Refused(f"{part} holds {m.name!r}: not a plain relative file or folder")
        kw = {"filter": "data"} if hasattr(tarfile, "data_filter") else {}
        tar.extractall(tmp, members=members, **kw)   # noqa: S202 (every member checked above)
    aar = _check_unpacked(tmp, pin(here))
    (tmp / "STORED.txt").write_text(f"stored build {key} from {want_url} sha256 {got}\n")
    if out.exists():
        out.rmdir()
    tmp.rename(out)
    part.unlink()
    return out / aar.relative_to(tmp)


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    k = sub.add_parser("key")
    k.add_argument("--github-output")
    pk = sub.add_parser("pack")
    pk.add_argument("src")
    pk.add_argument("dest")
    rc = sub.add_parser("record")
    rc.add_argument("tar")
    rc.add_argument("run")
    f = sub.add_parser("fetch")
    f.add_argument("--work", required=True)
    f.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    try:
        if a.cmd == "key":
            inputs = key_inputs()
            key = key_of(inputs)
            n = names(inputs, key)
            print(json.dumps({"key": key, **n, "inputs": inputs}, indent=2))
            if a.github_output:
                with open(a.github_output, "a") as g:
                    g.write(f"key={key}\ntag={n['tag']}\nfile={n['file']}\nversion={inputs['mln_version']}\n")
        elif a.cmd == "pack":
            sha, size = pack(Path(a.src), Path(a.dest))
            print(f"{Path(a.dest).name} sha256 {sha} size {size}")
        elif a.cmd == "record":
            print(json.dumps(record(Path(a.tar), a.run), indent=2))
        else:
            aar = fetch(Path(a.work), Path(a.out))
            print(f"stored MapLibre build verified and unpacked: {aar}")
    except Refused as e:
        print(f"stored.py {a.cmd}: REFUSED: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
