"""Shared Cloudflare R2 storage for all recurring/cron jobs (S3-compatible, boto3).

One transport for every cron (in-season, stocking, hydro) so there's a single
credential set and mental model. R2 speaks the S3 API, so this is plain boto3.

Credentials (env):
    R2_S3_ENDPOINT        e.g. https://<account>.r2.cloudflarestorage.com
                          (or derived from CLOUDFLARE_ACCOUNT_ID)
    R2_ACCESS_KEY_ID      R2 API token → Access Key ID
    R2_SECRET_ACCESS_KEY  R2 API token → Secret Access Key
    DEPLOY_ENV            staging | production (selects the bucket)
    R2_BUCKET             optional explicit bucket override

CLI (used by the shell scripts so they don't shell out to wrangler):
    python -m pipeline.recurring.r2_storage get  <key> <dest>
    python -m pipeline.recurring.r2_storage put  <src> <key> [--content-type ct]
    python -m pipeline.recurring.r2_storage put-tree <dir> <key_prefix>
    python -m pipeline.recurring.r2_storage exists <key>
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path
from typing import Optional

_BUCKET_BY_ENV = {
    "staging": "bc-fishing-regulations-staging",
    "production": "bc-fishing-regulations",
    "local": "bc-fishing-regulations-staging",
}


class LocalStorage:
    """Filesystem-backed storage (mirrors an R2 bucket under a local root).

    Used for tests and ``--local`` dry runs — identical orchestration, local
    transport.
    """

    def __init__(self, root: Path):
        self.root = Path(root)

    def _p(self, key: str) -> Path:
        return self.root / key

    def exists(self, key: str) -> bool:
        return self._p(key).exists()

    def get(self, key: str, dest: Path) -> bool:
        src = self._p(key)
        if not src.exists():
            return False
        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        return True

    def put(self, src: Path, key: str, content_type: Optional[str] = None) -> None:
        dst = self._p(key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    def get_keys(self, prefix: str) -> list[str]:
        base = self._p(prefix)
        if not base.exists():
            return []
        return [str(p.relative_to(self.root)) for p in base.rglob("*") if p.is_file()]


class R2Storage:
    """Cloudflare R2 via the S3-compatible API (boto3)."""

    def __init__(self, bucket: str, endpoint: str, access_key: str, secret_key: str):
        import boto3  # deferred: only needed on the R2 path

        self.bucket = bucket
        self._s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def get(self, key: str, dest: Path) -> bool:
        from botocore.exceptions import ClientError

        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        try:
            self._s3.download_file(self.bucket, key, str(dest))
            return True
        except ClientError:
            return False

    def put(self, src: Path, key: str, content_type: Optional[str] = None) -> None:
        extra = {"ContentType": content_type} if content_type else None
        self._s3.upload_file(str(src), self.bucket, key, ExtraArgs=extra)

    def get_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        token = None
        while True:
            kw = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            resp = self._s3.list_objects_v2(**kw)
            keys.extend(o["Key"] for o in resp.get("Contents", []))
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return keys


def resolve_bucket() -> str:
    env = os.environ.get("DEPLOY_ENV", "local")
    return os.environ.get("R2_BUCKET") or _BUCKET_BY_ENV.get(env, _BUCKET_BY_ENV["local"])


def storage_from_env(local_root: Optional[Path] = None):
    """Return a LocalStorage (if local_root) else an R2Storage from env creds."""
    if local_root is not None:
        return LocalStorage(local_root)

    endpoint = os.environ.get("R2_S3_ENDPOINT")
    account = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    if not endpoint and account:
        endpoint = f"https://{account}.r2.cloudflarestorage.com"
    access = os.environ.get("R2_ACCESS_KEY_ID")
    secret = os.environ.get("R2_SECRET_ACCESS_KEY")
    if not (endpoint and access and secret):
        raise RuntimeError(
            "R2 S3 credentials missing — set R2_S3_ENDPOINT (or CLOUDFLARE_ACCOUNT_ID), "
            "R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY (or pass a local dir)."
        )
    return R2Storage(resolve_bucket(), endpoint, access, secret)


def _content_type_for(path: Path) -> Optional[str]:
    return "application/json" if path.suffix == ".json" else None


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="R2 storage CLI (S3/boto3).")
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("get", help="download a key to a local path")
    g.add_argument("key")
    g.add_argument("dest", type=Path)

    pu = sub.add_parser("put", help="upload a local file to a key")
    pu.add_argument("src", type=Path)
    pu.add_argument("key")
    pu.add_argument("--content-type", default=None)

    pt = sub.add_parser("put-tree", help="upload every file under a dir to key_prefix/…")
    pt.add_argument("dir", type=Path)
    pt.add_argument("key_prefix")

    ex = sub.add_parser("exists", help="exit 0 if key exists, 1 otherwise")
    ex.add_argument("key")

    args = p.parse_args(argv)
    storage = storage_from_env()

    if args.cmd == "get":
        ok = storage.get(args.key, args.dest)
        if not ok:
            print(f"not found: {args.key}", file=sys.stderr)
            return 1
        print(f"↓ {args.key} → {args.dest}")
        return 0

    if args.cmd == "put":
        ct = args.content_type or _content_type_for(args.src)
        storage.put(args.src, args.key, content_type=ct)
        print(f"↑ {args.src} → {args.key}")
        return 0

    if args.cmd == "put-tree":
        n = 0
        for f in sorted(pth for pth in args.dir.rglob("*") if pth.is_file()):
            rel = f.relative_to(args.dir).as_posix()
            storage.put(f, f"{args.key_prefix}/{rel}", content_type=_content_type_for(f))
            n += 1
        print(f"↑ {n} files → {args.key_prefix}/")
        return 0

    if args.cmd == "exists":
        return 0 if storage.exists(args.key) else 1

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
