# shellcheck shell=bash
# SOURCED by every deploy/prod script that takes --bucket (T865 review, 2026-11-18): the production guard compares the
# BARE name, so `--bucket bc-fishing-regulations/`, `r2:bc-fishing-regulations` or `BC-Fishing-Regulations` cannot slip
# past CONFIRM_PROD as "some other bucket".
#
#   BUCKET=$(bucket_name "$2") || exit 2
#
# prints the bucket name with any `<remote>:` prefix and leading/trailing slashes dropped, lower-cased (R2 names are);
# refuses (exit 2, said) an empty name or a path inside a bucket.
bucket_name() {
  local b=${1#*:}
  while [[ $b == /* ]]; do b=${b#/}; done
  while [[ $b == */ ]]; do b=${b%/}; done
  b=$(printf '%s' "$b" | tr '[:upper:]' '[:lower:]')
  if [[ -z $b || $b == */* ]]; then
    echo "--bucket: '$1' is not a bucket name (a bare name, e.g. bc-fishing-regulations-staging)" >&2; return 2
  fi
  printf '%s\n' "$b"
}
