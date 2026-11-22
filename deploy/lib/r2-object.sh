# shellcheck shell=bash
# SOURCED by the deploy scripts: read one R2 object, telling "there is none" from "rclone failed" (T994 #19: an
# `rclone cat … 2>/dev/null || true` once read a dead rclone or expired credentials as "staging has no run timing").
#
#   r2_object <remote:path>          0 an object; 1 none (R2 answers a missing key as an empty "directory", rc 0);
#                                    2 rclone failed (its own words are on stderr)
#   r2_read <remote:path> <file>     0 the object is in <file>; 1 none (<file> is left empty); 2 rclone failed
#
# The same test as deploy/prod/upload.sh and rollback.sh `obj()` (T865), which keep their own copy.
r2_object() {
  local o
  o=$(rclone lsjson --stat "$1" --s3-no-check-bucket) || return 2
  [[ $o == *'"IsDir": false'* ]] || return 1
}
r2_read() {
  local rc=0
  : > "$2"
  r2_object "$1" || rc=$?
  (( rc == 0 )) || return "$rc"
  rclone cat "$1" --s3-no-check-bucket > "$2" || return 2
}
