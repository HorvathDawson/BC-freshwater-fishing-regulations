# shellcheck shell=bash
# SOURCED by deploy/staging/site.sh and deploy/prod/site.sh: one copy, so the two cannot drift (T788 #4).
#
#   site_bundle <who> <web export dir>    prints the ONE app bundle the export's index.html names
#                                         (/_expo/static/js/web/index-<hash>.js); refuses none or several, by name
#   site_stamp <who> <web export dir> <bundle>
#                                         sets SITE_COMMIT (THE EXPORT'S commit) and SITE_DIRTY, or refuses
#
# THE SITE IS BUILT FROM A COMMIT (2026-11-14: an export of a tree holding other lanes' uncommitted app/ edits went to
# staging; a tap opened nothing):
#   - a dirty app/ is refused unless SITE_DIRTY_OK=<reason>, printed (staging's site.sh did not ask on its own: only
#     site-all.sh did);
#   - the commit stamped in /site-release.json is the one the EXPORT was built from, read from its bundle (version-stamp.js
#     inlines `sha:"<commit>"` at export, which About shows), not HEAD at deploy time: production stamped the deploy's
#     HEAD. When HEAD has moved since the export, both are said, and whether app/ differs between them.
#   SITE_DIRTY (set by site-all.sh, which looks before its export) is kept when given; else app/ is looked at now.
site_bundle() {
  local who=$1 W=$2 names
  [[ -f $W/index.html ]] || { echo "$who: $W/index.html is missing: run the web export first" >&2; return 1; }
  names=$(grep -o '/_expo/static/js/web/index-[0-9a-f]*\.js' "$W/index.html" | sort -u) || {
    echo "$who: $W/index.html names no app bundle (/_expo/static/js/web/index-<hash>.js): run the web export again" >&2; return 1; }
  [[ $names != *$'\n'* ]] || { echo "$who: $W/index.html names more than one app bundle:" $names >&2; return 1; }
  printf '%s\n' "$names"
}

site_stamp() {
  local who=$1 W=$2 js=$3 shas sha dirty head same
  [[ -f $W$js ]] || { echo "$who: $W$js (the bundle index.html names) is missing" >&2; return 1; }
  shas=$(grep -o 'sha:"[0-9a-f]\{40\}"' "$W$js" | sort -u) || {
    echo "$who: $W$js carries no commit (sha:\"<40 hex>\", version-stamp.js): export from a git checkout" >&2; return 1; }
  [[ $shas != *$'\n'* ]] || { echo "$who: $W$js carries more than one commit:" $shas >&2; return 1; }
  sha=${shas#sha:\"}; sha=${sha%\"}
  dirty=$(git status --porcelain -- app) || { echo "$who: git status failed (above): app/ cannot be checked" >&2; return 1; }
  if [[ -n $dirty ]]; then
    if [[ -z ${SITE_DIRTY_OK:-} ]]; then
      echo "$who: app/ has uncommitted changes: the site must be built and deployed from a commit (a clean worktree:" \
           "git worktree add --detach <dir> <sha>), or SITE_DIRTY_OK=<reason>" >&2
      head -20 <<<"$dirty" >&2; return 1
    fi
    echo "$who: SITE_DIRTY_OK: $(wc -l <<<"$dirty" | tr -d ' ') uncommitted app/ path(s) - $SITE_DIRTY_OK"
  fi
  head=$(git rev-parse HEAD) || { echo "$who: git rev-parse HEAD failed (above)" >&2; return 1; }
  if [[ $sha != "$head" ]]; then
    if ! git cat-file -e "$sha^{commit}" 2>/dev/null; then   # rc 1 = not here; said, never a pass
      same="the export's commit is not in this checkout (fetch it to compare)"
    elif git diff --quiet "$sha" HEAD -- app; then same="app/ is the same in both"
    else same="app/ DIFFERS between them: the site is the export's app, as stamped"; fi
    echo "$who: the export was built at $sha; HEAD is $head ($same)"
  fi
  SITE_COMMIT=$sha
  SITE_DIRTY=${SITE_DIRTY:-$([[ -n $dirty ]] && echo true || echo false)}
}
