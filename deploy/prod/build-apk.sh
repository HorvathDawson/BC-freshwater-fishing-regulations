#!/bin/bash
# PRODUCTION — THE OWNER RUNS THIS (T404). The production AAB for Google Play: deploy/staging/build-apk.sh --play
# --production (one build script, so the two cannot drift; that script holds every difference). Upload track:
# PLAY_TRACK (default internal — the owner promotes the same build to Closed testing in Play Console, T371; `alpha` is
# the Closed track's API name if uploading straight there).
#   CONFIRM_PROD=canifishthis.ca deploy/prod/build-apk.sh [--no-upload] [--no-email]
#   deploy/prod/build-apk.sh --dry-run          (prints the plan; builds and uploads nothing; no confirmation needed)
set -euo pipefail
cd "$(dirname "$0")/../.."
exec deploy/staging/build-apk.sh --play --production "$@"
