# (T313) Sourced by session.sh and session-ios.sh. `xcrun simctl` is Xcode's wrapper: when CoreSimulator is older than
# Xcode wants it first runs `xcodebuild -runFirstLaunch`, which waits for an admin password and hangs, unattended.
# Maestro runs `xcrun simctl list -j` at start, even for an Android device, so every flow hung. A PATH shim sends
# `xcrun simctl` to the CoreSimulator framework's own simctl (which answers either way); every other xcrun is untouched.
# Harmless once the owner has run `sudo xcodebuild -runFirstLaunch`. Needs $OUT (the test bed's out/ dir).
_DIRECT_SIMCTL=/Library/Developer/PrivateFrameworks/CoreSimulator.framework/Versions/A/Resources/bin/simctl
if [[ -x $_DIRECT_SIMCTL ]]; then
  mkdir -p "$OUT/xcrun-shim"
  cat > "$OUT/xcrun-shim/xcrun" <<SHIM
#!/bin/bash
[[ \$1 == simctl ]] && { shift; exec $_DIRECT_SIMCTL "\$@"; }
exec /usr/bin/xcrun "\$@"
SHIM
  chmod +x "$OUT/xcrun-shim/xcrun"
  export PATH="$OUT/xcrun-shim:$PATH"
fi
