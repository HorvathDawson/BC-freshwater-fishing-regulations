# (T595) THE TEST TAG, in one place: every build the test bed or an emulator/simulator check makes carries
# EXPO_PUBLIC_TEST_REPORTS=1, so its crash and problem reports are tagged `test` (the staging worker stores those apart).
# Sourced by session.sh (the Android emulator APK) and deploy/staging/build-ios-sim.sh (the simulator build). Never by a
# store or owner-phone build (build-apk.sh, build-ios-testflight.sh); `test-tag.test.ts` pins both lists.
export EXPO_PUBLIC_TEST_REPORTS=1
