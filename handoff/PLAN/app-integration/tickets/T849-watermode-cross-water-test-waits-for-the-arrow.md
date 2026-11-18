# T849 — WaterMode's cross-water test waits for the ▲ before it taps (a CI flake)

Status: OPEN (after 0.8.0).

Source: app CI on d6cbad35d, attempt 2 (2026-11-18): "across waters: ▲ on the Vedder opens the Chilliwack's map at its
lowest part IN ITS PLACE" failed at 22.1 s, the first `until()` (20 s) after the "Upstream into Chilliwack River" tap.
Passed on attempt 1 and locally 9/9 on the same code. Under CI load the tap lands before the screen is ready, nothing
moves, and the wait can never succeed.

## Fix

Wait until the arrow is ready (the screen settled on the Vedder's top part, the button enabled) before each tap, or
re-tap inside the wait; prove it under load (several suites at once). Not an app fault; the app's own tap handling is
untouched.
