# T820 — The salmon year bar is empty on a closed day instead of the full red closed stripe

Status: OPEN (app; app chat)

Source: owner 2026-11-16, in the app chat (~21:37, on iPhone build 36), verbatim: "also the calendar bar doesnt have a full red line for salmon when closed but
instead it is empty with no color on the calendar bar..".

## History (from the main chat)
T227 deliberately kept the year bar off for a region-wide (header) closure, and T796 X1 (a0504abc1) kept "no year bar"
for the region-closed card. The owner's words now overrule that for salmon: a closed day draws the full red closed
stripe, never an empty bar. If the emptiness comes from the chain's map runs (data), send the main chat the sid and day.
