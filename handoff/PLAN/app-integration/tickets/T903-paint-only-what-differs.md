# T903 — Map modes paint only what differs from a default (Salmon red, Conditions/Runs grey)

Status: IN PROGRESS (app chat; lane app/t903-paint-default; measured on the emulator; lands in 0.8.2 only if it clearly wins).

Owner 2026-11-19 ~01:33 (app chat), verbatim: "i feel like salmon painting is still slow. maybe it should default stream
color to red then only paint things different then that? might reduce the load when on salmon view? then goes back to
default for normal rules after to keep it fast too. similar default grey for conditions and runs?"

Idea: today a mode switch writes a per-feature colour (feature-state) for every drawn water. Instead, each mode's layer
paint gets a DEFAULT (Salmon: the most common answer, likely closed red; Conditions / Runs: grey / no data), and
feature-state is written only for the waters whose answer differs; switching back to Trout clears only those. Fewer
writes per switch and per tile load. Prove first: count each mode's states over the live bundle (how many differ from
the default), then bench mode switch + pinch/pan on Salmon, Conditions and Runs (testbed zoom-pinch, a mode-switch
scenario) before and after on the emulator, patched MapLibre. Lands only if it clearly wins; otherwise the numbers go in
this ticket for the next chat. Related: T848, T855, T861, T420.
