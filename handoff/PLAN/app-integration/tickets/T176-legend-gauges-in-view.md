# T176 — The Conditions map legend's "N gauges in view" counts sections, not gauges

Status: OPEN

Source: T168 review (2026-10-24): the "13 gauges in view" the owner saw on Nicomen Slough is the Conditions map legend
(`app/packages/ui-native/src/Chrome.tsx` `LegendRamp`), which counts the VISIBLE SECTIONS that have a standing — not
gauges. Count distinct stations in view (split as T168 does: on this water / estimating / not reporting) or relabel it to
what it counts ("13 sections coloured"). One function in core for the count; whole-value test; shots. Ships: app. Size: S.
