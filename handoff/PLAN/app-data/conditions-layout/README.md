# conditions-layout — before / after (2026-10-21)

Owner feedback addressed: the gauge switch sat far from the plot; the plot did not fill the page and desktop fell
apart; "Not reporting today" never said when the gauge last reported; the flow chart could not be zoomed or read.

Shot on the web build (Expo web on a private port, data from the owner's :39217 read-only), phone 390x844 and desktop
1440x900, at device scale 2. `*-full.png` is the whole scrolled page; the plain one is the first screen. Light, plus
dark for the gauged river.

| case | before | after |
|---|---|---|
| Gauged river (Chilliwack River) | `before-gauged-chilliwack-{phone,desktop}[-full].png`, `before-gauged-chilliwack-dark-*` | `after-gauged-chilliwack-*`, `after-gauged-chilliwack-dark-*` |
| Ungauged stream (Borden Creek, borrows Vedder Crossing) | `before-ungauged-borden-creek-*` | `after-ungauged-borden-creek-*` |
| Lake (Cultus Lake, no estimate) | `before-lake-cultus-*` | `after-lake-cultus-*` |
| Saved spot (Chilliwack River) | `before-spot-*` | `after-spot-*` |
| Chart zoom + read-out | none (the chart could not be zoomed) | `after-zoom-readout-{phone,desktop}.png` (zoomed one step, value tapped), `after-zoom-hover-*` (mouse hover over a forecast step), `after-zoom-wheel-desktop.png` (mouse wheel zoomed in) |

## What changed

- **The gauge switch is beside the plot.** "CHART THIS GAUGE" chips (pin colour, station, its share) sit right above
  that gauge's reading and the chart. The donor list rows still pick the chart too.
- **The chart fills its column.** It measures its width (onLayout) and is built for that box, so labels stay
  11 px at every width; height follows the width (190 to 340 px). Axis ticks are real times (months on their
  own first days, hours/days in a zoomed window), three on a phone, five on a column; end labels hang inward.
- **Desktop layout.** The view stops at 1280 px and centres. At 880 px and up the panel is two columns: the answer,
  reading and chart on the left; the gauge map, donor list and notes on the right. The phone's layout is unchanged.
  A saved spot stays one column at a 760 px reading measure (it is a record, not a dashboard).
- **Quiet gauges say when they last reported.** "Last reported 2026-10-17 at 10:00 PDT." from the index's
  `observedAt`, or for a station the index has dropped, from its own file (`now.at`, then the last recent row, then the
  last daily mean's day, which is said as "Last reported on 2026-09-25."). Unknown is said: "When it last reported is
  not known." A gauge that reported today but cannot be ranked says that instead of "Not reporting today."
  08MH055 in these shots has no feed file at all, so it correctly says "not known".
- **The chart zooms and reads out.** Pinch or mouse wheel zooms where it points; drag moves along a zoomed window;
  − / + / Whole chart buttons beside the read-out; double-tap or double-click shows the whole chart. A tap, or a
  mouse hover, reads the nearest sample: "2026-10-20 at 21:00 PDT · 3.85 m³/s · median 11", or "… forecast" for a
  model step. Inside a forecast's span the read-out snaps to the nearest published step.

## Not covered here

- The Conditions map overlay (upstream/downstream, your-location icon, contribution labels, geometry at low zoom)
  belongs to conditions-map; the Regs / Conditions / Map swipe belongs to view-swipe.
- A saved spot has no flow chart (the saved record holds the answer, not a series), so there is no switch or zoom on
  it.
- Touch gestures (pinch, drag, double-tap) are tested with RNGH's test harness, not on a device or emulator.
