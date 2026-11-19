# T878 — The day's sunrise/sunset moves from its own header row to a small line under the date

Status: OPEN (app chat; built; lands after the owner sees the pictures).

Owner 2026-11-18 ~20:49, with a shot of Babine Lake — Main Body, verbatim: "There seems to be a ton of space used by the
sunset/rise could we maybe move it to top of something? Where would be good?"

In a dialog he picked "Under the date (Recommended)": a small line under the day picker on the right of the water page
header, beside the name, tied to the picked day. Declined: the Conditions tab; beside the daylight rule only; the
place-words line.

Built (lane `app/t878-daylight-under-date`, from `app/integrate-081-web`, 21:08 MST):
- The "Sunrise 05:07 PT · Sunset 21:52 PT" row under the place words is gone from the water page header.
- Under the date pill: the sun icon (ui-native `sun`) and "05:07–21:52 PT", read "Sunrise 5:07, sunset 21:52 PT"; words
  composed in core (`sunLine` → `{ text, aria }`), right-aligned under the date (`DayControl`). On a desktop the date pill
  is in the side column, so the line sits under it there.
- The empty place-words slot is no longer drawn when a water has no place words (Babine Lake — Main Body, Kootenay Lake —
  Lower West Arm); a water with them (Coquitlam River, "From the Lougheed Highway Bridge up to Coquitlam Watershed
  boundary") keeps that line where it was.
- The daylight-only rule's hours (T473, `daylightRange`) are unchanged.

Pictures (390 phone and 1280 desktop, dark; before / after): `t878-{before,after}-{390,1280}-dark-{babine,coquitlam}.png`
in the lane's scratchpad, sent to the owner by the app chat.
