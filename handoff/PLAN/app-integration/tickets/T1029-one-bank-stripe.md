# T1029 — One-bank DFO places: the one-side stripe on the Salmon map (T754 option B, the rest)

Status: OPEN — after 0.9.0; needs owner looks (T754 questions 4 and 6) and question 5.

Source: T754 (owner approved option B 2026-11-15 round 4; question 2 answered 2026-11-22 23:45, "Other bank: the water's
other rules"). T754 built the words and the verdict (5953ef057, eeb19b4f3): a placed one-bank place stands beside the water's other
places (`u`, `p[lid].side`), the stretch paints by the other bank's answer and the card shows the one-side caveat. What
option B also had is left here.

## Work
1. Bundle: per section of a placed one-bank stretch, which side of the DRAWN line the bank is (+1 right / -1 left of the
   exported line's direction), computed against the exact exported geometry, never assumed (T754 §3 B, §4: the Kitimat's
   west bank is the right bank looking downstream; FWA lines most likely run upstream).
2. App map: two runtime layers over `stream` (`line-offset` ± half width + 1 px) in the bank place's own colour that day
   (closed for the Kitimat), opacity by feature-state. Pictures for the owner first (light + dark, Kitimat at the signs).
3. Owner questions still open from T754: Q4 (a stripe along the route line inside a polygon reach), Q5 (the Kitimat
   bank place binds 12 unnamed tributary sections from its "(including tributaries)" heading, T807: should a one-bank
   place take tributaries? Today they read beside, the other rules answering), Q6 (the synopsis `kitimat_river.r1`
   `side: west` gets the stripe too?).

Depends: T754. Size: M. Owner: yes (looks, Q4–Q6).
