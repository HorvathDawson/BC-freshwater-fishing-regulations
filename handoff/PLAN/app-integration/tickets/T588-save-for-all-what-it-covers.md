# T588 — "Save for all wordings": what it should cover

Status: DONE — 72e95c144, 2026-11-10. Owner: option 3 — the behaviour stays (this wording and the older wordings that can take the extent); the button's label gives the row count, the line under it and a confirmation list each wording written and what is NOT written (a wording that cannot take it; any live wording of the same place, which has its own "same extent as…"). Test: frontend `saveForAllWords`. Not looked at in a browser

Source: T532 round 6, leftover b.

## Today
"Save this extent for all wordings" gives the extent to the OLDER wordings of the place's DFO historic group only
(`checklist.saveForAll`, group `dfo historic`). It does not reach: a live twin wording of the same place (that has its
own one-click "same extent as…"), wordings the owner linked by hand in this session before the group reloads, or the
book's rule on the same water.

## Question for the owner
Should one save cover (1) only older wordings (today), (2) every wording in the group box, live ones too, or (3) stay
as is with the button's label saying exactly which rows it will write? No code until he answers; (3) is the smallest.

Depends: owner. Size: S. Owner: yes.
