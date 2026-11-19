# T898 — A daylight-only rule shows today's actual times for the section

Status: IN PROGRESS (app chat; lane app/t898-t899; pictures to the owner; in 0.8.2).

Owner 2026-11-19 ~01:10 (app chat), verbatim: "stuff like this (Daylight only: closed from 1 h after sunset to 1 h
before sunrise Rule book p.23 ↗) could give you the times for it on the section you are currently on (average time for
the section maybe) could jsut use centroid of median location fo the section idk"

Build: under such a rule, the day's times for the open section, e.g. "Today: closed 18:12 to 6:58", from the SAME sun
times the header's sunrise/sunset line uses (T878) for that section and day, offset by the rule's own hours (read from
the rule's stored fields, never parsed from its words in the app). Changes with the chosen day. Where the rule's offsets
are not stored as data, say so in the ticket and show nothing new for it (no guessing). Pictures before it lands.
