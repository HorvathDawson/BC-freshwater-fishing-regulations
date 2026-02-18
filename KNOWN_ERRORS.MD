Similkameen River cut off early? - this is because we only match in the zone... need to figure out how to handle this more generally. 
 - this should be fixable by making it so if a stream (same watershed) is represented in a different zone with regulations but not in another zone then it should get the combined regulations from all zones that the stream has regulations in maybe? 



===============
Complete
===============

---



---

maybe it shouldnt go past lakes for stream tributaries too? is it really a tributary of both? or is it only lake/only stream


---

Kettle river is missing segment - This is because of 500m zone buffer it gets some of the segments but not all there...

Do we really care about across the border?? just remove those on export (use no zone?)

---

chilliwack lake doesnt get rid of stream segments
- had a bunch of issues here. mostly waterbody_keys were different in differen locations

---

make sure tributaries enrichment only gets tributaries of provided edges does not travel up mainstem giving more then we want...

---

another issue - not a tributary of chealhis but instead one of pit river... think same issue as the it travels up mainstem instead of just regs provided
===
Morris Creek
Listed as: MORRIS CREEK
REGULATIONS
CHEHALIS RIVER
===

---

why is this entrysplit into 2 segments at columbia lake (becoems tributary of columbia lake)

====
Kootenay River
Listed as:
KOOTENAY RIVER (downstream of Idaho border)
KOOTENAY RIVER (upstream of Koocanusa Reservoir)
===
