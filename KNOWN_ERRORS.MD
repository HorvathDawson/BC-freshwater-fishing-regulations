
1. JUST NEED TO DECIDE: Do we want the river flowing through a lake that has includes tributaries true to add the regulations to the streams entering the lake too?
2. Should we make it so that ignored matches can link to another regulation which will then be able to display (and allow search) with all name variations. Similarly name variations should make sure both names are available.
3. Dinosaur lake WBK: 329657308 is not a visible polygon but the river is removed under it? Theory is if it iis regulated but not in regulations because it is Error or similar.


===============
Complete
===============

---

Similkameen River cut off early? - this is because we only match in the zone... need to figure out how to handle this more generally. 
 - this should be fixable by making it so if a stream (same watershed) is represented in a different zone with regulations but not in another zone then it should get the combined regulations from all zones that the stream has regulations in maybe? 
- could try to see if direct match works just to verify...

all regs go to all segments of a match now

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
