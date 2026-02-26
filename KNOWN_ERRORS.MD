

2. Should we make it so that ignored matches can link to another regulation which will then be able to display (and allow search) with all name variations. Similarly name variations should make sure both names are available.

------------
4. Campbell lake is not removing the streams inside of it because the park is there changing some of the regulations... then the streams are gone for the first unregulated section of it?
    - So looking more into it issue one john hart l;ake is tributaries only as regulation so for now the stream is gone while the lake doesn't have regulations because of that.
    - cambell lake (lower) has its tributaries (it is only a tributary only regulation too) leaking up into the upper campbell lake area. this means we get streams up there with tributaries. (MAYBE ALL LAKES SHOULD BLOCK LAKE TRIBUTARIES FROM PROGRESSING...). Also the lake became visible because of the park waters regulations but the streams didnt go away  because it doesnt recognize it. 
5. when a admin area intersects a lake or other polygon we should only put the regulations added into the intersected portion of the polygon. this means splitting the polygon? could also split streams if we desire.

7. need to make the naming better for admin zone unnamed streams. we shouldnt add regulation name if it was an admin match. because then everything has the name. or at least dont show in search? 

8. need to add tidal region into the map. (will remove other regulations and say refer to tidal regulations)

9. we will want to add in the stocked lake info probably here https://www.gofishbc.com/wp-json/wpgmza/v1/features can query it. then we can link to lake fwa and add info for stoked info. 




===============
Complete
===============

---

parker lake (Waterbody Key 29311070, Area In Hectares 66.0851953431, GNIS ID 1 22600, GNIS Name 1 Parker Lake)). has a little tiny lake friend??? It grouped on the same bluelinekey "lakes_blue_line_359456560_0".. so looking more it seems like things matched with admin areas will not have the waterbody key as an allowed grouping object. which then means all items in the same blk will get grouped together. sometimes gettiung the name of what ever named items are there. so this means when we match a named item in a admin area we need to make it split on the name too? or at least add the waterbodykey to the matched waterboidy keys. Issue will be if we later split the polygons for the different regulation zones (eco zone on one end of lake) then it will have same waterbody key and name but different regs. how to handle this. should mostly work. may need to get creative with waterbodie poyl id etc... ULTIMATELY WE ONLY CARE ABOUT THIS IF SOMETHING HAS A NAME WE DONT WANT UNNAMED ITEMS GROUPED IN WITH IT. -- updated the merging groups and also made sure the admin matches are done inline before mergeing instead of a weird 4th stage to the pipeline... still might want to allow merging of items in nationalparks/ eco reserves? but i am not sure aboutt that could be excessive.

it all just got split up.

---

Dinosaur lake WBK: 329657308 is not a visible polygon but the river is removed under it? Theory is if it iis regulated but not in regulations because it is Error or similar.

- All streams were being filtered regardless of the match or not

---

JUST NEED TO DECIDE: Do we want the river flowing through a lake that has includes tributaries true to add the regulations to the streams entering the lake too?
 - decided to have them stop at a lake that has regulations.

---

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
