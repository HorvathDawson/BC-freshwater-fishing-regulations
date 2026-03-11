

# Known Errors & Issues

------------
 
5. when a admin area intersects a lake or other polygon we should only put the regulations added into the intersected portion of the polygon. this means splitting the polygon? could also split streams if we desire.

9. we will want to add in the stocked lake info probably here https://www.gofishbc.com/wp-json/wpgmza/v1/features can query it. then we can link to lake fwa and add info for stoked info. 


11. fix missing knap malcom and rubble creek admin boundaries, etc...

14. tidal issues
-campbell river - in regs
-englishman river
-naniamo river - in regs
-little qualicum river
-East Newcastle Creek
-Salmon river
-adam river
-somass
-gordon river
-San Juan River
-Rainy River
-clowhom lake??
-jordan river
-koprino river

15. 



===============
Complete
===============

----

Tsitika River has multiple eco reserves different name that get grouped together because same reg code
- done each eco reserve is its own thing.

---

bear river is in region 1 and 2 but should only be in 2 because it is fully in 2 not straddling border...
- handled with tests


----

DEAN RIVER PARSING
    "regs_verbatim": "**No Fishing** upstream of Iltasyuko River[Includes Tributaries], including upstream of Anahim Lake[Includes Tributaries], Apr 1-June 14\n**No Fishing** from Iltasyuko River to Crag Creek[Includes Tributaries]\n**No Fishing** from Crag Creek to fishing boundary signs approximately 500 m upstream of canyon[Includes Tributaries], Oct 1-May 31\n**No Fishing** from fishing boundary signs approximately 500 m upstream of canyon to signs 100 m downstream of canyon\n**No Fishing** from fishing boundary signs approximately 100 m downstream of canyon to tidal boundary, Oct 1 - May 31\nTrout/char daily quota = 1 (none under 35 cm)\nBait ban all parts[Includes Tributaries]\nFly fishing only from** (a)** Anahim Lake to Iltasyuko River, June 15- Mar 31, and** (b)** Crag Creek to fishing boundary signs approximately 500 m\nupstream of canyon, July 15-Sept 30\nNo powered boats between signs 0.5 km and 3.5 km upstream of canyon, Aug 1-Aug 31\n**From Crag Creek to signs 500 m upstream of the canyon**[Includes Tributaries]**: Class I water June 1-Sept 30 and Steelhead Stamp mandatory June 1-Sept 30; see map below (Non-residents see notice on **page 42**)\nFrom signs 100 m downstream of canyon to tidal boundary**[Includes Tributaries]**: Class I water June 1-Sept 30 and Steelhead Stamp mandatory June 1-Sept 30: see map below\nFrom Anahim Lake to Iltasyuko River**[Includes Tributaries]**: Class II water June 16-Oct 31 (see map below); Steelhead Stamp not required**\nNOTE: \"canyon\" means lower canyon 3-5 km from river's mouth",

lost classified water note in reg too. this is very bads we need parser to check no rule was missed? also split the one list. not checking if the rule is in the rule verbatim...
{
        "rule_text_verbatim": "Fly fishing only from** (a)** Anahim Lake to Iltasyuko River, June 15- Mar 31",
        "scope": {
          "type": "SEGMENT",
          "lookup_name": "ALL",
          "location_verbatim": "Anahim Lake to Iltasyuko River",
          "landmark_verbatim": "Anahim Lake",
          "landmark_end_verbatim": "Iltasyuko River",
          "direction": "BETWEEN",
          "includes_tributaries": null
        },
        "restriction": {
          "type": "gear_restriction",
          "details": "Fly fishing only",
          "dates": [
            "June 15- Mar 31"
          ]
        }
      },
      {
        "rule_text_verbatim": "Crag Creek to fishing boundary signs approximately 500 m\nupstream of canyon, July 15-Sept 30",
        "scope": {
          "type": "SEGMENT",
          "lookup_name": "ALL",
          "location_verbatim": "Crag Creek to fishing boundary signs approximately 500 m\nupstream of canyon",
          "landmark_verbatim": "Crag Creek",
          "landmark_end_verbatim": "fishing boundary signs approximately 500 m\nupstream of canyon",
          "direction": "BETWEEN",
          "includes_tributaries": null
        },
        "restriction": {
          "type": "gear_restriction",
          "details": "Fly fishing only",
          "dates": [
            "July 15-Sept 30"
          ]
        }
      },

----

piggott creek is not a tributary of oyster creek above a odd point where it should be?   

---

need to add tidal region into the map. (will remove other regulations and say refer to tidal regulations)


---

Unnamed Waterbody in liumchen eco reserve split weird. the split on boundary did not work clean here why? 
- fixed somewhere along the way

---

need to make the naming better for admin zone unnamed streams. we shouldnt add regulation name if it was an admin match. because then everything has the name. or at least dont show in search? 
- this i think naturally got fixed.


---

nicomen slough lakes are part of nicomen slough. sloughs include lakes...
- used a direct match on them all + gnis id


---


Should we make it so that ignored matches can link to another regulation which will then be able to display (and allow search) with all name variations. Similarly name variations should make sure both names are available.
- done and implemented. 

---

Campbell lake is not removing the streams inside of it because the park is there changing some of the regulations... then the streams are gone for the first unregulated section of it?
    - So looking more into it issue one john hart l;ake is tributaries only as regulation so for now the stream is gone while the lake doesn't have regulations because of that.
    - cambell lake (lower) has its tributaries (it is only a tributary only regulation too) leaking up into the upper campbell lake area. this means we get streams up there with tributaries. (MAYBE ALL LAKES SHOULD BLOCK LAKE TRIBUTARIES FROM PROGRESSING...). Also the lake became visible because of the park waters regulations but the streams didnt go away  because it doesnt recognize it.
- not an issue when zone regulations are there since everything is regulated.
- might need future though but most likely unneccasry. 
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
