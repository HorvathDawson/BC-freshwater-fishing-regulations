"""THE KNOWN-STEELHEAD LIST, BUILT FROM EVIDENCE (first run 2026-10-02 as pipeline/hack/steelhead_waters).

A SEPARATE STEP — NOT part of the atlas build, the reach run or the bundle, and nothing in them calls
it. It writes `data/generated/steelhead/`:

    steelhead_waters.json   the list, in the format of the curated list, with a `generated` block
                            (the generator, the observations' fetch date, the `fingerprint` of its
                            `waters` — `pipeline.atlas.reach.steelhead.fingerprint`)
    steelhead_review.csv    every candidate water with its evidence and decision
    steelhead_dropped.json  every water with steelhead evidence that is NOT listed, and why

A HUMAN then copies `steelhead_waters.json` over `data/curated/regulations/steelhead_waters.json`
(the reach builder reads only that copy, `pipeline.atlas.reach.steelhead.load_list`), and runs the
reach run and the bundle. `test_steelhead_waters.py` fails while the curated copy and the generated
file disagree. The list is a PRESENCE INDICATOR (user ruling 2026-10-02): it makes a water's
`steelhead` attribute "known" and binds no rule; on its flowing sections a rainbow over 50 cm is a
steelhead (`anadromous_rainbow`, user ruling 2026-10-03).

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.steelhead.known_waters

DOWNLOADS ARE CACHED under `data/source/steelhead/` (git-ignored) and reused, so a rerun is
reproducible: the accessibility model GeoPackage (~6.6 GB) and the steelhead observation records
(`st_observations.json`), with `fetched.json` recording each URL, the fetch date and the records'
sha256. Delete a file there to fetch it again. The run also reads the current atlas build and
registry (`GENERATED.require_build()`) and the live reach run's `steelhead_presence` (only for the
review sheet's `book_known` column).

A water is listed when sea-run steelhead are in it somewhere within Canada (user ruling
2026-10-02: no trimming at dams or falls, the whole river is listed):

1. RECORDS. Every record coded `ST` in the province's Known BC Fish Observations layer
   (WHSE_FISH.FISS_FISH_OBSRVTN_PNT_SP, ~17,000).
2. REACHABLE. Each record is placed on the province's Freshwater Fish Habitat Accessibility
   MODEL — Pacific Salmon and Steelhead (bcfishpass). A record on a segment the model marks
   OBSERVED or INFERRED for steelhead is reachable from the sea; one above a natural barrier
   (falls, gradient >= 20 %, subsurface flow) is a resident rainbow, whatever it was coded.
   Natural barriers predate every record; the model already lifts a barrier that fish are
   recorded above.
3. DAMS. Dams are newer than many records, so a record above a BLOCKING dam is dropped: the
   model calls it a barrier and it is >= 3 m (tide gates, flood boxes and lake-outlet weirs
   are not), less the passable ones below, plus Comox Dam.
4. ROUTE. A water with no record of its own counts when fish must pass through it to reach a
   confirmed record (lower rivers, sloughs, side channels).
5. REVIEW. Every candidate was reviewed by hand on 2026-10-02; `REVIEW` below records the
   ones that were not kept (single old records on small creeks, hatchery releases or
   stockings, waters behind dams the model misses), and `HAND` the waters decided by hand. Both
   are REVIEWED data (AGENTS 35): a rerun reuses them, never regenerates them; edit them by hand.
6. NO LAKES (user rulings 2026-10-02): a steelhead cannot be told from a rainbow in a lake.
   Sloughs, canals and channels are streams, so a lake-typed registry item whose name says it
   flows (`steelhead.flows` / `FLOWING`, the ONE definition the reach builder's steelhead water
   shares: Vedder Canal, Gravel Slough, Maria Slough) is listed.
7. THE BOOK'S WATERS TOO (user ruling 2026-10-02): a water the regulation rows already make
   known is still listed when the evidence supports it, so the list stands on its own.
   `book_known` in the review sheet says which.

Records the model's own observation table lacks are snapped to the nearest model segment
(<= 300 m); the rest (interior lakes, northern rivers the model does not cover) were decided
by hand, `HAND` below.
"""
from __future__ import annotations

import bisect
import collections
import csv
import datetime
import hashlib
import json
import sqlite3
import urllib.request
import zipfile

import pandas as pd

from pipeline.atlas.reach.steelhead import CURATED_LIST, fingerprint
from pipeline.common.water_kind import flows
from pipeline.common.curated import GENERATED, SOURCE

MODEL_URL = "https://nrs.objectstore.gov.bc.ca/bchamp/freshwater_fish_habitat_accessibility_MODEL.gpkg.zip"
WFS = ("https://openmaps.gov.bc.ca/geo/pub/wfs?service=WFS&version=2.0.0&request=GetFeature"
       "&typeName=pub:WHSE_FISH.FISS_FISH_OBSRVTN_PNT_SP&outputFormat=application/json"
       "&CQL_FILTER=SPECIES_CODE='ST'&sortBy=FISH_OBSERVATION_POINT_ID&srsName=EPSG:3005"
       "&count=10000&startIndex={start}")

ACCESSIBLE = {"OBSERVED", "INFERRED"}
MIN_DAM_HEIGHT_M = 3
#: fishways, or low weirs fish pass
PASSABLE_DAMS = {"Seton Dam", "Great Central Lake Dam", "Kokish River Dam",
                 "Puntledge River Diversion Dam", "Cameron Lake Dam"}
#: no passage although the model says only POTENTIAL; Comox Lake is above it
BLOCKING_DAMS = {"Comox Dam"}
SAME_RECORD_M = 200      # a DataBC record this close to a model record is that record
SNAP_M = 300             # farther than this from any model segment: decided by hand

#: Waters decided by hand: records the model could not place, and named sources with no record.
HAND: dict[str, tuple[str, str]] = {
    "gnis:32777": ("keep", "Elkin Creek: Chilcotin summer-steelhead spawning stream (COSEWIC 2018/2020)"),
    "gnis:23935": ("keep", "Taseko River: route from the Chilko to Elkin Creek; 19 records to 1998"),
    "gnis:4622": ("keep", "Quaal River: BC Parks Kwaal Conservancy names steelhead; record 1990"),
    "gnis:36313": ("keep", "Khutze River: coastal river, 5 records to 2000"),
    "gnis:39163": ("keep", "Tenderfoot Creek: Cheakamus tributary, Tenderfoot hatchery steelhead (DFO)"),
    "gnis:7250": ("keep", "Little Campbell River: Greater Georgia Basin stock (provincial framework 2016)"),
    "gnis:32069": ("keep", "Okanagan River: Upper Columbia summer steelhead (2006 redd survey)"),
    "gnis:10228": ("keep", "Inkaneep Creek: 10 steelhead redds 2006"),
    "gnis:29653": ("keep", "Vaseux Creek: 10 steelhead redds 2006"),
    "wbk:328961724": ("keep", "Osoyoos Lake: route to the Okanagan River (a lake: dropped by the no-lakes rule)"),
    "gnis:36317": ("unsure", "Kiskosh Creek: one 2002 record; coastal, accessibility not modelled"),
    "gnis:39081": ("unsure", "Moore Cove Creek: one 2002 record; coastal, accessibility not modelled"),
    "gnis:33586": ("unsure", "Gudal Creek: BC Parks says it hosts steelhead; no record"),
    "gnis:22015": ("unsure", "Shakes Creek: lower Stikine tributary, records to 1988; Stikine not modelled"),
    "gnis:24402": ("unsure", "Dokdaon Creek: lower Stikine tributary, records 1978; Stikine not modelled"),
    "gnis:28085": ("unsure", "Mess Creek: Stikine tributary, one 1994 record"),
    "gnis:3089": ("unsure", "Unuk River: steelhead mostly in Alaska; one 1989 BC record"),
    "gnis:21471": ("unsure", "Hodder Creek: upper Bell-Irving tributary, one 1986 record"),
    "gnis:30772": ("unsure", "Rochester Creek: upper Bell-Irving tributary, one 1986 record"),
    "gnis:22221": ("unsure", "Dudidontu River: Taku (Nahlin) tributary, records to 1995"),
}

#: The 2026-10-02 review: candidates NOT kept (lakes are handled by the no-lakes rule).
REVIEW: dict[str, tuple[str, str]] = {
    "gnis:20577": ("drop", "Sandhill Creek: Single 1968 record on a small Saanich Peninsula creek mostly above dams; no steelhead run known."),
    "gnis:13051": ("drop", "Bigtree Creek: Single record above a barrier on a small creek."),
    "wbk:329457630": ("drop", "Skidder: Skidder (pond) has only 2007 fry release records."),
    "gnis:10134": ("drop", "Bugaboo Creek: Single record above a barrier on a small creek."),
    "gnis:10907": ("drop", "Clapp Creek: Wholly above a barrier."),
    "gnis:16117": ("drop", "Hatton Creek: Small creek; records above a barrier."),
    "gnis:24785": ("drop", "Oliver Creek: Only records are 1912-1929 hatchery releases; no evidence of a run."),
    "gnis:22274": ("drop", "Dunsmuir Creek: Above South Fork dam; no passage."),
    "gnis:19852": ("drop", "Fleece Creek: Small creek; records above a barrier."),
    "gnis:25703": ("drop", "Solly Creek: Single record above a barrier."),
    "gnis:37437": ("drop", "Comox Creek: Comox Lake tributary above Comox Dam; at most fry stocking."),
    "gnis:10241": ("drop", "Idiens Creek: Small Comox Lake tributary above Comox Dam."),
    "gnis:24999": ("drop", "Rees Creek: Small Comox Lake tributary above Comox Dam."),
    "gnis:26654": ("drop", "Rogers Creek: Small urban Port Alberni creek; record above barrier/dam."),
    "gnis:13001": ("drop", "Cheam Slough: The only record is a 1938 hatchery eyed-egg release, which is not evidence that wild steelhead use this slough…"),
    "gnis:27961": ("drop", "Slollicum Creek: Single record above a barrier."),
    "gnis:6329": ("drop", "Squawkum Creek: The only record is a 1940 egg release, which is not evidence of use."),
    "gnis:25901": ("drop", "Tipella Creek: Single record above a barrier."),
    "wbk:329292027": ("drop", "Latimer Pond: Small pond; single record above a barrier."),
    "gnis:17532": ("drop", "Clowhom River: Clowhom River is above Clowhom Lake and its dam at Clowhom Falls (head of Salmon Inlet), so anadromous fish ca…"),
    "gnis:11960": ("drop", "Kenyon Creek: Wholly above a barrier."),
    "gnis:29274": ("drop", "Tsuahdi Creek: Records above a barrier."),
    "blk:360219234": ("drop", "Squamish Powerhouse Channel: A powerhouse tailrace channel, which is not a natural migration route."),
    "gnis:24622": ("drop", "Tatlow Creek: Wholly above a barrier."),
    "gnis:7684": ("drop", "Bertram Creek: Wholly above a barrier near Howe Sound; resident rainbow."),
    "gnis:11298": ("drop", "Kallahne Creek: Wholly above a barrier; resident rainbow."),
    "wbk:329532481": ("drop", "Lost Lagoon: Lost Lagoon (Stanley Park) is a closed urban lagoon."),
    "gnis:28792": ("drop", "Ray Creek: Wholly above a barrier."),
    "gnis:34740": ("drop", "Spring Creek: The only record is a 1923 fry release, which is not evidence of use."),
    "gnis:27966": ("drop", "Sloquet Creek: Lillooet River tributary with reachable lower reaches but no confirmed steelhead record; only record is above …"),
    "gnis:18818": ("drop", "Barrie Creek: Small Thompson tributary wholly above a barrier; 'known' today only via the Thompson tributaries row, not real…"),
    "gnis:25664": ("drop", "Squakum Creek: Single record above a barrier."),
    "gnis:7811": ("drop", "Mosher Creek: Small upper Atnarko tributary; only record above a barrier; 'known' today only via the Atnarko/Bella Coola tri…"),
    "wbk:329595989": ("drop", "Cheẑich'ed Biny: Corridor on upper Chilcotin plateau; far inland beyond documented Chilcotin steelhead range."),
    "gnis:6727": ("drop", "Lockhart Gordon Creek: Rivers Inlet creek; only record above a barrier, lower reach unconfirmed."),
    "gnis:27361": ("drop", "Smitley River: Mostly above barriers; single record above barrier."),
    "gnis:10388": ("drop", "Link River: Above barrier/Ocean Falls dam."),
    "gnis:4510": ("drop", "Morse Creek: Wholly above a barrier."),
    "gnis:36808": ("drop", "North Seaskinnish Creek: Nass tributary; only record above a barrier, lower reach unconfirmed."),
    "gnis:4386": ("drop", "Falls River: Above Falls River falls/dam."),
    "gnis:1851": ("unsure", "Craigflower Creek: Urban Victoria creek; historic steelhead records to 1992 but run likely extirpated; coho/cutthroat dominate."),
    "gnis:22204": ("unsure", "Duck Creek: Salt Spring Island creek; records 1984-2002 may reflect enhancement/stocking rather than a self-sustaining run…"),
    "gnis:3051": ("unsure", "Mill Stream: Millstream Creek (Langford) is urban with most of its length above dams/falls; no evidence of a current steelh…"),
    "gnis:21071": ("unsure", "Niagara Creek: Goldstream tributary with only ~100 m below Niagara Falls; single 1977 record."),
    "gnis:3075": ("unsure", "Claud Elliott Creek: Only a 1979 fry release; release does not demonstrate a run."),
    "gnis:37932": ("unsure", "Cooper Creek: Only 1 old (1977) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:27087": ("unsure", "Pye Creek: Only 1 old (1977) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:28166": ("unsure", "Amai Creek: Only 1 old (1980) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:9669": ("unsure", "Brodick Creek: Only 1 old (1979) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:17734": ("unsure", "Calvin Creek: Single undated record (Nootka Sound)."),
    "gnis:17826": ("unsure", "Canton Creek: Only 1 old (1981) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:30567": ("unsure", "Culleet Creek: Only 1 old (1987) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:11329": ("unsure", "Kapoose Creek: Single undated record."),
    "gnis:31150": ("unsure", "Kwois Creek: Only 1 old (1977) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:25575": ("unsure", "McKelvie Creek: Single undated record (Tahsis)."),
    "gnis:23943": ("unsure", "Tatchu Creek: Only 2 old (1973) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:3118": ("unsure", "Kewquodie Creek: Only 1 old (1987) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:6563": ("unsure", "Nuknimish Creek: Only 1 old (1987) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:28163": ("unsure", "Allardyce Creek: Only 1 old (1986) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:8431": ("unsure", "Embley Creek: Only 2 old (1988) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:31870": ("unsure", "Rainbow Creek: Single undated record on a small mainland creek."),
    "gnis:28807": ("unsure", "Read Creek: Only 1 old (1977) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:20514": ("unsure", "Sallie Creek: Single undated record."),
    "gnis:22469": ("unsure", "Wahkash Creek: Only 1 old (1986) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:2522": ("unsure", "Ayum Creek: Only 2 old (1977) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:11457": ("unsure", "Kemp Stream: Only a single 2007 fry release; release does not demonstrate a run."),
    "gnis:21306": ("unsure", "Desolation Creek: Only 1 old (1988) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:24471": ("unsure", "Doobah Creek: Single undated record; another record above a barrier."),
    "gnis:9765": ("unsure", "East Klanawa River: Only 2 old (1987) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:19880": ("unsure", "Floodwood Creek: Single undated record."),
    "gnis:37366": ("unsure", "Loup Creek: Two undated records; one above barrier."),
    "gnis:16300": ("unsure", "Sugsaw Creek: Only 2 old (1987) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:29711": ("unsure", "Uglow Creek: Only 2 old (1983) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:16704": ("unsure", "Ashburnham Creek: Only 1 old (1986) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:11113": ("unsure", "Beaver Creek: Records are 1912-1985 hatchery releases into a Cowichan Lake tributary; not evidence of a run."),
    "gnis:37068": ("unsure", "Boucicault Creek: Only 1 old (1985) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:36172": ("unsure", "Croft Creek: Only 1 old (1985) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:9780": ("unsure", "East Robertson River: Only 2 old (1986) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:39318": ("unsure", "Fellows Creek: Only 2 old (1985) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:10460": ("unsure", "Glenora Creek: Only 2 old (1985) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:17395": ("unsure", "Jasper Creek: Only 1 old (1989) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:25555": ("unsure", "McKay Creek: Only 1 old (1985) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:27270": ("unsure", "Meade Creek: Only 1 old (1986) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:20389": ("unsure", "Neel Creek: Only 1 old (1985) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:1815": ("unsure", "Nixon Creek: Only 1 old (1971) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:25710": ("unsure", "Somenos Creek: Slow, warm, eutrophic slough-lake system off the Cowichan; one 1999 record; poor steelhead habitat."),
    "gnis:20883": ("unsure", "Sutton Creek: Only 2 old (1985) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:32261": ("unsure", "Tzouhalem Creek: Small Cowichan-estuary creek, two 1985 records."),
    "gnis:25176": ("unsure", "Wardroper Creek: Only 2 old (1985) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:27697": ("unsure", "Widow Creek: Only 1 old (1985) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:27712": ("unsure", "Wild Deer Creek: Only 1 old (1985) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:17648": ("unsure", "Averill Creek: Only 1 old (1985) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:28052": ("unsure", "Menzies Creek: Only 1 old (1982) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:12322": ("unsure", "Benson Creek: Single 1969 record; another above a barrier."),
    "gnis:13570": ("unsure", "Blackjack Creek: Single undated record on a small Nanaimo tributary."),
    "gnis:5557": ("unsure", "Green Creek: 1979 records with twice as many above a barrier."),
    "gnis:2920": ("unsure", "Whisky Creek: Single 1993 record; one record above Whisky Creek Dam and a tiny accessible reach."),
    "gnis:8985": ("unsure", "Adrian Creek: Two undated records on a small Comox-area creek."),
    "gnis:13681": ("unsure", "Bloedel Creek: Only 2 old (1985) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:8643": ("unsure", "Chef Creek: Single undated record on a small Bowser-area creek."),
    "gnis:36224": ("unsure", "Cruickshank River: Historic summer steelhead and fry stocking above Comox Dam, but dam fishway ineffective; settle with evidence …"),
    "gnis:22310": ("unsure", "Hunts Creek: Small Qualicum Beach-area creek with a release and one survey record."),
    "gnis:12695": ("unsure", "Kitty Coleman Creek: Only 1 old (1985) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:26332": ("unsure", "Simms Creek: Single undated record on a small urban Campbell River creek."),
    "gnis:21675": ("unsure", "Thames Creek: Single undated record on a small Bowser-area creek."),
    "gnis:19569": ("unsure", "Bear Creek: Single 1955 record, Alberni-valley creek."),
    "gnis:11116": ("unsure", "Beaver Creek: Only 1 old (1989) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:29704": ("unsure", "Ca'aqu'a Creek: Only 2 old (1987) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:20599": ("unsure", "Deer Creek: Single 1955 record."),
    "gnis:22159": ("unsure", "Drinkwater Creek: Single 1955 record at head of Great Central Lake; steelhead not known to spawn in GCL tributaries."),
    "gnis:16338": ("unsure", "Lanterman Creek: Two records (1955, 1989) on a small Ash/Stamp-area creek."),
    "gnis:15161": ("unsure", "McBride Creek: Single 1955 record in Great Central Lake drainage."),
    "gnis:22903": ("unsure", "McFarland Creek: Only 2 old (1989) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:26540": ("unsure", "Spaht Creek: Single 1955 record."),
    "gnis:25171": ("unsure", "Ward Creek: Single 1955 record."),
    "gnis:37297": ("unsure", "Wolf Creek: Single 1955 record."),
    "gnis:2496": ("unsure", "Jacklah River: Only 1 old (1979) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:7392": ("unsure", "Little Toquaht Creek: Only 1 old (1989) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:28856": ("unsure", "Redford Creek: Only 1 old (1989) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:20576": ("unsure", "Sandhill Creek: Single undated record (Clayoquot)."),
    "gnis:22790": ("unsure", "Shelter Creek: Only 1 old (1979) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:23096": ("unsure", "Sydney River: Only 2 old (1979) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:25964": ("unsure", "Watta Creek: Only 1 old (1979) record(s) on a small creek; may be resident rainbow or cutthroat misID."),
    "gnis:18271": ("unsure", "Bancroft Creek: Only a 1988 fall-fry release and most of the creek above barriers."),
    "gnis:10720": ("unsure", "Elbow Creek: Single undated record."),
    "gnis:28448": ("unsure", "Trio Creek: Single undated record."),
    "gnis:26226": ("unsure", "Poole Creek: One 1987 record on a small Lillooet Lake-area creek."),
    "gnis:23851": ("unsure", "Taillefer Creek: One 1994 record on a small creek near Lillooet Lake."),
    "gnis:22753": ("unsure", "Powell River: Powell River is a short tailwater below Powell Lake dam; the 5 records may be catches in the tidal outlet."),
    "gnis:27938": ("unsure", "Sliammon Creek: Two records on a small Powell River-area creek."),
    "gnis:27674": ("unsure", "Whittall Creek: Two records on a small creek."),
    "gnis:21666": ("unsure", "x̱enichen stulu: Two 1999 records on a small Sunshine Coast creek."),
    "gnis:21414": ("unsure", "Hillis Creek: One 2001 record on a small Toba-area creek."),
    "gnis:16376": ("unsure", "Larson Creek: Two 1980 records on a small Toba-area creek."),
    "gnis:2767": ("unsure", "Tahumming River: Two records on a Toba-area river."),
    "gnis:5726": ("unsure", "Cumsack Creek: Two 1977 records on a small Bute Inlet creek."),
    "gnis:3236": ("unsure", "Jewakwa River: One 1996 record on a remote Homathko-area tributary."),
    "gnis:6270": ("unsure", "Teaquahan River: One 1977 record on a Bute Inlet river."),
    "gnis:15727": ("unsure", "Mouat Creek: One undated record on a small coastal creek."),
    "gnis:18438": ("unsure", "Carry Creek: One undated record on a Coquihalla-area creek far upstream; steelhead passage that high is unclear. A Coquihal…"),
    "gnis:2101": ("unsure", "Sucker Creek: Two 2002 records on a small creek near Hope."),
    "gnis:6062": ("unsure", "Nicolum River: One 1995 record on a Coquihalla tributary near Hope."),
    "gnis:13047": ("unsure", "Big Silver Creek: A Harrison Lake tributary with 3 survey records; steelhead passage through Harrison Lake into this creek is un…"),
    "gnis:10437": ("unsure", "Clear Creek: One 1995 record on a small Harrison-area creek."),
    "gnis:7946": ("unsure", "Cogburn Creek: Two 1995 records on a Harrison Lake tributary; steelhead use is unconfirmed."),
    "gnis:24516": ("unsure", "Hornet Creek: A Harrison Lake tributary with 6 survey records; steelhead use above Harrison Lake is unconfirmed."),
    "gnis:13499": ("unsure", "Maria Slough: Two 2001 records on a Fraser side slough near Agassiz."),
    "gnis:23875": ("unsure", "Talc Creek: One 1995 record on a small Harrison creek."),
    "gnis:18446": ("unsure", "Cartmell Creek: One 1979 record on a small Chehalis/Harrison-area creek."),
    "gnis:12130": ("unsure", "Maisal Creek: Two records on a small Chehalis-area creek."),
    "gnis:26928": ("unsure", "Pretty Creek: One 1983 record on a small Harrison creek."),
    "gnis:2406": ("unsure", "Statlu Creek: Three records incl. a smolt on a Chehalis tributary."),
    "gnis:17821": ("unsure", "Cantelon Creek: A few release records on a small creek near Hope; release records do not show wild use."),
    "gnis:30426": ("unsure", "Yola Creek: Three release records on a small creek."),
    "gnis:10014": ("unsure", "Anderson Creek: One 1977 survey record on a small Fraser-valley creek; no other evidence of steelhead use. A recent fish surve…"),
    "wbk:329083386": ("unsure", "Hope Slough: Hope Slough is floodboxed off the Fraser; single 1977 record."),
    "gnis:17985": ("unsure", "Lorenzetta Creek: Two records on a small creek near Laidlaw."),
    "gnis:21920": ("unsure", "Paleface Creek: One 1995 record on a creek at Chilliwack Lake; steelhead use above the lake is doubtful."),
    "gnis:2710": ("unsure", "Post Creek: Four records on a creek near Chilliwack Lake."),
    "gnis:24496": ("unsure", "Hope Slough: Corridor through floodgated Hope Slough; the upstream records are weak."),
    "gnis:28341": ("unsure", "Ryder Creek: Two records on a small Chilliwack creek."),
    "gnis:12391": ("unsure", "Best Creek: One 1981 survey record on a small Langley creek. A recent juvenile survey would settle it."),
    "gnis:8950": ("unsure", "Bori Creek: One 1993 record on a small Abbotsford creek."),
    "gnis:11702": ("unsure", "Cave Creek: One 1998 record on a small creek near the border."),
    "gnis:12926": ("unsure", "Chantrell Creek: One undated record on a small Surrey creek."),
    "gnis:7267": ("unsure", "Chilliwack Creek: One 1985 record on a small lowland Chilliwack creek behind floodgates."),
    "gnis:10942": ("unsure", "Clayburn Creek: One 1995 record on a small Abbotsford creek."),
    "gnis:18040": ("unsure", "Coghlan Creek: One 1998 record on a small Langley creek."),
    "gnis:2275": ("unsure", "Elgin Creek: Two 1976 records on a small Surrey creek."),
    "gnis:19019": ("unsure", "Fergus Creek: Three records on a Little Campbell tributary; use is plausible but unconfirmed."),
    "gnis:8021": ("unsure", "Fishtrap Creek: Three records on Fishtrap Creek (Nooksack system); this urbanised creek may have lost its run."),
    "gnis:13973": ("unsure", "Gifford Slough: Two records on a Matsqui slough."),
    "gnis:8009": ("unsure", "Gravel Slough: Records 1994-1997 on a lowland Chilliwack-area slough."),
    "gnis:21590": ("unsure", "Howes Creek: One 1985 record on a small Abbotsford creek."),
    "gnis:16400": ("unsure", "Latimer Creek: Three records on a small Surrey creek."),
    "gnis:26380": ("unsure", "McLennan Creek: One 1994 record on a small Abbotsford creek."),
    "gnis:37230": ("unsure", "Muckle Creek: Two 1976 records on a small Langley creek."),
    "gnis:15836": ("unsure", "Munday Creek: One 1985 record on a small creek."),
    "gnis:15876": ("unsure", "Murray Creek: Six records incl. fry on a small Langley creek; these could be cutthroat or resident rainbow."),
    "gnis:37145": ("unsure", "Newlands Brook: One 1995 record on a small Langley creek."),
    "gnis:43645": ("unsure", "Pepin Creek: Five records on a small Nooksack-system creek."),
    "gnis:27869": ("unsure", "Quibble Creek: Three records on a small Surrey creek."),
    "gnis:20087": ("unsure", "Saar Creek: One 1994 record on a small creek."),
    "gnis:20556": ("unsure", "Sam Hill Creek: One 1985 record below the Sam Hill Creek dam on a tiny creek."),
    "gnis:16215": ("unsure", "Street Creek: Three records on a small Chilliwack-area creek."),
    "wbk:328997872": ("unsure", "Sumas River: Sumas River behind Barrowtown pump station; steelhead passage uncertain."),
    "gnis:22465": ("unsure", "Wades Creek: Two 1985 records on a small creek."),
    "gnis:6476": ("unsure", "Willband Creek: One 1985 record on a small creek."),
    "gnis:8341": ("unsure", "Williamson Slough: Corridor on a Fraser South Arm slough; it is unclear that it is on the route."),
    "gnis:37373": ("unsure", "Lower Hatzic Slough: Corridor through floodgated Hatzic Slough; the upstream evidence is weak."),
    "gnis:8742": ("unsure", "Palmateer Creek: One 2002 record on a small Langley creek."),
    "gnis:26286": ("unsure", "Silverdale Creek: Six records 1983-1989 on a small Mission creek."),
    "gnis:26057": ("unsure", "West Creek: Five records 1972-1992 on a small Langley creek."),
    "gnis:30435": ("unsure", "Yorkson Creek: One 1985 record on a small Langley creek."),
    "gnis:7591": ("unsure", "Anderson Creek: Small Sunshine Coast creek with only 3 survey records from 1977; most likely coastal cutthroat or resident rai…"),
    "gnis:10482": ("unsure", "Angus Creek: Two undated records on a small Sechelt creek, and most of it is above a barrier; steelhead use is unproven. A …"),
    "gnis:18395": ("unsure", "Carlson Creek: One 1997 record on a small Sechelt-area creek that is mostly above a barrier."),
    "gnis:12981": ("unsure", "Chaster Creek: One undated record on a small Gibsons creek."),
    "gnis:13725": ("unsure", "Chickwat Creek: A few fry/juvenile records on a small Sechelt Inlet-area creek; these could be resident rainbow or cutthroat."),
    "gnis:37419": ("unsure", "Colvin Creek: One 1995 record on a small Sunshine Coast creek."),
    "gnis:13960": ("unsure", "Gibson Creek: One undated record on a small Gibsons creek."),
    "gnis:7744": ("unsure", "Kleindale Creek: One 1995 record on a small Pender Harbour creek."),
    "gnis:34561": ("unsure", "Malcolm Creek: One undated record on a small Sunshine Coast creek."),
    "gnis:29028": ("unsure", "Mill Creek: One 1979 record on a small Howe Sound creek that is mostly above a barrier."),
    "gnis:7407": ("unsure", "Myers Creek: Corridor only, on a small Pender Harbour creek; the upstream record is unclear."),
    "gnis:21819": ("unsure", "Ouillet Creek: One undated record on a small Sunshine Coast creek."),
    "gnis:27550": ("unsure", "Ruby Creek: Corridor on Ruby Creek (Egmont area) through a lake system; it is unclear that it is the route of a real popul…"),
    "gnis:14527": ("unsure", "Stakawus Creek: One undated record on a small creek."),
    "gnis:27627": ("unsure", "Treat Creek: One 1995 record on a small creek."),
    "gnis:17521": ("unsure", "Cloudburst Creek: One 1997 record on a small Squamish-area creek."),
    "gnis:18491": ("unsure", "Evans Creek: One 1995 record on a small Squamish-area creek that is mostly above a barrier."),
    "gnis:12544": ("unsure", "Fries Creek: One 1978 record on a small Squamish-area creek."),
    "gnis:21395": ("unsure", "High Falls Creek: High Falls Creek has a large falls near its mouth, so only a very short reach is accessible."),
    "gnis:14806": ("unsure", "Monmouth Creek: Two 1978-1979 records on a small Howe Sound creek."),
    "gnis:23176": ("unsure", "Pillchuck Creek: One 1987 record on a small Squamish creek."),
    "gnis:2898": ("unsure", "Sigurd Creek: Two records on a small Squamish creek that is mostly above a barrier."),
    "gnis:23914": ("unsure", "Tantalus Creek: One 1978 record on a small Squamish creek."),
    "gnis:5659": ("unsure", "Cheekye River: Two records on the steep, debris-flow-prone Cheekye; use is likely limited to the lowest reach near the Cheaka…"),
    "gnis:7784": ("unsure", "Culliton Creek: One 1995 record on a small Cheakamus-area creek."),
    "gnis:24498": ("unsure", "Hop Ranch Creek: Two records on a small Squamish creek."),
    "gnis:22180": ("unsure", "Dryden Creek: Two records on a small Squamish creek."),
    "gnis:25801": ("unsure", "Ring Creek: Two 1978-1979 records on a small Mamquam tributary."),
    "gnis:14243": ("unsure", "Mashiter Creek: Three records on a small Squamish creek."),
    "gnis:13643": ("unsure", "Blaney Creek: One 1994 record on a small Alouette tributary that is known mainly for coho and cutthroat."),
    "gnis:38779": ("unsure", "Boise Creek: One 1995 record on an upper Pitt tributary. An upper Pitt juvenile survey would settle it."),
    "gnis:37521": ("unsure", "Como Creek: Corridor only: an urban creek with no record of its own, and it is unclear that it leads to a real upstream po…"),
    "gnis:19995": ("unsure", "Corbold Creek: One 1995 record on an upper Pitt tributary."),
    "gnis:14354": ("unsure", "Cypress Creek: Old (1969-1992) records on a West Vancouver creek where any run is likely lost or very small. Streamkeeper dat…"),
    "gnis:12595": ("unsure", "Furry Creek: Small Howe Sound creek with a falls near its mouth; only 4 old records."),
    "gnis:16120": ("unsure", "Hatzis Slough: Corridor through Hatzic Slough (floodgated/pumped); the upstream records are single old ones on Lagace/Pattiso…"),
    "gnis:21605": ("unsure", "Hoy Creek: Two records on a small Coquitlam River tributary known mainly for coho."),
    "gnis:22345": ("unsure", "Hyde Creek: One 2001 record on a small Port Coquitlam creek."),
    "gnis:11421": ("unsure", "Keith Creek: Two records on a small North Vancouver creek."),
    "gnis:15024": ("unsure", "Lagace Creek: One 1994 record on a Hatzic-system creek behind floodgates."),
    "gnis:32057": ("unsure", "Mackay Creek: Two records on a small North Vancouver creek."),
    "gnis:14971": ("unsure", "Mosquito Creek: Four records 1996-1999 on an urban North Vancouver creek."),
    "gnis:20408": ("unsure", "Nelson Creek: One 1979 record on a small West Vancouver creek."),
    "gnis:23366": ("unsure", "Noons Creek: Two records on a small Port Moody creek."),
    "gnis:24850": ("unsure", "Or Creek: Records 1987-1996 on a small Coquitlam tributary."),
    "gnis:2704": ("unsure", "Paton Creek: One 1982 record on a small Indian Arm creek."),
    "gnis:22694": ("unsure", "Pattison Creek: One 1994 record on a Hatzic-system creek."),
    "gnis:23203": ("unsure", "Pinecone Creek: One 1996 record on a small upper Pitt creek."),
    "gnis:24243": ("unsure", "Scott Creek: Four records on a small Coquitlam tributary."),
    "gnis:27986": ("unsure", "Smiling Creek: One 2001 record on a small Port Coquitlam creek."),
    "gnis:16210": ("unsure", "Strawberry Slough: Corridor through a slough; the upstream evidence is weak."),
    "gnis:26993": ("unsure", "Prospect Creek: Single 1976 record on a tiny creek; no recent evidence; a juvenile survey would settle it."),
    "gnis:24273": ("unsure", "Scudamore Creek: Single 1994 record high in the Cayoosh drainage, likely above Walden North diversion dam and canyon falls; con…"),
    "gnis:21812": ("unsure", "Texas Creek: Single 1997 record on a small steep Fraser tributary near Lillooet; a juvenile survey would settle it."),
    "gnis:24249": ("unsure", "Scottie Creek: Two records 1980-85 only on a small Bonaparte tributary; no recent evidence; a current juvenile survey would s…"),
    "gnis:16771": ("unsure", "Cache Creek: Two 1980 records only in a warm, degraded Bonaparte tributary; current steelhead use doubtful; a recent juveni…"),
    "gnis:39497": ("unsure", "Tranquille River: Kamloops Lake tributary above the main Thompson steelhead range; records are hatchery releases/old surveys; ad…"),
    "gnis:11127": ("unsure", "Beaverdam Creek: Single 1995 record on a small Yalakom/Bridge-area creek; could be resident rainbow; a juvenile steelhead surve…"),
    "gnis:14728": ("unsure", "Peridotite Creek: Single 1995 record on a small Yalakom-area creek; may be resident rainbow; a juvenile survey would settle it."),
    "gnis:6297": ("unsure", "Retaskit Creek: Single 1995 record on a small Yalakom-area creek; may be resident rainbow; a juvenile survey would settle it."),
    "gnis:23849": ("unsure", "Tahyesco River: Single 1986 record in the upper Dean area."),
    "gnis:10962": ("unsure", "Goat Creek: Single 1978 record on a small Atnarko-area creek; may be resident rainbow."),
    "gnis:11247": ("unsure", "Janet Creek: Single 1978 record; most of creek above barrier."),
    "gnis:16287": ("unsure", "Sugar Camp Creek: Single 1978 record on a small Atnarko-area creek."),
    "gnis:1914": ("unsure", "Young Creek: Single 1986 record; most of creek above barrier."),
    "gnis:37911": ("unsure", "Cariboo River: Records sit only at the confluence reach; steelhead in the Quesnel/Cariboo system are rare or absent; confirm …"),
    "gnis:27867": ("unsure", "Quesnel River: Steelhead in the Quesnel system are rare or uncertain; confirm against Interior Fraser steelhead distribution."),
    "gnis:33412": ("unsure", "Lingfield Creek: Single 1981 record on a small Chilko-area creek; Chilcotin steelhead spawning there unconfirmed."),
    "wbk:329079684": ("unsure", "Tŝilhqox Biny: Chilko Lake: Chilko steelhead spawn in Chilko River below the lake; lake use unconfirmed."),
    "gnis:30996": ("unsure", "Allard Creek: Single 1980 record on a small Rivers Inlet-area creek; no corroboration."),
    "gnis:31783": ("unsure", "Takush River: Single 1989 record; most of river above barrier."),
    "gnis:31017": ("unsure", "Asseek River: Two 1986 records only; South Bentinck stream with limited accessible reach."),
    "gnis:31711": ("unsure", "Cold Creek: Single 1981 record on a small coastal creek; no corroboration."),
    "gnis:32327": ("unsure", "Namu River: Corridor only, no direct records; the Namu outlet has a historic cannery dam; confirm the upstream record and …"),
    "gnis:23934": ("unsure", "Tarrant Creek: Two 1986 records; most of creek above barrier."),
    "gnis:6543": ("unsure", "Tseapseahoolz Creek: Single 1978 record on a small creek."),
    "gnis:5549": ("unsure", "Clatse Creek: Single 1990 record; most of the creek is above barrier."),
    "gnis:27815": ("unsure", "Quartcha Creek: Single 1989 record; no corroboration."),
    "gnis:6413": ("unsure", "Roscoe Creek: Single 1989 record; no corroboration."),
    "gnis:20499": ("unsure", "Sakumtha River: Single 1986 record in a Kimsquit-area stream."),
    "gnis:3200": ("unsure", "Scotia River: Only one undated record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead …"),
    "gnis:3969": ("unsure", "Cecil Creek: Only one 1976 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:3975": ("unsure", "Coldwater Creek: Only one 1987 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:3840": ("unsure", "Lone Wolf Creek: Only record is a 1986 hatchery smolt release, not evidence of a wild run. A wild juvenile/adult steelhead reco…"),
    "gnis:32345": ("unsure", "Nalbeelah Creek: Only record is a 1986 hatchery smolt release, not evidence of a wild run. A wild juvenile/adult steelhead reco…"),
    "gnis:33276": ("unsure", "Braverman Creek: Only one 1983 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:38515": ("unsure", "Fairfax Creek: Only one 1982 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:8526": ("unsure", "Sachs Creek: Only one 1986 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:8749": ("unsure", "Salmon River: Only one 1986 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:32709": ("unsure", "Blackwater Creek: Only one 1986 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:33689": ("unsure", "Canoe Creek: Only one 1986 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:6805": ("unsure", "Dinan Creek: Only one 1986 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:32468": ("unsure", "Jalun River: Only one 1986 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:8622": ("unsure", "Lagins Creek: Only one 1986 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:3406": ("unsure", "Phantom Creek: Only one 1986 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:35464": ("unsure", "Three Mile Creek: Only one undated record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead …"),
    "gnis:4714": ("unsure", "Denise Creek: Single 1999 record on a tiny creek mostly above a barrier or unmodelled. A repeat record would settle it."),
    "gnis:3903": ("unsure", "McNichol Creek: Single 2002 record on a small Prince Rupert-area creek better known for coho; no steelhead population document…"),
    "gnis:4800": ("unsure", "Shawatlan River: Short Prince Rupert stream with a dam on part of it and only two 1980s records. Confirming whether the dam blo…"),
    "gnis:2060": ("unsure", "Stumaun Creek: Single undated record on a tiny creek that is mostly above a barrier and partly above a dam. A dated record be…"),
    "gnis:3827": ("unsure", "Toon River: Only one undated record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead …"),
    "gnis:3987": ("unsure", "Erlandsen Creek: Only one 1976 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:36392": ("unsure", "Little Paw Creek: Single 1994 record; most of the creek is above a barrier. A repeat record below the barrier would settle it."),
    "gnis:3213": ("unsure", "Paw Creek: Single 1994 record; most of the creek is above a barrier. A repeat record below the barrier would settle it."),
    "gnis:11093": ("unsure", "Beatty Creek: Single 1986 record on a small Stikine-area creek near the edge of steelhead range (steelhead only below the Gr…"),
    "gnis:17470": ("unsure", "Little Tahltan River: Single 1958 record in the far upper Tahltan system; no modern confirmation of steelhead above the lower Tahlta…"),
    "gnis:38489": ("unsure", "Bearskin Creek: Single 1994 record; model says nearly all of the creek (16 of 17 sections) is above a barrier in the sparse Ta…"),
    "gnis:25344": ("unsure", "Samotua River: Single 1988 record on a Taku-system tributary; steelhead in this part of the Taku are sparse and unconfirmed. …"),
    "gnis:30410": ("unsure", "Yeth Creek: Single 1994 record on a small upper Taku/Nakina tributary at the edge of a sparse steelhead range. A repeat re…"),
    "gnis:4100": ("unsure", "McKay Creek: Records come from Kitimat hatchery smolt releases (1986-1996), not wild steelhead observations. A wild steelhe…"),
    "gnis:36497": ("unsure", "Aluk Creek: Only one 1987 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:36636": ("unsure", "Brown Paint Creek: Only one 1981 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:36858": ("unsure", "Clifford Creek: Only one 1977 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:37312": ("unsure", "Hevenor Creek: Only one 1987 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:20120": ("unsure", "Saicote Creek: Only one undated record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead …"),
    "gnis:36935": ("unsure", "Skunsnat Creek: Only one 1986 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:20879": ("unsure", "Sutherland River: Records sit in the Babine Lake headwaters where the large Sutherland-strain Babine Lake rainbow trout spawn; s…"),
    "gnis:18773": ("unsure", "Gramophone Creek: Only one undated record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead …"),
    "gnis:8299": ("unsure", "Skilokis Creek: Only one undated record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead …"),
    "gnis:26770": ("unsure", "Touhy Creek: Only one undated record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead …"),
    "gnis:13142": ("unsure", "Blackberry Creek: Only one 1961 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:11906": ("unsure", "Foxy Creek: Single 1993 record in the upper Bulkley headwaters on a creek partly above a barrier. A repeat record would se…"),
    "gnis:2221": ("unsure", "Goathorn Creek: Only one undated record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead …"),
    "gnis:21602": ("unsure", "Howson Creek: Only one undated record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead …"),
    "gnis:12690": ("unsure", "Kitsuns Creek: Only one 1986 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:2431": ("unsure", "Maxan Creek: Single 1982 record in the far upper Bulkley headwaters where steelhead use is marginal. A recent upper-Bulkley…"),
    "gnis:21855": ("unsure", "Owen Creek: Only one 1981 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
    "gnis:6786": ("unsure", "Salmon Run Creek: Only one undated record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead …"),
    "gnis:26301": ("unsure", "Silvern Creek: Only one undated record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead …"),
    "gnis:14575": ("unsure", "Starr Creek: Only one undated record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead …"),
    "gnis:36259": ("unsure", "Swede Creek: Only one 1987 record on a small stream; plausibly steelhead parr in an accessible tributary of a steelhead sys…"),
}


# ----------------------------------------------------------------------------- fetch
def fetch() -> tuple[str, str]:
    """The model GeoPackage and the steelhead records, downloaded ONCE into data/source/steelhead/
    and reused on every rerun (delete a file to fetch it again). `fetched.json` records what was
    fetched, from where and when."""
    d = SOURCE / "steelhead"
    d.mkdir(parents=True, exist_ok=True)
    manifest = d / "fetched.json"
    got = json.loads(manifest.read_text()) if manifest.exists() else {}
    gpkg = d / "freshwater_fish_habitat_accessibility_MODEL.gpkg"
    if not gpkg.exists():
        z = d / "model.gpkg.zip"
        print(f"downloading the accessibility model (~3 GB) -> {z}")
        urllib.request.urlretrieve(MODEL_URL, z)
        with zipfile.ZipFile(z) as zf:
            zf.extract(gpkg.name, d)
        z.unlink()
        got[gpkg.name] = {"url": MODEL_URL, "fetched": datetime.date.today().isoformat()}
    obs = d / "st_observations.json"
    if not obs.exists():
        feats, start = [], 0
        while True:
            with urllib.request.urlopen(WFS.format(start=start), timeout=600) as r:
                page = json.load(r)["features"]
            feats += page
            if len(page) < 10000:
                break
            start += 10000
        obs.write_text(json.dumps(feats))
        print(f"{len(feats):,} steelhead records -> {obs}")
        got[obs.name] = {"url": WFS.format(start=0), "fetched": datetime.date.today().isoformat(),
                         "records": len(feats)}
    if obs.name in got and "sha256" not in got[obs.name]:
        got[obs.name]["sha256"] = hashlib.sha256(obs.read_bytes()).hexdigest()
    manifest.write_text(json.dumps(got, indent=1, sort_keys=True) + "\n")
    return str(gpkg), str(obs)


def fetched_on() -> str:
    """The date the observations were fetched (`fetched.json`), else the cache file's mtime — so
    the list's `generated.date` names the evidence, not the day the script ran."""
    d = SOURCE / "steelhead"
    m = d / "fetched.json"
    if m.exists():
        got = json.loads(m.read_text()).get("st_observations.json", {}).get("fetched")
        if got:
            return got
    return datetime.date.fromtimestamp((d / "st_observations.json").stat().st_mtime).isoformat()


# ----------------------------------------------------------------------------- records
def load_records(gpkg: str, obs_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Every steelhead record on a model segment: its access class and the dams below it — and
    the records no model segment holds (left to `HAND`; reported, never listed)."""
    import geopandas as gpd
    from shapely.geometry import Point

    con = sqlite3.connect(gpkg)
    obs = pd.read_sql("""select observation_key key, observation_date date, source, life_stage,
        linear_feature_id lfid, blue_line_key blk, downstream_route_measure m, wscode, localcode
        from observations where species_code='ST'""", con)
    lfids = tuple(int(x) for x in obs.lfid.dropna().unique())
    seg = pd.read_sql(f"""select linear_feature_id lfid, blue_line_key blk, downstream_route_measure dm,
        upstream_route_measure um, model_access_steelhead acc, barriers_dams_dnstr dams
        from model_access where linear_feature_id in {lfids}""", con)
    m = obs.merge(seg, on=["lfid", "blk"], how="left")
    m = m[((m.m >= m.dm - 1) & (m.m <= m.um + 1)) | m.dm.isna()]
    m = m.sort_values("dm").drop_duplicates("key", keep="last").drop(columns=["dm", "um"])

    # DataBC records the model's table lacks -> the nearest model segment
    feats = json.load(open(obs_path))
    pts = gpd.GeoDataFrame([{**f["properties"], "geometry": Point(f["geometry"]["coordinates"])}
                            for f in feats], crs=3005)
    mod = gpd.read_file(gpkg, layer="observations", where="species_code='ST'")[["geometry"]]
    j = gpd.sjoin_nearest(pts, mod.to_crs(3005), max_distance=SAME_RECORD_M, how="left")
    miss = pts[j[~j.index.duplicated()].index_right.isna().values].copy()
    groups = tuple(miss.WATERBODY_IDENTIFIER.dropna().str[-4:].unique())
    segs = gpd.read_file(gpkg, layer="model_access", where=f"watershed_group_code in {groups}",
                         columns=["linear_feature_id", "blue_line_key", "downstream_route_measure",
                                  "wscode", "localcode", "model_access_steelhead", "barriers_dams_dnstr"])
    k = gpd.sjoin_nearest(miss, segs, max_distance=SNAP_M, how="inner")
    k = k[~k.index.duplicated()]
    k = k[k.model_access_steelhead.notna()]      # beyond the model's anadromous range: by hand
    k["m"] = [s.project(p) for s, p in zip(segs.geometry.loc[k.index_right].values, k.geometry.values)]
    add = pd.DataFrame({
        "key": "databc:" + k.FISH_OBSERVATION_POINT_ID.astype(str), "date": k.OBSERVATION_DATE.str[:10],
        "source": k.SOURCE, "life_stage": k.LIFE_STAGE, "lfid": k.linear_feature_id,
        "blk": k.blue_line_key, "m": k.m + k.downstream_route_measure, "wscode": k.wscode,
        "localcode": k.localcode, "acc": k.model_access_steelhead, "dams": k.barriers_dams_dnstr})
    print(f"records: {len(m):,} on the model, {len(add):,} snapped, "
          f"{len(miss) - len(add):,} left to the hand list")
    out = pd.concat([m, add], ignore_index=True)
    out["blk"] = out.blk.astype(int)
    left = pd.DataFrame(miss[~miss.index.isin(k.index)].drop(columns="geometry"))
    return out, left


def blocking_dams(gpkg: str):
    dams = pd.read_sql("""select dam_id, dam_name, dam_height, barrier_status, gnis_stream_name
        from crossings where dam_id is not null""", sqlite3.connect(gpkg))
    dams = dams.drop_duplicates("dam_id").set_index("dam_id")
    blocks: dict[str, str] = {}
    for i, d in dams.iterrows():
        name = d.dam_name if isinstance(d.dam_name, str) else None
        if name in PASSABLE_DAMS:
            continue
        if name in BLOCKING_DAMS or (d.barrier_status == "BARRIER" and pd.notna(d.dam_height)
                                     and d.dam_height >= MIN_DAM_HEIGHT_M):
            blocks[i] = name or f"unnamed dam on {d.gnis_stream_name} ({d.dam_height:.0f} m)"
    return blocks


def classify(rec: pd.DataFrame, blocks: dict[str, str]) -> pd.DataFrame:
    def dam(v):
        if not isinstance(v, str):
            return None
        return next((blocks[x.strip()] for x in v.split(";") if x.strip() in blocks), None)
    rec["blocking_dam"] = rec.dams.map(dam)
    rec["status"] = "confirmed"
    rec.loc[rec.blocking_dam.notna(), "status"] = "above_dam"
    rec.loc[~rec.acc.isin(ACCESSIBLE), "status"] = "above_natural_barrier"
    print(rec.status.value_counts().to_string())
    return rec


# ----------------------------------------------------------------------------- the atlas
class Atlas:
    """Atlas sections (`{blk}:{measure}`, `lake:{wbk}`) and the registry items holding them."""

    def __init__(self):
        build = GENERATED.require_build()
        self.items = {it["id"]: it for it in json.load(open(GENERATED.registry()))["items"]
                      if it["kind"] != "area"}
        self.sec_items = collections.defaultdict(list)
        for i, it in self.items.items():
            for s in it["section_ids"]:
                self.sec_items[s].append(i)
        self.starts = collections.defaultdict(list)
        for line in open(build / "section_handles.txt"):
            b, _, m = line.strip().rpartition(":")
            if b and not b.startswith("lake") and m.lstrip("-").isdigit():
                self.starts[int(b)].append(int(m))
        for v in self.starts.values():
            v.sort()
        still = {s for it in self.items.values() if it["kind"] in ("lake", "wetland")
                 for s in it["section_ids"] if s.startswith("lake:")}
        fwa = sqlite3.connect(SOURCE / "bc_fisheries_data.gpkg")
        self.lf_wbk = {int(a): int(b) for a, b in fwa.execute(
            "select LINEAR_FEATURE_ID, WATERBODY_KEY from streams "
            "where WATERBODY_KEY is not null and WATERBODY_KEY != 0") if f"lake:{int(b)}" in still}

    def sections(self, blk, dm, um, lfid) -> list[str]:
        w = self.lf_wbk.get(int(lfid)) if lfid == lfid else None
        if w:
            return [f"lake:{w}"]
        st = self.starts.get(int(blk))
        if not st:
            return []
        i = max(bisect.bisect_right(st, dm + 0.5) - 1, 0)
        j = bisect.bisect_left(st, um - 0.5)
        return [f"{int(blk)}:{st[k]}" for k in range(i, max(j, i + 1))]


def corridor(rec: pd.DataFrame, atlas: Atlas, gpkg: str) -> set[str]:
    """Sections downstream of a confirmed record: its own stream below it and every stream
    below the junctions on the way to the sea (FWA watershed codes)."""
    conf = rec[rec.status == "confirmed"]
    anc = {".".join(w.split(".")[:k]) for w in conf.wscode.unique()
           for k in range(1, len(w.split(".")) + 1)}
    con = sqlite3.connect(gpkg)
    con.execute("create temp table anc(w text primary key)")
    con.executemany("insert into anc values (?)", [(a,) for a in anc])
    seg = pd.read_sql("""select m.linear_feature_id lfid, m.blue_line_key blk,
        m.downstream_route_measure dm, m.upstream_route_measure um, m.wscode, m.localcode
        from model_access m join anc on anc.w = m.wscode""", con)

    def pos(local, ws):
        lg, wg = local.split("."), ws.split(".")
        return int(lg[len(wg)]) if len(lg) > len(wg) else 0

    seg["pos"] = [pos(l, w) for l, w in zip(seg.localcode, seg.wscode)]
    by_ws = dict(tuple(seg.groupby("wscode")))
    out: set[str] = set()
    for w, sub in conf.groupby("wscode"):
        g = w.split(".")
        hits = []
        own = by_ws.get(w)
        if own is not None:
            top_pos = max(pos(l, w) for l in sub.localcode)
            top_m = sub.groupby("blk").m.max().to_dict()
            hits.append(own[(own.pos < top_pos) | (own.blk.isin(top_m.keys())
                                                   & (own.dm <= own.blk.map(top_m).fillna(-1)))])
        for k in range(1, len(g)):
            a = by_ws.get(".".join(g[:k]))
            if a is not None:
                hits.append(a[a.pos < int(g[k])])
        for h in hits:
            for r in h.itertuples():
                out.update(atlas.sections(r.blk, r.dm, r.um, r.lfid))
    return out


def book_known(atlas: Atlas) -> set[str]:
    """Items every section of which a regulation row already makes known."""
    p = GENERATED.reaches / "full" / "steelhead_presence.jsonl"
    if not p.exists():
        raise FileNotFoundError(f"no reach run at {p.parent} — run "
                                "`python -m pipeline.atlas.reach.cli --build <build> --out <reaches>`")
    known = set()
    for line in open(p):
        r = json.loads(line)
        # book-known: a steelhead row's rule binds it (`regulations`); older runs said it by entry
        book = r.get("regulations") if "regulations" in r else r.get("entry_id") != CURATED_LIST
        if r["steelhead"] == "known" and book:
            known.add(r["section_id"])
    return {i for i, it in atlas.items.items() if it["section_ids"]
            and all(s in known for s in it["section_ids"])}


# ----------------------------------------------------------------------------- main
def main() -> None:
    gpkg, obs = fetch()
    rec, unplaced = load_records(gpkg, obs)
    rec = classify(rec, blocking_dams(gpkg))
    atlas = Atlas()
    rec["items"] = [sorted({i for s in atlas.sections(r.blk, r.m, r.m + 0.1, r.lfid)
                            for i in atlas.sec_items.get(s, ())}) for r in rec.itertuples()]
    route = corridor(rec, atlas, gpkg)
    on_route = collections.Counter(i for s in route for i in atlas.sec_items.get(s, ()))
    known = book_known(atlas)

    agg = collections.defaultdict(lambda: collections.Counter())
    last = collections.defaultdict(str)
    for r in rec.itertuples():
        y = str(r.date)[:4]
        for i in r.items:
            agg[i][r.status] += 1
            if r.status == "confirmed" and y.isdigit() and y < "9999":
                last[i] = max(last[i], y)

    rows = []
    for i in sorted(set(agg) | set(on_route) | set(HAND)):
        it = atlas.items[i]
        a = agg[i]
        basis = ("record" if a["confirmed"] else "route" if on_route[i] else
                 "above dam" if a["above_dam"] else "above natural barrier" if a else "hand")
        decision, why = "keep" if basis in ("record", "route") else "drop", ""
        if i in HAND:
            decision, why = HAND[i]
        elif i in REVIEW:
            decision, why = REVIEW[i]
        if not flows(it["kind"], it["name"]):
            decision, why = "drop", why or "a lake: steelhead cannot be told from rainbow"
        listed = decision == "keep"
        rows.append({"item_id": i, "name": it["name"], "kind": it["kind"],
                     "mus": ",".join(it.get("mus") or []), "basis": basis,
                     "confirmed_records": a["confirmed"], "above_dam_records": a["above_dam"],
                     "above_barrier_records": a["above_natural_barrier"], "latest_record": last[i],
                     "decision": decision, "reason": why, "book_known": i in known, "listed": listed})

    out = GENERATED.mkdir("steelhead")
    with open(out / "steelhead_review.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    def note(r):
        ev = (f"{r['confirmed_records']} steelhead record(s), latest {r['latest_record'] or 'undated'}"
              if r["basis"] == "record" else "on the route to confirmed records"
              if r["basis"] == "route" else r["reason"])
        return f"{r['name']}: {ev}"

    listed = sorted((r for r in rows if r["listed"]), key=lambda r: (r["mus"], r["name"]))
    waters = [{"item_id": r["item_id"], "note": note(r)} for r in listed]
    doc = {"$comment": [
        "GENERATED by pipeline.regs.steelhead.known_waters — the known-steelhead list built from "
        "evidence: BC Fish Observations (ST) on water sea-run steelhead can reach (provincial "
        "accessibility model, blocking dams), the route to the sea, a per-water review, no lakes "
        "(sloughs and canals count as streams). A presence indicator that binds no rule; on its "
        "streams a rainbow over 50 cm is a steelhead (anadromous_rainbow).",
        "To use it: a human copies this file over data/curated/regulations/steelhead_waters.json "
        "(keep that file's $comment), then the reach run and the bundle."],
        "generated": {"source": "data/generated/steelhead/steelhead_waters.json",
                      "generator": "pipeline.regs.steelhead.known_waters",
                      "date": fetched_on(), "fingerprint": fingerprint(waters)},
        "waters": waters}
    (out / "steelhead_waters.json").write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    print(f"{len(listed)} waters -> {out / 'steelhead_waters.json'}; "
          f"{len(rows)} reviewed -> {out / 'steelhead_review.csv'}")

    # WHAT IS NOT LISTED, and why — beside the list, since the list's own format takes `waters` only
    why_not = {"keep": "kept by the review, but a lake: steelhead cannot be told from rainbow",
               "drop": "dropped", "unsure": "unsure: evidence too thin to list"}
    dropped = [{"item_id": r["item_id"], "name": r["name"], "kind": r["kind"], "mus": r["mus"],
                "decision": r["decision"], "basis": r["basis"],
                "steelhead_records": r["confirmed_records"],
                "above_dam_records": r["above_dam_records"],
                "above_barrier_records": r["above_barrier_records"],
                "reason": r["reason"] or why_not[r["decision"]]}
               for r in sorted(rows, key=lambda r: (r["decision"], r["mus"], r["name"]))
               if not r["listed"]]
    un = unplaced.assign(name=unplaced.GAZETTED_NAME.fillna("(unnamed)").str.title(),
                         year=unplaced.OBSERVATION_DATE.fillna("").astype(str).str[:4],
                         group=unplaced.WATERBODY_IDENTIFIER.fillna("").str[-4:])
    not_placed = [{"name": n, "watershed_group": g, "waterbody_type": d.WATERBODY_TYPE.iloc[0],
                   "records": len(d), "latest": max(d.year) or "undated",
                   "hand": next((HAND[i][0] + ": " + HAND[i][1] for i in HAND
                                 if HAND[i][1].lower().startswith(n.lower() + ":")), None)}
                  for (g, n), d in un.groupby(["group", "name"])]
    (out / "steelhead_dropped.json").write_text(json.dumps({
        "$comment": "GENERATED by pipeline.regs.steelhead.known_waters: every water with steelhead "
                    "evidence that is NOT on steelhead_waters.json, with why (lakes, above a "
                    "barrier or dam, dropped or unsure in review), and the records no model "
                    "segment holds (interior lakes, northern rivers), by water.",
        "not_listed": dropped, "records_not_placed": not_placed}, indent=1, ensure_ascii=False) + "\n")
    print(f"{len(dropped)} not listed + {len(not_placed)} unplaced waters -> "
          f"{out / 'steelhead_dropped.json'}")



if __name__ == "__main__":
    main()
