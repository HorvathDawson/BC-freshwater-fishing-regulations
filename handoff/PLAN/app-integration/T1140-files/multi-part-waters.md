# T1140 step 1: every multi-part water in candidate release r-71d09ad82e81, status on 2026-11-25

Read-only. Lead part = `targetFor(store, item, null)` (app/packages/data/src/regs/inputs.ts:184-198): the first part in `part_ix` order (river order, mouth first) that has an answer key. Status per part = `regsPage(...).status` (app/packages/data/src/regs/pageable.ts:27, the page the site and app draw, adult resident) on 2026-11-25; 'closed all year' = closed and (part.closed_all_year or every strip run closed).

- waters with 2+ live (keyed, non-tidal) parts: **478** (all 478 have a /water page)
- of those, 2+ entries on the Map tab after T517 joins (live salmon assumed to read joined pieces alike): **456**
- all parts same status today: 227
- (a) lead CLOSED, another part OPEN today: **89**
- (b) lead OPEN, another part CLOSED today (unsafe): **156**
- lead part's head is just the water's name (no part named): 11 of 478
- lead part is not the longest part: 471; of which status differs from the longest: 173


## How sure, and what was checked by hand

- Read with the app's own code (origin/main 9c901dbce, whose app/packages + tools/seo source equals release/0.10.0 except changelog.ts) over the candidate folder: `targetFor` for the lead, `regsPage(...).status` + `keep.banner` + `keep.strip.runs` for each part, `loadRawWaterGroup` + `mergeNav` (every piece read alike) for M. Scripts: scan.ts / run.mjs / report.py beside this file; raw output scan.json.
- 'closed all year' agreed with water `part.closed_all_year` on every closed part (0 disagreements).
- Hand check vs production's own built pages (app/dist/prod-site, built 2026-11-24 from the previous release): Squamish heads "Squamish River", Game fish "Closed today", "Closed all year ... Federal / parks" = this scan's lead p1 (closed all year, head = water name). Capilano River (case b) heads "From the CN Railway Bridge (tidal boundary) up to the Cable Pool Bridge footbridge", Game fish "Open" with nothing saying parts 2-3 (Cable Pool Bridge up, Capilano Lake up) are closed all year = this scan.
- Not determined: M assumes the live salmon reads joined pieces alike (the Map can show more parts on a day DFO splits a run); the Salmon card's own open/closed is not in this list (game-fish page status only); adult resident only.

## Lead heads that do not name the part (head = the water's name): 11, all side-channel leads

Burman River, Bedwell River, Gordon River (CAY), Thelwood Creek (CAY), Rees Creek (CAY), Squamish River (CAY), Snehumption Creek, Comox Creek (CAY), Manson River, Shelagyote River, Omineca River. Every other lead's head names its span (lo -> hi), but no Regulations head says 'part N of M' or that other parts differ.

## (b) THE UNSAFE DIRECTION: lead part open, another part closed

| water | id | lead part (status) | closed part(s): ix, status, head |
|---|---|---|---|
| Brunette River | gnis:10070 | p1 open: Burlington Northern Railway Bridge (tidal boundary) → Cariboo Dam | p2 closed all year: Cariboo Dam → Salamander Creek → Brunette River |
| Bucklin Creek | gnis:10125 | p0 open: From the mouth up to the Garibaldi Park boundary | p1 closed all year: Garibaldi Park boundary → Source |
| Clachnacudainn Creek | gnis:10426 | p0 open: From the mouth up to Mount Revelstoke National Park of Canada boundary | p1 closed all year: Mount Revelstoke National Park boundary → Source |
| Angus Creek | gnis:10479 | p0 open: From the mouth up to the New Lake outlet | p1 closed all year: New Lake → Source |
| Little Qualicum River | gnis:10886 | p0 open: Mouth → 35 m downstream of hatchery fence | p1 closed all year: 35 m downstream of hatchery fence → Hatchery fence |
| Greenburn Creek | gnis:11027 | p0 open: From the mouth up to Southern Gulf Islands National Park Reserve of Canada boundary | p1 closed all year: Southern Gulf Islands National Park Reserve boundary → Greenburn Lake outlet |
| Gladys Creek | gnis:11216 | p0 open: From the mouth up to Gladys Lake Ecological Reserve boundary | p1 closed all year: Gladys Lake Ecological Reserve boundary → Gladys Lake outlet |
| Goat River | gnis:11219 | p0 open: From the mouth up to the Macleod Creek confluence | p1 closed today: From the Macleod Creek confluence up to the top of the river |
| Kanaka Creek | gnis:11318 | p1 open: CPR Bridge (tidal boundary) → 112 Avenue bridge | p2 closed all year: 112 Avenue bridge → Cliff Park Falls |
| Bell Creek | gnis:11613 | p0 open: From the mouth up to Haley Lake Ecological Reserve boundary | p1 closed all year: Haley Lake Ecological Reserve boundary → Source |
| Forsyth Creek | gnis:11815 | p0 open: Mouth → Connor Lake (3000 m downstream) | p2 closed all year: Connor Lake (3000 m downstream) → Connor Lakes outlet |
| Fort Nelson River | gnis:11819 | p0 open: Mouth → Fort Nelson River Ecological Reserve boundary (km 155.2) | p1 closed all year: Fort Nelson River Ecological Reserve boundary (km 155.2) → Fort Nelson River Ecological Reserve boundary (km 158) |
| Mahood Creek | gnis:12114 | p0 open: From the mouth up to 152nd Street | p2 closed all year: 152nd Street → Source |
| Cowichan River | gnis:12227 | p0 open: Mouth → White triangle boundary sign located in Sandy Pool Regional Park | p5 closed today: From the Greendale Trestle up to the Cowichan Lake outlet |
| Kingfisher Creek | gnis:12625 | p0 open: Mouth → Kingfisher Creek Ecological Reserve boundary (km 23.4) | p1 closed all year: Kingfisher Creek Ecological Reserve boundary (km 23.4) → Kingfisher Creek Ecological Reserve boundary (km 29.3) |
| Chapman Creek | gnis:12936 | p1 open: Tidal boundary → 100 m below the falls (below the power line crossing) | p2 closed all year: 100 m below the falls (below the power line crossing) → Falls below the power line crossing |
| Chehalis River | gnis:13015 | p0 open: Mouth → Chehalis River Hatchery outlet | p2 closed all year: Main logging road bridge ~2.4km downstream Chehalis Lk → Chehalis Lake outlet |
| Blue Canyon Creek | gnis:13698 | p0 open: From the mouth up to Ilgachuz Range Ecological Reserve boundary | p1 closed all year: Ilgachuz Range Ecological Reserve boundary → Source |
| Geddes Creek | gnis:13870 | p0 open: From the mouth up to Kootenay National Park of Canada boundary | p1 closed all year: Kootenay National Park boundary → Source |
| Kootenay River | gnis:14097 | p0 open: From the mouth up to the Brilliant Dam | p2 closed all year: Navigation dolphin (2 km below CPR Bridge) → CPR Bridge near Creston<br>p9 closed all year: Kootenay National Park boundary near the Yearling Creek confluence → Kootenay National Park boundary near the Boyce Creek confluence |
| Hamilton Creek | gnis:14360 | p0 open: From the mouth up to Mount Revelstoke National Park of Canada boundary | p1 closed all year: Mount Revelstoke National Park boundary → Source |
| Hansard Creek | gnis:14427 | p0 open: Mouth → Aleza Lake Ecological Reserve boundary (km 20.7) | p1 closed all year: Aleza Lake Ecological Reserve boundary (km 20.7) → Aleza Lake Ecological Reserve boundary (km 22) |
| Peace River | gnis:14619 | p1 open: B.C. border → Clayhurst Ecological Reserve boundary (km 1531.9) | p2 closed all year: Clayhurst Ecological Reserve boundary (km 1531.9) → Clayhurst Ecological Reserve boundary near the Alces River confluence<br>p4 closed all year: 1200 m downstream of Site C dam → Site C dam<br>p8 closed all year: Hwy. 29 bridge → Peace Canyon Dam<br>p9 closed all year: Dinosaur Lake → Williston Lake — Zone B outlet |
| Morice River | gnis:14918 | p0 open: Bulkley River confluence (the Forks) → Forks fishing boundary signs, 100 m up the Morice | p3 closed all year: Morice River Ecological Reserve boundary (km 29.7) → Morice River Ecological Reserve boundary (km 32.9)<br>p8 closed all year: Outlet of Morice Lake (2000 m downstream) → Morice Lake outlet |
| Maunder Creek | gnis:15107 | p0 open: From the mouth up to Mount Revelstoke National Park of Canada boundary | p1 closed all year: Mount Revelstoke National Park boundary → Source |
| Harris Creek | gnis:15312 | p0 open: From the mouth up to the Hemmingsen Creek confluence | p1 closed all year: Hemmingsen Creek confluence → Source |
| Stoddart Creek | gnis:15456 | p0 open: From the mouth up to Kootenay National Park of Canada boundary | p1 closed all year: Kootenay National Park boundary → Source |
| Lardeau River | gnis:16359 | p0 open: From the mouth up to the fishing boundary signs near the Mobbs Creek confluence (600 m below Trout Lake outlet) | p1 closed all year: The Mobbs Creek confluence (600 m below Trout Lake outlet) → Trout Lake outlet |
| Nahatlatch River | gnis:16471 | p0 open: Mouth → Logging bridge | p1 closed all year: Logging bridge → Frances Lake outlet |
| Nanaimo River | gnis:16505 | p1 open: From the Cedar Road bridge up to the Hwy 19 bridge | p5 closed all year: Power line at Bore Hole → Boulder Creek confluence |
| Ash River | gnis:17161 | p0 open: From the mouth up to the Lanternman Falls (200m downstream) | p2 closed all year: Dickson Falls (30 m downstream) → Dickson Falls<br>p4 closed all year: Dickson Lake → Elsie Lake outlet |
| Atnarko River | gnis:17209 | p0 open: Talchako River confluence → Tweedsmuir Park Lodge (Stuie) | p2 closed all year: Atnarko Park campsite boundary signs → Young Creek confluence<br>p3 closed all year: Young Creek confluence → Goat Creek confluence<br>p4 closed all year: Goat Creek confluence → South Atnarko River confluence |
| Englishman River | gnis:17355 | p0 open: Mouth → Top Bridge | p2 closed all year: 100 m downstream of lower falls, Englishman River Park → Lower falls, Englishman River Park |
| Jewel Creek | gnis:17440 | p0 open: From the mouth up to the Jewel Lake (1500 m downstream) | p1 closed all year: Jewel Lake (1500 m downstream) → Jewel Lake outlet |
| Clo-oose Creek | gnis:17519 | p0 open: From the mouth up to Pacific Rim National Park Reserve of Canada boundary | p1 closed all year: Pacific Rim National Park Reserve boundary → Clo-oose Lake outlet |
| Gold River | gnis:17593 | p0 open: Mouth → Cascade falls (80 m downstream) | p1 closed all year: Cascade falls (80 m downstream) → Cascade falls<br>p3 closed all year: Muchalat River confluence → Side channel above Elbow Creek<br>p4 closed all year: Side channel above Elbow Creek → Source |
| Babine River | gnis:17687 | p0 open: From the mouth up to the Nilkitkwa River confluence | p3 closed all year: 80 m downstream of adult fish counting fence → Adult counting fence<br>p4 closed all year: Adult counting fence → 100 m upstream of adult fish counting fence |
| Canyon Creek | gnis:17831 | p0 open: Mouth → Burnt Cabin Bog Ecological Reserve boundary near the Auber Creek confluence | p1 closed all year: Burnt Cabin Bog Ecological Reserve boundary near the Auber Creek confluence → Burnt Cabin Bog Ecological Reserve boundary (km 26.1)<br>p3 closed all year: Burnt Cabin Bog Ecological Reserve boundary (km 27.7) → Burnt Cabin Bog Ecological Reserve boundary near the Burbridge Creek confluence |
| Evans Creek | gnis:18490 | p0 open: Mouth → Evans Lake Ecological Reserve boundary (km 18.8) | p1 closed all year: Evans Lake Ecological Reserve boundary (km 18.8) → Evans Lake Ecological Reserve boundary (km 19.3) |
| Festuca Creek | gnis:19048 | p0 open: Mouth → Ilgachuz Range Ecological Reserve boundary (km 17.6) | p1 closed all year: Ilgachuz Range Ecological Reserve boundary (km 17.6) → Ilgachuz Range Ecological Reserve boundary (km 19.4) |
| Jump Creek | gnis:19172 | p0 open: From the mouth up to Haley Lake Ecological Reserve boundary (through 1 lake) | p1 closed all year: Haley Lake Ecological Reserve boundary → Source |
| Lyall Creek | gnis:19214 | p0 open: From the mouth up to Southern Gulf Islands National Park Reserve of Canada boundary | p1 closed all year: Southern Gulf Islands National Park Reserve boundary → Source |
| Lynn Creek | gnis:19230 | p0 open: Mouth → 150 m downstream of Twin Falls Bridge | p1 closed all year: 150 m downstream of Twin Falls Bridge → 200 m upstream of Twin Falls Bridge |
| Attichika Creek | gnis:19629 | p0 open: Mouth → 500 m downstream of the Kemess Creek confluence | p1 closed all year: 500 m downstream of the Kemess Creek confluence → 500 m upstream of the Kemess Creek confluence<br>p3 closed all year: 500 m downstream of the Thorne Creek confluence → 500 m upstream of the Thorne Creek confluence |
| Eaglenest Creek | gnis:19719 | p0 open: Mouth → Gladys Lake Ecological Reserve boundary (km 40.4) | p1 closed all year: Gladys Lake Ecological Reserve boundary (km 40.4) → Gladys Lake Ecological Reserve boundary (km 45.4) |
| Fording River | gnis:19955 | p0 open: From the mouth up to the Josephine Falls | p1 closed all year: Josephine Falls → Source |
| Coquihalla River | gnis:19983 | p0 open: From the mouth up to the southern entrance to the lower most railway tunnel | p1 closed all year: Southern entrance to the lower most railway tunnel → Northern entrance to the upper most railway tunnel<br>p2 closed today: From the northern entrance to the upper most railway tunnel up to the top of the river |
| Cormier Creek | gnis:20010 | p1 open: B.C. border → Blue/Dease Rivers Ecological Reserve boundary | p2 closed all year: Blue/Dease Rivers Ecological Reserve boundary → km 56.4<br>p4 closed all year: km 56.5 → Blue/Dease Rivers Ecological Reserve boundary |
| Sunbeam Creek | gnis:20290 | p0 open: From the mouth up to Sunbeam Creek Ecological Reserve boundary | p1 closed all year: Sunbeam Creek Ecological Reserve boundary → Source |
| Salmon River | gnis:20523 | p1 open: From the shore up to 232 Street | p2 closed all year: 232 Street → Naval Radio Section Aldergrove boundary (km 26.2)<br>p3 closed all year: Naval Radio Section Aldergrove boundary (km 26.2) → Naval Radio Section Aldergrove boundary (km 27.9)<br>p4 closed all year: Naval Radio Section Aldergrove boundary (km 27.9) → Source |
| Sutton Creek | gnis:20883 | p0 open: Mouth → Honeymoon Bay Ecological Reserve boundary (km 2.4) | p1 closed all year: Honeymoon Bay Ecological Reserve boundary (km 2.4) → Honeymoon Bay Ecological Reserve boundary (km 2.7) |
| Kliki Damen Creek | gnis:2106 | p0 open: Mouth → Tow Hill Ecological Reserve boundary (km 2.1) | p1 closed all year: Tow Hill Ecological Reserve boundary (km 2.1) → Tow Hill Ecological Reserve boundary (km 4.3) |
| Nicomekl River | gnis:21100 | p1 open: Burlington Northern Railway Bridge (tidal boundary) → 23 m below the Nicomekl flood dam | p4 closed all year: 208th Street → Source |
| Nimpkish River | gnis:21141 | p1 open: From the tidal boundary up to the Davie River confluence (through 1 lake) | p2 closed all year: Nimpkish River Ecological Reserve boundary near the Kiyu Creek confluence → Nimpkish River Ecological Reserve boundary near the Sebalhall Creek confluence |
| Ningunsaw River | gnis:21160 | p0 open: Mouth → Ningunsaw River Ecological Reserve boundary (km 12.9) | p1 closed all year: Ningunsaw River Ecological Reserve boundary (km 12.9) → Ningunsaw River Ecological Reserve boundary near the Alger Creek confluence |
| San Juan River | gnis:21201 | p0 open: Mouth → San Juan River Estuary Ecological Reserve boundary near the Murton Creek confluence | p2 closed all year: San Juan River Estuary Ecological Reserve boundary near the Murton Creek confluence → San Juan River Estuary Ecological Reserve boundary near the Renfrew Creek confluence<br>p4 closed all year: Fleet River confluence → Source |
| Hobiton Creek | gnis:21456 | p0 open: From the mouth up to Pacific Rim National Park Reserve of Canada boundary | p1 closed all year: Pacific Rim National Park Reserve boundary → Source |
| Theodosia River | gnis:21687 | p0 open: From the mouth up to the top of the river | p1 closed today: Theodosia River |
| Bulkley River | gnis:2195 | p0 open: Mouth → 100 m below Moricetown/Witset Canyon | p5 closed all year: Morice River confluence → Forks fishing boundary signs, 100 m up the upper Bulkley<br>p6 closed all year: Forks fishing boundary signs, 100 m up the upper Bulkley → Source |
| Shaft Creek | gnis:22009 | p0 open: From the mouth up to Kootenay National Park of Canada boundary | p1 closed all year: Kootenay National Park boundary → Source |
| Grayling River | gnis:2214 | p0 open: Mouth → Grayling River Hot Springs Ecological Reserve boundary (km 57.9) | p1 closed all year: Grayling River Hot Springs Ecological Reserve boundary (km 57.9) → Grayling River Hot Springs Ecological Reserve boundary (km 64.3) |
| Dry Gulch Creek | gnis:22184 | p0 open: From the mouth up to Kootenay National Park of Canada boundary | p1 closed all year: Kootenay National Park boundary → Source |
| Duncan River | gnis:22255 | p0 open: From the mouth up to the Lardeau River confluence | p1 closed all year: Lardeau River confluence → Duncan Dam |
| Hyland Creek | gnis:22355 | p0 open: From the mouth up to 152nd Street | p1 closed all year: 152nd Street → Source |
| Wahleach Creek | gnis:22472 | p0 open: From the mouth up to the Wahleach Lake outlet | p1 closed all year: Wahleach Lake → Source |
| Patsuk Creek | gnis:22688 | p0 open: Mouth → Patsuk Creek Ecological Reserve boundary (km 1) | p1 closed all year: Patsuk Creek Ecological Reserve boundary (km 1) → Patsuk Creek Ecological Reserve boundary (km 5.3) |
| Powell River | gnis:22753 | p0 open: From the mouth up to the Powell Lake outlet | p1 closed today: From Powell Lake up to the top of the river |
| Swift Creek | gnis:23077 | p0 open: Mouth → CNR bridge, Valemount | p1 closed all year: CNR bridge, Valemount → Weir |
| Tahltan River | gnis:23163 | p0 open: Mouth → 400 m upstream of Tahltan River Bridge | p1 closed all year: 400 m upstream of Tahltan River Bridge → Landslide ~750 m above Tahltan River Bridge |
| Nitinat River | gnis:23318 | p1 open: Head of Nitinat Lake (tidal boundary) → "Red Rock Pool," (100 m downstream) | p2 closed today: From the "Red Rock Pool," (100 m downstream) up to the fishing boundary signs 100 m upstream of Red Rock Pool<br>p8 closed all year: Parker Creek confluence → Source |
| Optimist Creek | gnis:24844 | p0 open: From the mouth up to Bowen Island Ecological Reserve boundary | p1 closed all year: Bowen Island Ecological Reserve boundary → Source |
| Serpentine River | gnis:24978 | p0 open: Mouth → 23 m below the Serpentine flood dam | p3 closed all year: 168th Street at Bothwell Park → Source |
| Warwick Creek | gnis:25205 | p0 open: From the mouth up to Bowen Island Ecological Reserve boundary | p1 closed all year: Bowen Island Ecological Reserve boundary → Source |
| Sustut River | gnis:25386 | p0 open: From the mouth up to the BCR Bridge at Bear River mouth | p1 closed all year: BCR Bridge at Bear River mouth → Source |
| Shuswap River | gnis:25478 | p0 open: From the mouth up to the Mara Lake outlet | p1 closed all year: Mara Lake → Mara Bridge (fishing boundary signs nearby)<br>p10 closed all year: Upper Shuswap River Ecological Reserve boundary (km 220.1) → Upper Shuswap River Ecological Reserve boundary near the Cirque Creek confluence |
| Sooke River | gnis:25725 | p1 open: Tidal boundary → 100 m downstream of Potholes Falls | p2 closed all year: 100 m downstream of Potholes Falls → Lower potholes falls |
| Weaver Creek | gnis:25951 | p0 open: From the mouth up to the Sakwi Creek confluence | p1 closed all year: Sakwi Creek confluence → Weaver Lake outlet |
| Salmon River | gnis:2628 | p1 open: From the tidal boundary up to the White River confluence | p3 closed all year: Kay Creek confluence → Rock Creek confluence<br>p4 closed all year: Rock Creek confluence → Source |
| Silverhope Creek | gnis:26290 | p0 open: From the mouth up to the Bailey Bridge | p1 closed all year: Bailey Bridge → Silver Lake outlet<br>p2 closed today: From Silver Lake up to the top of the river |
| Sinclair Creek | gnis:26343 | p0 open: From the mouth up to Kootenay National Park of Canada boundary | p1 closed all year: Kootenay National Park boundary → Source |
| Shale Creek | gnis:2676 | p0 open: From the mouth up to the Garibaldi Park boundary | p1 closed all year: Garibaldi Park boundary → Source |
| Ptarmigan Creek | gnis:27021 | p0 open: Mouth → Quarry Bridge | p1 closed all year: Quarry Bridge → Falls |
| Puntledge River | gnis:27060 | p0 open: Tsolum River confluence → 100 m downstream of the Morrison Creek confluence | p1 closed all year: 100 m downstream of the Morrison Creek confluence → 100 m upstream of the Morrison Creek confluence<br>p3 closed all year: 75 m downstream of Puntledge River hatchery fence → Puntledge lower hatchery weir<br>p4 closed all year: Puntledge lower hatchery weir → 50 m upstream of BC Hydro Puntledge generating-station tailrace<br>p6 closed all year: Base of Stotan Falls → BC Hydro diversion dam |
| Ruby Creek | gnis:27550 | p0 open: From the mouth up to the fishing boundary signs 100 m below Ruby Lake (through 1 lake) | p1 closed all year: 100 m below Ruby Lake → Ruby Lake outlet<br>p2 closed all year: Ruby Lake → Ambrose Lake outlet |
| Trematon Creek | gnis:27638 | p0 open: Mouth → Lasqueti Island Ecological Reserve boundary (km 0.8) | p1 closed all year: Lasqueti Island Ecological Reserve boundary (km 0.8) → Lasqueti Island Ecological Reserve boundary (km 0.9) |
| Wiggs Creek | gnis:27700 | p0 open: Mouth → Pacific Rim National Park Reserve boundary (km 0) | p1 closed all year: Pacific Rim National Park Reserve boundary (km 0) → Pacific Rim National Park Reserve boundary (km 1.3) |
| Quesnel River | gnis:27867 | p0 open: Mouth → Johnson Subdivision Bridge | p5 closed all year: 50 m downstream of Likely Bridge → 50 m upstream of Likely Bridge |
| Quinsam River | gnis:27883 | p0 open: Mouth → 300 m downstream of Quinsam Hatchery weir | p1 closed all year: 300 m downstream of Quinsam Hatchery weir → Power line crossing |
| Artlish River | gnis:28190 | p0 open: From the mouth up to the LA1000 FSR bridge | p1 closed today: From the LA1000 FSR bridge up to the top of the river |
| Ram Creek | gnis:28715 | p0 open: Mouth → Ram Creek Ecological Reserve boundary (km 3.4) | p1 closed all year: Ram Creek Ecological Reserve boundary (km 3.4) → Ram Creek Ecological Reserve boundary (km 4.9) |
| Violet Creek | gnis:28895 | p0 open: Mouth → Mara Meadows Ecological Reserve boundary near the Metcalfe Creek confluence | p1 closed all year: Mara Meadows Ecological Reserve boundary near the Metcalfe Creek confluence → Mara Meadows Ecological Reserve boundary (km 7.1) |
| Schmidt Creek | gnis:29131 | p0 open: Mouth → Robson Bight (Michael Bigg) Ecological Reserve boundary (km 0) | p1 closed all year: Robson Bight (Michael Bigg) Ecological Reserve boundary (km 0) → Robson Bight (Michael Bigg) Ecological Reserve boundary (km 0.3) |
| Toquaht River | gnis:2915 | p1 open: From the tidal boundary (DFO Subarea 23-10) up to the Toquart mainline | p3 closed today: From the boundary sign near falls up to the top of the river (through 1 lake) |
| Whatshan River | gnis:2918 | p0 open: From the mouth up to the Whatshan Lake outlet | p1 closed all year: Whatshan Lake → Source |
| White River | gnis:2923 | p0 open: Mouth → Pool signs 100 m below the salmon viewing platform | p1 closed all year: Pool signs 100 m below the salmon viewing platform → Pool signs 100 m above the salmon viewing platform<br>p3 closed today: From the Sayward Road bridge up to the top of the river |
| Skeena River | gnis:2936 | p1 open: Tidal boundary → Skeena River Ecological Reserve boundary (km 63.5) | p2 closed all year: Skeena River Ecological Reserve boundary (km 63.5) → Skeena River Ecological Reserve boundary near the Exchamsiks River confluence<br>p4 closed all year: Skeena River |
| Vance Creek | gnis:29621 | p0 open: Mouth → Vance Creek Ecological Reserve boundary (km 3.5) | p1 closed all year: Vance Creek Ecological Reserve boundary (km 3.5) → Vance Creek Ecological Reserve boundary (km 4.1) |
| Verdant Creek | gnis:29687 | p0 open: From the mouth up to Kootenay National Park of Canada boundary | p1 closed all year: Kootenay National Park boundary → Source |
| Tupper River | gnis:29883 | p0 open: From the mouth up to 100 m below the Swan Lake outlet weir | p1 closed all year: 100 m below the Swan Lake outlet weir → Swan Lake outlet |
| Yahk River | gnis:30003 | p1 open: B.C. border → Gilnockie Creek Ecological Reserve boundary (km 8) | p2 closed all year: Gilnockie Creek Ecological Reserve boundary (km 8) → Gilnockie Creek Ecological Reserve boundary (km 9) |
| Clanninick Creek | gnis:30031 | p0 open: Mouth → Clanninick Creek Ecological Reserve boundary (km 3.2) | p1 closed all year: Clanninick Creek Ecological Reserve boundary (km 3.2) → Clanninick Creek Ecological Reserve boundary (km 4.9) |
| Kokish River | gnis:30726 | p0 open: Mouth → 500 m downstream of IPP tail race | p1 closed all year: 500 m downstream of IPP tail race → The IPP tail race<br>p3 closed today: In Kokish canyon → The tail of the canyon pool (300 m downstream)<br>p4 closed all year: The tail of the canyon pool (300 m downstream) → Log boom ~100 m above the IPP intake<br>p5 closed today: Log boom ~100 m above the IPP intake → Bonanza River confluence |
| Zymoetz River | gnis:3137 | p0 open: Mouth → Hwy 16 bridge over the Zymoetz (Copper) | p2 closed all year: Zymoetz Canyon lower entrance = transmission-line sign → Zymoetz Canyon upper entrance<br>p5 closed all year: McDonell Lake (3000 m downstream) → McDonell Lake outlet |
| Chuckwalla River | gnis:31700 | p0 open: Mouth → Ten Mile Pool (200 m downstream) | p1 closed all year: Ten Mile Pool (200 m downstream) → Ten Mile Pool (100 m upstream) |
| Wahpeeto Creek | gnis:31813 | p0 open: Mouth → 100 m below the falls (4.5 km above Wakeman River) | p1 closed all year: 100 m below the falls (4.5 km above Wakeman River) → Falls (4.5 km above Wakeman River) |
| Drizzle Creek | gnis:32639 | p0 open: Mouth → Drizzle Lake Ecological Reserve boundary (km 6.5) | p1 closed all year: Drizzle Lake Ecological Reserve boundary (km 6.5) → Drizzle Lake Ecological Reserve boundary (km 11.4) |
| Big Creek | gnis:33206 | p0 open: Mouth → Big Creek Ecological Reserve boundary (km 2) | p1 closed all year: Big Creek Ecological Reserve boundary (km 2) → Big Creek Ecological Reserve boundary (km 4.3) |
| Lee Creek | gnis:34559 | p0 open: From the mouth up to Bowen Island Ecological Reserve boundary | p1 closed all year: Bowen Island Ecological Reserve boundary → Source |
| Yakoun River | gnis:3485 | p0 open: Mouth → 6 Mile Bridge | p2 closed today: From the fishing boundary signs ~13 km below Yakoun Lake up to the Yakoun Lake outlet |
| Longfellow Creek | gnis:35968 | p0 open: From the mouth up to Gwaii Haanas National Park Reserve and Haida Heritage Site boundary | p1 closed all year: Gwaii Haanas National Park Reserve and Haida Heritage Site boundary → Source |
| Haysport Creek | gnis:36570 | p0 open: From the mouth up to Gamble Creek Ecological Reserve boundary | p1 closed all year: Gamble Creek Ecological Reserve boundary → Source |
| Gingietl Creek | gnis:36750 | p0 open: From the mouth up to Gingietl Creek Ecological Reserve boundary | p1 closed all year: Gingietl Creek Ecological Reserve boundary → Source |
| Columbia River | gnis:37414 | p1 open: From the B.C. border up to the CPR Bridge, Robson | p5 closed all year: Hwy 1 bridge in Revelstoke → Revelstoke Dam |
| Will Creek | gnis:37496 | p0 open: Mouth → Gladys Lake Ecological Reserve boundary | p1 closed all year: Gladys Lake Ecological Reserve boundary → km 17.6<br>p3 closed all year: km 17.6 → Source |
| Russell Creek | gnis:37607 | p0 open: Mouth → Tsitika River Ecological Reserve boundary (km 0) | p1 closed all year: Tsitika River Ecological Reserve boundary (km 0) → Tsitika River Ecological Reserve boundary (km 1) |
| Bridge Creek | gnis:37782 | p0 open: From the mouth up to Mount Revelstoke National Park of Canada boundary | p1 closed all year: Mount Revelstoke National Park boundary → Source |
| Whitefoot Creek | gnis:37908 | p0 open: From the mouth up to Big White Mountain Ecological Reserve boundary | p1 closed all year: Big White Mountain Ecological Reserve boundary → Source |
| Williams Creek | gnis:3820 | p0 open: Mouth → Williams Creek Ecological Reserve boundary (km 24.6) | p1 closed all year: Williams Creek Ecological Reserve boundary (km 24.6) → Williams Creek Ecological Reserve boundary (km 29.1) |
| Ksi Sii Aks | gnis:3828 | p0 open: From the mouth up to the Nass Road Bridge | p1 closed today: From the Nass Road Bridge up to the top of the river (through 4 lakes) |
| Airplane Creek | gnis:38698 | p0 open: From the mouth up to Gladys Lake Ecological Reserve boundary | p1 closed all year: Gladys Lake Ecological Reserve boundary → Source |
| Pouce Coupé River | gnis:38821 | p1 open: B.C. border (km 50.8) → B.C. border (km 51.1) | p3 closed all year: B.C. border → Rolla Canyon Ecological Reserve boundary |
| Chemainus River | gnis:39267 | p1 open: From the tidal boundary up to the Banon Creek confluence | p3 closed all year: Copper Canyon Falls signs → Copper Canyon Falls |
| Fraser River | gnis:39325 | p1 open: CPR Bridge at Mission → Landstrom Bar sign zone boundary (km 156.4) | p7 closed all year: 2800 m downstream of Hell's Gate → Hell's Gate<br>p8 closed all year: Hell's Gate → 6500 m downstream of Boston Bar<br>p10 closed today: From the CNR bridge ~1km below Thompson up to the Thompson River confluence<br>p13 closed today: BC Hydro's tail race outflow channel → Seton River confluence<br>p14 closed today: Seton River confluence → Hwy 99 bridge at Lillooet |
| Kicking Horse River | gnis:39363 | p0 open: From the mouth up to Yoho National Park of Canada boundary | p1 closed all year: Yoho National Park boundary → Wapta Lake outlet |
| Campbell River | gnis:39532 | p1 open: From the cement block boundary sign up to the Maple Street boundary sign | p5 closed all year: Elk Falls → John Hart Dam power station<br>p9 closed all year: Strathcona Dam (100 m downstream) → Strathcona Dam |
| Hays Creek | gnis:4152 | p0 open: From the mouth up to the lower culvert near cannery | p1 closed all year: Lower culvert near cannery → Source |
| Cranberry River | gnis:4674 | p0 open: Mouth → Cranberry River Canyon (downstream end, estimated) | p1 closed all year: Cranberry River Canyon (downstream end, estimated) → Cranberry River falls (top of the canyon) |
| Bridge River | gnis:4945 | p0 open: From the mouth up to the Hwy 40 bridge | p2 closed all year: Yalakom River confluence → Terzaghi Dam |
| Mohun Creek | gnis:5316 | p0 open: Mouth → Menzies Bay logging mainline bridge | p1 closed all year: Menzies Bay logging mainline bridge → Morton Lake outlet |
| Woolsey Creek | gnis:5371 | p0 open: Mouth → Mount Revelstoke National Park boundary (km 0) | p1 closed all year: Mount Revelstoke National Park boundary (km 0) → Mount Revelstoke National Park boundary near the Maunder Creek confluence |
| Amor De Cosmos Creek | gnis:5480 | p0 open: From the mouth up to the logging road bridge | p1 closed all year: Logging road bridge → Upper falls |
| Capilano River | gnis:5922 | p1 open: From the CN Railway Bridge (tidal boundary) up to the Cable Pool Bridge footbridge | p2 closed all year: Cable Pool Bridge footbridge → Capilano Lake outlet<br>p3 closed all year: Capilano Lake → Source |
| Campbell River | gnis:7250 | p1 open: Burlington Northern Railway Bridge (tidal boundary) → Pedestrian bridge at the foot of Stayte Road | p4 closed all year: 12th Avenue → Source |
| Pitt River | gnis:7551 | p1 open: From the CPR Bridge up to the Pitt Lake outlet | p3 closed all year: Garibaldi Park boundary → Source |
| Anzac River | gnis:7607 | p0 open: From the mouth up to the North Anzac River confluence | p2 closed today: From 500 m below the Upper Anzac bridge up to 500 m above the Upper Anzac bridge |
| Beaver River | gnis:7636 | p0 open: Mouth → Glacier National Park boundary near the Mountain Creek confluence | p1 closed all year: Glacier National Park boundary near the Mountain Creek confluence → Glacier National Park boundary (km 70.2) |
| Coursier Creek | gnis:7678 | p0 open: From the mouth up to Mount Revelstoke National Park of Canada boundary | p1 closed all year: Mount Revelstoke National Park boundary → Source |
| Nathan Creek | gnis:7892 | p1 open: From the CN Railway Bridge (tidal boundary) up to 272 Street | p2 closed all year: 272 Street → Source |
| Goosegrass Creek | gnis:7953 | p0 open: From the mouth up to Goosegrass Creek Ecological Reserve boundary | p1 closed all year: Goosegrass Creek Ecological Reserve boundary → Source |
| St. Cyr Creek | gnis:8035 | p0 open: From the mouth up to Mount Revelstoke National Park of Canada boundary | p1 closed all year: Mount Revelstoke National Park boundary → Source |
| Palmer Creek | gnis:8098 | p0 open: From the mouth up to Kootenay National Park of Canada boundary | p1 closed all year: Kootenay National Park boundary → Source |
| Ospika River | gnis:8216 | p0 open: Mouth → Ospika Cones Ecological Reserve boundary (km 104.2) | p1 closed all year: Ospika Cones Ecological Reserve boundary (km 104.2) → Ospika Cones Ecological Reserve boundary (km 109.9) |
| Wiedenman Creek | gnis:8324 | p0 open: From the mouth up to Yoho National Park of Canada boundary | p1 closed all year: Yoho National Park boundary → Source |
| Wood River | gnis:8372 | p0 open: From the mouth up to the Hamber Prov. Park boundary | p1 closed today: From the Hamber Prov. Park boundary up to the Fortress Lake outlet |
| Hoodas Creek | gnis:8456 | p0 open: Mouth → Kingcome River/Atlatzi River Ecological Reserve boundary (km 1.7) | p1 closed all year: Kingcome River/Atlatzi River Ecological Reserve boundary (km 1.7) → Kingcome River/Atlatzi River Ecological Reserve boundary (km 2.8)<br>p3 closed all year: Kingcome River/Atlatzi River Ecological Reserve boundary (km 3.8) → Kingcome River/Atlatzi River Ecological Reserve boundary (km 4.3) |
| Chilliwack River | gnis:8634 | p0 open: Vedder Crossing Bridge → Orange triangular boundary signs (200 m upstream of Vedder Crossing bridge) | p3 closed all year: 100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary<br>p4 closed all year: Chilliwack River Ecological Reserve boundary → Source |
| Chef Creek | gnis:8643 | p0 open: Mouth → Bowser Ecological Reserve boundary (km 1.7) | p1 closed all year: Bowser Ecological Reserve boundary (km 1.7) → Bowser Ecological Reserve boundary (km 2.8) |
| Brem River | gnis:9238 | p0 open: Mouth → 100 m below the 1st falls | p1 closed all year: 100 m below the 1st falls → 1st falls<br>p3 closed today: From 2nd set of falls up to the top of the river |
| Ice River | gnis:9448 | p0 open: From the mouth up to Yoho National Park of Canada boundary | p1 closed all year: Yoho National Park boundary → Source |
| Alouette River | gnis:9630 | p0 open: From the mouth up to 216 Street bridge | p2 closed all year: Alouette boundary signs → Source |
| East Liumchen Creek | gnis:9766 | p0 open: From the mouth up to Liumchen Ecological Reserve boundary | p1 closed all year: Liumchen Ecological Reserve boundary → Source |
| Inchawka Creek | gnis:9810 | p0 open: From the mouth up to Skwaha Lake Ecological Reserve boundary | p1 closed all year: Skwaha Lake Ecological Reserve boundary → Source |
| Incomappleux River | gnis:9814 | p0 open: From the mouth up to Glacier National Park of Canada boundary | p1 closed all year: Glacier National Park boundary → Source |
| Lew Creek | gnis:9871 | p0 open: From the mouth up to Lew Creek Ecological Reserve boundary | p1 closed all year: Lew Creek Ecological Reserve boundary → Source |
| Liard River | gnis:9891 | p0 open: Mouth → Portage Brule Rapids Ecological Reserve boundary (km 698.9) | p1 closed all year: Portage Brule Rapids Ecological Reserve boundary (km 698.9) → Portage Brule Rapids Ecological Reserve boundary near the Coal River confluence |
| Okanagan River Oxbows | wsc:300-432687-459615 | p0 open: From the mouth up to the top of the river | p1 closed all year: Okanagan River Oxbows |

## (a) lead part closed, another part open

| water | id | lead part (status): head | banner |
|---|---|---|---|
| Lindeman Creek | gnis:10356 | p0 closed all year: Mouth → Chilkoot Trail National Historic Site boundary | Closed all year |
| Beaverfoot River | gnis:11132 | p0 closed all year: Mouth → Yoho National Park boundary | Closed all year |
| Crazy Creek | gnis:12863 | p0 closed all year: Mouth → Falls | Closed all year |
| Marmalade Creek | gnis:13531 | p0 closed all year: Mouth → Ram Creek Ecological Reserve boundary | Closed all year |
| Black River | gnis:13593 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Chilcotin River | gnis:13744 | p0 closed today: From the mouth up to the Chilko River confluence | No fishing for any game fish Oct 1–Jun 10. |
| Gilnockie Creek | gnis:14012 | p0 closed all year: Mouth → Gilnockie Creek Ecological Reserve boundary | Closed all year |
| Kootowis Creek | gnis:14096 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Kulaht Creek | gnis:14127 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Kuskanax Creek | gnis:14145 | p0 closed all year: Mouth → Kuskanax Creek Falls | Closed all year |
| Cultus Creek | gnis:14287 | p0 closed all year: Mouth → Laib Creek confluence | Closed all year |
| Cullite Creek | gnis:14288 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Lachine Creek | gnis:14991 | p0 closed all year: Mouth → Kootenay National Park boundary | Closed all year |
| Daer Creek | gnis:15180 | p0 closed all year: Mouth → Kootenay National Park boundary | Closed all year |
| Darling River | gnis:15267 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Kemess Creek | gnis:15709 | p0 closed all year: Mouth → Point 500 m upstream of Attichka Creek | Closed all year |
| Dean River | gnis:16075 | p0 closed today: From the mouth up to the signs ~100 m below the canyon (100 m downstream) | No fishing for any game fish Oct 1–May 31. |
| Logan Creek | gnis:17049 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Clonard Creek | gnis:17115 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Coffee Creek | gnis:17589 | p0 closed all year: Mouth → Falls | Closed all year |
| Camper Creek | gnis:17781 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Lost Shoe Creek | gnis:18015 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Wap Creek | gnis:1806 | p0 closed today: Mouth → Mount Griffin Ecological Reserve boundary near the Bowman Creek confluence | No fishing for any game fish Sep 1–Jun 30. Together these closures cover it: |
| Gordon River | gnis:18167 | p0 closed all year: Gordon River | Closed all year |
| Carmanah Creek | gnis:18401 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary (km 0.8) | Closed all year |
| Young Creek | gnis:1914 | p0 closed all year: Mouth → Hwy 20 | Closed all year |
| St. Leon Creek | gnis:20482 | p0 closed all year: Mouth → St. Leon Falls | Closed all year |
| Sandhill Creek | gnis:20576 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Sandstone Creek | gnis:20595 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Hellroaring Creek | gnis:20676 | p0 closed all year: Mouth → Angus Creek confluence | Closed all year |
| Hitchie Creek | gnis:21438 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Hunlen Creek | gnis:22290 | p0 closed all year: Mouth → Hunlen Falls | Closed all year |
| Walbran Creek | gnis:22491 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Powers Creek | gnis:22758 | p0 closed all year: Mouth → Hwy 97 bridge | Closed all year |
| Noons Creek | gnis:23366 | p1 closed all year: Railway bridge → Coquitlam Watershed boundary (km 6.7) | Closed all year |
| Norns Creek | gnis:23396 | p0 closed all year: Mouth → Norns Creek falls | Closed all year |
| Homan River | gnis:23788 | p0 closed all year: Mouth → Chilkoot Trail National Historic Site boundary | Closed all year |
| Horsefly River | gnis:24531 | p0 closed today: From the mouth up to the Woodjam Bridge | No fishing for any game fish Nov 1–May 31. |
| Liumchen Creek | gnis:2500 | p0 closed all year: Mouth → Lower falls | Closed all year |
| Shorts Creek | gnis:25455 | p0 closed all year: Mouth → Westside Road bridge | Closed all year |
| Squamish River | gnis:25671 | p1 closed all year: Squamish River | Closed all year |
| McRae Creek | gnis:27258 | p0 closed all year: Mouth → Falls/potholes | Closed all year |
| Snow Creek | gnis:27411 | p0 closed all year: Mouth → Hail Creek confluence | Closed all year |
| Trépanier Creek | gnis:27648 | p0 closed all year: Mouth → Hwy 97C bridge | Closed all year |
| Tamihi Creek | gnis:2773 | p0 closed all year: Mouth → Tamihi Creek Falls | Closed all year |
| Qualicum River | gnis:27804 | p1 closed all year: Hwy 19A bridge → = QFN | Closed all year |
| Aldworth Creek | gnis:28151 | p0 closed all year: Mouth → Tsitika River Ecological Reserve boundary | Closed all year |
| Trisle Creek | gnis:28455 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Trout Creek | gnis:28474 | p0 closed all year: Mouth → Rail trestle, Trout Creek Canyon | Closed all year |
| Wilson Creek | gnis:28553 | p0 closed all year: Mouth → Burkitt Creek confluence | Closed all year |
| Michigan Creek | gnis:28961 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Simpson River | gnis:2902 | p0 closed all year: Mouth → Kootenay National Park boundary | Closed all year |
| Millar Creek | gnis:29025 | p0 closed all year: Mouth → Honeymoon Bay Ecological Reserve boundary | Closed all year |
| Bonanza River | gnis:29055 | p0 closed today: From the mouth up to the Ida Lake outlet | No fishing for any game fish Nov 1–Apr 30. |
| Woodbury Creek | gnis:29329 | p0 closed all year: Mouth → Falls at the small hydro structure (800 m above Hwy 31 bridge) | Closed all year |
| Megin River | gnis:2939 | p0 closed all year: Mouth → Megin River Ecological Reserve boundary | Closed all year |
| Whitetail Creek | gnis:2959 | p0 closed all year: Mouth → Kootenay National Park boundary | Closed all year |
| Mission Creek | gnis:29608 | p0 closed all year: Mouth → Falls at Gallagher Canyon | Closed all year |
| Tahsish River | gnis:29803 | p0 closed all year: Mouth → Tahsish River Ecological Reserve boundary | Closed all year |
| Mitchell River | gnis:29965 | p0 closed today: From the mouth up to the Cameron Creek confluence | No fishing for any game fish Oct 1–Jun 30. |
| Keogh River | gnis:30697 | p0 closed all year: Mouth → Lower fish counting fence | Closed all year |
| Kiyu Creek | gnis:30715 | p0 closed all year: Mouth → Nimpkish River Ecological Reserve boundary | Closed all year |
| Klaskish River | gnis:30722 | p0 closed all year: Mouth → Klaskish River Ecological Reserve boundary | Closed all year |
| Thorne Creek | gnis:32114 | p0 closed all year: Mouth → 500 m above Attichika Creek | Closed all year |
| Exchamsiks River | gnis:3499 | p0 closed all year: Mouth → Skeena River Ecological Reserve boundary | Closed all year |
| Cross River | gnis:36203 | p0 closed all year: Mouth → Kootenay National Park boundary | Closed all year |
| Bonaparte River | gnis:38393 | p0 closed all year: Mouth → Falls at Bonaparte fishway | Closed all year |
| MacMillan Creek | gnis:38420 | p0 closed all year: Mouth → Gladys Lake Ecological Reserve boundary | Closed all year |
| Robson Creek | gnis:39445 | p0 closed all year: Mouth → Robson Bight (Michael Bigg) Ecological Reserve boundary | Closed all year |
| Thompson River | gnis:39492 | p0 closed today: Mouth → Easterly border of the Skihist Ecological reserve along the Thompson River locat | No fishing for any game fish Oct 1–Jun 30. Together these closures cover it: |
| Hegstrom Creek | gnis:43126 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Kalide Creek | gnis:44295 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Peachland Creek | gnis:5012 | p0 closed all year: Mouth → Hardy Falls | Closed all year |
| Tsitika River | gnis:5379 | p0 closed all year: Mouth → Robson Bight (Michael Bigg) Ecological Reserve boundary | Closed all year |
| Seymour River | gnis:6105 | p1 closed all year: CN Railway Bridge (tidal boundary) → Seymour Lake outlet | Closed all year |
| Halfway River | gnis:7059 | p0 closed all year: Mouth → Falls | Closed all year |
| 150 Yard Creek | gnis:7513 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Orange Juice Creek | gnis:7514 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Tsocowis Creek | gnis:7515 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Trestle Creek | gnis:7516 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Adrenaline Creek | gnis:7523 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Billy Goat Creek | gnis:7524 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Coal Creek | gnis:7525 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Cribs Creek | gnis:7526 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Illecillewaet River | gnis:7586 | p0 closed all year: Mouth → Albert Canyon | Closed all year |
| Klanawa River | gnis:7741 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Bonilla Creek | gnis:8899 | p0 closed all year: Mouth → Pacific Rim National Park Reserve boundary | Closed all year |
| Bowman Creek | gnis:9099 | p0 closed all year: Mouth → Mount Griffin Ecological Reserve boundary | Closed all year |
| Boyce Creek | gnis:9173 | p0 closed all year: Mouth → Kootenay National Park boundary | Closed all year |

## Every multi-part water

parts: `p<part_ix>` (`*` = the lead), O open / CT closed today / CAY closed all year, km, sections. M = entries on the Map tab (max join); lead entry = the lead's 'part N of M'.

| water | id | kind | raw parts | M | lead (part N) | lead status | lead head names the part | longest part (status) | case | parts |
|---|---|---|---|---|---|---|---|---|---|---|
| Canyon Creek | blk:356317915 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (0.36 km, 1 s); p1 O (4.53 km, 1 s) |
| Brunette River | gnis:10070 | stream | 3 | 3 | p1 (1) | open | yes | p3 (open) | b | p1* O (6.28 km, 1 s); p2 CAY (6.78 km, 1 s); p3 O (7.81 km, 1 s) |
| Bucklin Creek | gnis:10125 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (7.56 km, 1 s); p1 CAY (13.64 km, 2 s) |
| International Creek | gnis:10254 | stream | 2 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (3.86 km, 1 s); p2 O (6.18 km, 1 s) |
| Lindeman Creek | gnis:10356 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (26.04 km, 18 s); p1 O (27.2 km, 1 s) |
| Clachnacudainn Creek | gnis:10426 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (0.02 km, 2 s); p1 CAY (10.59 km, 3 s) |
| Angus Creek | gnis:10479 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (4.31 km, 3 s); p1 CAY (4.81 km, 1 s) |
| Antler Creek | gnis:10524 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (3.76 km, 2 s); p1 O (27.33 km, 1 s); p2 O (54.78 km, 2 s) |
| Bull River | gnis:10583 | stream | 5 | 5 | p0 (1) | open | yes | p4 (open) |  | p0* O (2.07 km, 1 s); p1 O (11.57 km, 2 s); p2 O (24.12 km, 2 s); p3 O (35.87 km, 2 s); p4 O (108.8 km, 6 s) |
| Buntzen Creek | gnis:10607 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (7.52 km, 4 s); p1 O (9.38 km, 9 s) |
| Burman River | gnis:10631 | stream | 2 | 2 | p0 (1) | open | NO | p1 (open) |  | p0* O (5.46 km, 6 s); p1 O (29.43 km, 8 s) |
| Iskut River | gnis:10765 | stream | 4 | 4 | p0 (1) | open | yes | p3 (open) |  | p0* O (77.03 km, 111 s); p1 O (190.16 km, 73 s); p2 O (197.47 km, 1 s); p3 O (241.99 km, 3 s) |
| Little Qualicum River | gnis:10886 | stream | 5 | 4 | p0 (1) | open | yes | p4 (open) | b | p0* O (2.88 km, 3 s); p1 CAY (2.92 km, 1 s); p2 O (12.57 km, 2 s); p3 O (13.26 km, 1 s); p4 O (18.11 km, 1 s) |
| Little Slocan River | gnis:10895 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (17.51 km, 11 s); p1 O (40.81 km, 2 s) |
| Greenburn Creek | gnis:11027 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (0.73 km, 1 s); p1 CAY (1.37 km, 1 s) |
| Beaverfoot River | gnis:11132 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (16.73 km, 6 s); p1 O (34.83 km, 8 s) |
| Gladys Creek | gnis:11216 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (1.85 km, 1 s); p1 CAY (7.28 km, 3 s) |
| Goat River | gnis:11219 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed today) | b | p0* O (35.91 km, 5 s); p1 CT (61.79 km, 3 s) |
| Goat River | gnis:11220 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (23.98 km, 1 s); p1 O (77.33 km, 9 s) |
| Kanaka Creek | gnis:11318 | stream | 3 | 3 | p1 (1) | open | yes | p3 (open) | b | p1* O (7.2 km, 1 s); p2 CAY (8.21 km, 1 s); p3 O (24.77 km, 3 s) |
| Bedwell River | gnis:11575 | stream | 2 | 2 | p0 (1) | open | NO | p1 (open) |  | p0* O (4.39 km, 7 s); p1 O (26.46 km, 6 s) |
| Beef Trail Creek | gnis:11578 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (16.38 km, 1 s); p1 O (49.04 km, 10 s) |
| Bell Creek | gnis:11613 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (5.44 km, 1 s); p1 CAY (5.9 km, 1 s) |
| Cautley Creek | gnis:11698 | stream | 2 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (135.23 km, 12 s); p2 O (138.84 km, 1 s) |
| Caycuse River | gnis:11710 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (11.66 km, 2 s); p1 O (26.22 km, 3 s) |
| Celista Creek | gnis:11747 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (1.93 km, 1 s); p1 O (42.79 km, 4 s) |
| Forsyth Creek | gnis:11815 | stream | 4 | 4 | p0 (1) | open | yes | p3 (open) | b | p0* O (13.41 km, 2 s); p1 O (- km, 1 s); p2 CAY (16.41 km, 1 s); p3 O (31.57 km, 10 s) |
| Fort Nelson River | gnis:11819 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (155.19 km, 30 s); p1 CAY (158.03 km, 1 s); p2 O (300.34 km, 17 s) |
| Kettle River | gnis:11984 | stream | 2 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (93.99 km, 3 s); p2 O (337.42 km, 26 s) |
| Mackin Creek | gnis:12053 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (42.55 km, 27 s); p1 O (43.01 km, 1 s); p2 O (77.07 km, 40 s) |
| Mahood Creek | gnis:12114 | stream | 3 | 3 | p0 (1) | open | yes | p2 (closed all year) | b | p0* O (2.82 km, 1 s); p1 O (- km, 4 s); p2 CAY (10.09 km, 1 s) |
| Cottonwood River | gnis:12179 | stream | 4 | 4 | p0 (1) | open | yes | p3 (open) |  | p0* O (0.01 km, 2 s); p1 O (5.65 km, 1 s); p2 O (11.47 km, 1 s); p3 O (63.59 km, 7 s) |
| Council Creek | gnis:12200 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (0.01 km, 1 s); p1 O (6.15 km, 2 s) |
| Cowichan River | gnis:12227 | stream | 6 | 4 | p0 (1) | open | yes | p5 (closed today) | b | p0* O (20.04 km, 9 s); p1 O (30.53 km, 2 s); p2 O (34.21 km, 1 s); p3 O (41.39 km, 1 s); p4 O (48.41 km, 1 s); p5 CT (49.9 km, 2 s) |
| Big Bend Creek | gnis:12427 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (24.69 km, 2 s); p1 O (33.31 km, 3 s); p2 O (55.11 km, 4 s) |
| Fulton River | gnis:12586 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (3.44 km, 3 s); p1 O (5.58 km, 2 s); p2 O (100.39 km, 10 s) |
| Kingfisher Creek | gnis:12625 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (23.42 km, 1 s); p1 CAY (29.29 km, 3 s); p2 O (29.35 km, 1 s) |
| Crazy Creek | gnis:12863 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.83 km, 1 s); p1 O (22.68 km, 7 s) |
| Chapman Creek | gnis:12936 | stream | 3 | 3 | p1 (1) | open | yes | p3 (open) | b | p1* O (3.9 km, 2 s); p2 CAY (4 km, 1 s); p3 O (25.29 km, 8 s) |
| Chehalis River | gnis:13015 | stream | 4 | 3 | p0 (1) | open | yes | p3 (open) | b | p0* O (3.59 km, 1 s); p1 O (19.16 km, 3 s); p2 CAY (20.9 km, 1 s); p3 O (43.98 km, 4 s) |
| Cheslatta River | gnis:13033 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (0.15 km, 1 s); p1 O (93.03 km, 14 s) |
| Marmalade Creek | gnis:13531 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.63 km, 1 s); p1 O (5.2 km, 1 s) |
| Marmotte Creek | gnis:13538 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (closed today) |  | p0* CAY (0.41 km, 1 s); p1 CT (6.85 km, 2 s) |
| Black River | gnis:13593 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (2.44 km, 1 s); p1 O (4.89 km, 3 s) |
| Blanchard River | gnis:13637 | stream | 3 | 3 | p1 (1) | open | yes | p5 (open) |  | p1* O (25.14 km, 3 s); p3 O (26.21 km, 1 s); p5 O (47.73 km, 11 s) |
| Blaney Creek | gnis:13643 | stream | 5 | 5 | p0 (1) | open | yes | p4 (open) |  | p0* O (0.98 km, 1 s); p1 O (2.94 km, 1 s); p2 O (3.82 km, 1 s); p3 O (4.07 km, 1 s); p4 O (13.08 km, 5 s) |
| Blue Canyon Creek | gnis:13698 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (20.13 km, 1 s); p1 CAY (27.73 km, 1 s) |
| Chilko River | gnis:13741 | stream | 3 | 3 | p0 (1) | closed today | yes | p2 (closed today) |  | p0* CT (62.38 km, 10 s); p1 CT (74.31 km, 7 s); p2 CT (87.63 km, 3 s) |
| Chilcotin River | gnis:13744 | stream | 2 | 2 | p0 (1) | closed today | yes | p1 (open) | a | p0* CT (109.94 km, 37 s); p1 O (334.47 km, 44 s) |
| Geddes Creek | gnis:13870 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (3.44 km, 1 s); p1 CAY (4.21 km, 1 s) |
| Gilnockie Creek | gnis:14012 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.13 km, 1 s); p1 O (28.8 km, 3 s) |
| Moberly River | gnis:14038 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (122.3 km, 39 s); p1 O (228.29 km, 23 s) |
| Moffat Creek | gnis:14045 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (9.95 km, 2 s); p1 O (92.53 km, 22 s) |
| Koksilah River | gnis:14076 | stream | 2 | 2 | p0 (1) | open | yes | p0 (open) |  | p0* O (48.54 km, 8 s); p1 O (- km, 5 s) |
| Kootowis Creek | gnis:14096 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.76 km, 1 s); p1 O (10.74 km, 1 s) |
| Kootenay River | gnis:14097 | stream | 10 | 10 | p0 (1) | open | yes | p10 (open) | b | p0* O (2.93 km, 2 s); p1 O (25.86 km, 4 s); p2 CAY (123.37 km, 1 s); p3 O (140.97 km, 1 s); p4 O (168.73 km, 2 s); p6 O (431.81 km, 1 s); p7 O (632.64 km, 53 s); p8 O (679.53 km, 3 s); p9 CAY (740.88 km, 18 s); p10 O (776 km, 24 s) |
| Kruger Creek | gnis:14117 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (16.19 km, 4 s); p1 O (24.91 km, 1 s) |
| Kulaht Creek | gnis:14127 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.05 km, 1 s); p1 O (6.37 km, 1 s) |
| Kuskanax Creek | gnis:14145 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (10.88 km, 3 s); p1 O (48.72 km, 6 s) |
| Kweishun Creek | gnis:14165 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (closed all year) |  | p0* CAY (3.2 km, 3 s); p1 CAY (9.03 km, 5 s) |
| Cultus Creek | gnis:14287 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (8.34 km, 3 s); p1 O (29.29 km, 2 s) |
| Cullite Creek | gnis:14288 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.32 km, 1 s); p1 O (12.23 km, 5 s) |
| Hamilton Creek | gnis:14360 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (1.87 km, 1 s); p1 CAY (6.44 km, 1 s) |
| Hansard Creek | gnis:14427 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (20.69 km, 11 s); p1 CAY (21.99 km, 1 s); p2 O (32.67 km, 7 s) |
| Blue Lead Creek | gnis:14477 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (1.54 km, 1 s); p1 O (19.29 km, 2 s) |
| Stamp River | gnis:14533 | stream | 8 | 5 | p0 (1) | open | yes | p7 (open) |  | p0* O (4.02 km, 3 s); p1 O (5.99 km, 1 s); p2 O (6.69 km, 1 s); p3 O (14.51 km, 1 s); p4 O (17.85 km, 1 s); p5 O (18.38 km, 1 s); p6 O (18.87 km, 1 s); p7 O (20.23 km, 1 s) |
| Stave River | gnis:14589 | stream | 2 | 2 | p1 (1) | open | yes | p2 (open) |  | p1* O (3.49 km, 1 s); p2 O (97.95 km, 28 s) |
| Peace River | gnis:14619 | stream | 9 | 9 | p1 (1) | open | yes | p9 (closed all year) | b | p1* O (1531.86 km, 1 s); p2 CAY (1534.85 km, 3 s); p3 O (1595.78 km, 56 s); p4 CAY (1596.98 km, 3 s); p5 O (1632.43 km, 20 s); p6 O (1642.43 km, 7 s); p7 O (1683.4 km, 11 s); p8 CAY (1684.32 km, 1 s); p9 CAY (1706.74 km, 1 s) |
| Pelican Creek | gnis:14674 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (1.51 km, 2 s); p1 O (26.99 km, 28 s) |
| Pennask Creek | gnis:14707 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (closed all year) |  | p0* CAY (20.88 km, 3 s); p1 CAY (33.64 km, 1 s) |
| Morice River | gnis:14918 | stream | 9 | 6 | p0 (1) | open | yes | p8 (closed all year) | b | p0* O (0.1 km, 1 s); p1 O (4.75 km, 3 s); p2 O (29.74 km, 3 s); p3 CAY (32.9 km, 1 s); p4 O (54.53 km, 19 s); p5 O (75.07 km, 29 s); p6 O (75.17 km, 2 s); p7 O (89.75 km, 6 s); p8 CAY (91.75 km, 5 s) |
| Lachine Creek | gnis:14991 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (4.44 km, 1 s); p1 O (14.84 km, 2 s) |
| Maunder Creek | gnis:15107 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (0.03 km, 1 s); p1 CAY (10.25 km, 1 s) |
| Daer Creek | gnis:15180 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (11.66 km, 2 s); p1 O (21.54 km, 3 s) |
| Darling River | gnis:15267 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.96 km, 1 s); p1 O (9.16 km, 1 s) |
| Harris Creek | gnis:15312 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (10.82 km, 1 s); p1 CAY (32.55 km, 2 s) |
| Harrison River | gnis:15333 | stream | 3 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (5.96 km, 2 s); p1 O (20.18 km, 6 s); p2 O (85.31 km, 1 s) |
| Stirling Creek | gnis:15440 | stream | 2 | 2 | p0 (1) | open | yes | p0 (open) |  | p0* O (28.44 km, 3 s); p1 O (- km, 1 s) |
| Stoddart Creek | gnis:15456 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (6.4 km, 1 s); p1 CAY (11.09 km, 1 s) |
| Kemess Creek | gnis:15709 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.5 km, 1 s); p1 O (21.71 km, 6 s) |
| Moyie River | gnis:15783 | stream | 2 | 2 | p1 (1) | open | yes | p2 (open) |  | p1* O (94.92 km, 10 s); p2 O (149.2 km, 9 s) |
| McClinchy Creek | gnis:15937 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (14.07 km, 5 s); p1 O (80.57 km, 25 s) |
| Dean River | gnis:16075 | stream | 10 | 7 | p0 (1) | closed today | yes | p9 (open) | a | p0* CT (3.75 km, 1 s); p1 CAY (5.24 km, 2 s); p2 CT (8.24 km, 4 s); p3 CT (47.83 km, 15 s); p4 CAY (77.54 km, 2 s); p5 O (121.91 km, 8 s); p6 O (209.9 km, 12 s); p7 O (241.71 km, 27 s); p8 O (261.08 km, 12 s); p9 O (300.17 km, 24 s) |
| Deception Creek | gnis:16091 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (7.88 km, 1 s); p1 O (57.36 km, 7 s) |
| Sturgeon Slough | gnis:16259 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (6.71 km, 5 s); p1 O (7.26 km, 1 s) |
| Langton Creek | gnis:16335 | stream | 2 | 2 | p1 (1) | open | yes | p3 (open) |  | p1* O (2.09 km, 1 s); p3 O (7.63 km, 1 s) |
| Lardeau River | gnis:16359 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (43.65 km, 11 s); p1 CAY (44.25 km, 1 s) |
| Lattice Creek | gnis:16403 | stream | 2 | 2 | p1 (1) | open | yes | p3 (open) |  | p1* O (15.38 km, 2 s); p3 O (24.09 km, 8 s) |
| Myra Creek | gnis:16438 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (0.61 km, 2 s); p1 O (15.99 km, 4 s) |
| Nahatlatch River | gnis:16471 | stream | 4 | 4 | p0 (1) | open | yes | p3 (open) | b | p0* O (12.31 km, 3 s); p1 CAY (12.77 km, 1 s); p2 O (18.04 km, 3 s); p3 O (85.86 km, 12 s) |
| Nahmint River | gnis:16478 | stream | 2 | 2 | p0 (1) | closed today | yes | p1 (closed today) |  | p0* CT (12.93 km, 1 s); p1 CT (44.03 km, 3 s) |
| Nakina River | gnis:16492 | stream | 3 | 1 | p0 (1) | open | yes | p2 (open) |  | p0* O (15.04 km, 29 s); p1 O (16.04 km, 1 s); p2 O (120.98 km, 24 s) |
| Nanaimo River | gnis:16505 | stream | 7 | 6 | p1 (1) | open | yes | p7 (open) | b | p1* O (3.24 km, 1 s); p2 O (10.46 km, 3 s); p3 O (12.48 km, 2 s); p4 O (12.98 km, 1 s); p5 CAY (22.35 km, 1 s); p6 O (38.37 km, 3 s); p7 O (68.57 km, 4 s) |
| Natsadalia Creek | gnis:16562 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (1.98 km, 3 s); p1 O (21.64 km, 17 s) |
| Nechako River | gnis:16605 | stream | 6 | 6 | p0 (1) | open | yes | p5 (open) |  | p0* O (8.45 km, 7 s); p1 O (146.28 km, 25 s); p2 O (275.38 km, 34 s); p3 O (280.38 km, 1 s); p4 O (280.55 km, 2 s); p5 O (288.85 km, 23 s) |
| Eldred River | gnis:16827 | stream | 2 | 2 | p0 (1) | closed today | yes | p1 (closed today) |  | p0* CT (12.82 km, 1 s); p1 CT (51.81 km, 3 s) |
| Elk River | gnis:16880 | stream | 8 | 8 | p0 (1) | open | yes | p7 (open) |  | p0* O (22.95 km, 2 s); p1 O (40.99 km, 4 s); p2 O (58.82 km, 11 s); p3 O (70.12 km, 5 s); p4 O (92.68 km, 7 s); p5 O (110.47 km, 5 s); p6 O (164.32 km, 19 s); p7 O (219.72 km, 9 s) |
| Lodge Creek | gnis:17039 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (1.63 km, 1 s); p1 O (13.32 km, 11 s) |
| Lodgepole Creek | gnis:17044 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (23.08 km, 1 s); p1 O (29.86 km, 2 s) |
| Logan Creek | gnis:17049 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.27 km, 1 s); p1 O (11.27 km, 3 s) |
| Clearwater River | gnis:17065 | stream | 4 | 4 | p0 (1) | open | yes | p3 (open) |  | p0* O (2.49 km, 2 s); p1 O (42.13 km, 14 s); p2 O (75.74 km, 6 s); p3 O (177.33 km, 24 s) |
| Clonard Creek | gnis:17115 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.81 km, 1 s); p1 O (5.16 km, 1 s) |
| Ash River | gnis:17161 | stream | 6 | 6 | p0 (1) | open | yes | p5 (open) | b | p0* O (5.56 km, 4 s); p1 O (10.87 km, 1 s); p2 CAY (10.9 km, 1 s); p3 O (12.42 km, 1 s); p4 CAY (26.18 km, 5 s); p5 O (67.78 km, 8 s) |
| Atnarko River | gnis:17209 | stream | 7 | 6 | p0 (1) | open | yes | p6 (open) | b | p0* O (2.34 km, 1 s); p1 O (19.63 km, 8 s); p2 CAY (22.37 km, 1 s); p3 CAY (35.98 km, 12 s); p4 CAY (58.21 km, 10 s); p5 O (63.79 km, 1 s); p6 O (106.52 km, 10 s) |
| Eagle River | gnis:17258 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (69.47 km, 5 s); p1 O (92.33 km, 6 s) |
| Englishman River | gnis:17355 | stream | 4 | 4 | p0 (1) | open | yes | p3 (open) | b | p0* O (4.31 km, 3 s); p1 O (15.6 km, 2 s); p2 CAY (15.7 km, 1 s); p3 O (39.9 km, 6 s) |
| Jewel Creek | gnis:17440 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (3.25 km, 1 s); p1 CAY (4.75 km, 1 s) |
| Clo-oose Creek | gnis:17519 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (0.6 km, 1 s); p1 CAY (1.09 km, 1 s) |
| Coal Creek | gnis:17550 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (6.87 km, 2 s); p1 O (22.65 km, 1 s) |
| Coffee Creek | gnis:17589 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (10.76 km, 1 s); p1 O (23.58 km, 2 s) |
| Gold River | gnis:17593 | stream | 5 | 5 | p0 (1) | open | yes | p4 (closed all year) | b | p0* O (6.26 km, 4 s); p1 CAY (6.34 km, 1 s); p2 O (24.68 km, 3 s); p3 CAY (48.97 km, 3 s); p4 CAY (59.72 km, 11 s) |
| Babine River | gnis:17687 | stream | 8 | 8 | p0 (1) | open | yes | p7 (open) | b | p0* O (94.17 km, 7 s); p1 O (96.83 km, 1 s); p2 O (97.4 km, 1 s); p3 CAY (97.48 km, 1 s); p4 CAY (97.58 km, 1 s); p5 O (99.05 km, 1 s); p6 O (110.73 km, 1 s); p7 O (111.12 km, 1 s) |
| Camper Creek | gnis:17781 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.13 km, 1 s); p1 O (15.83 km, 2 s) |
| Canyon Creek | gnis:17831 | stream | 5 | 5 | p0 (1) | open | yes | p4 (open) | b | p0* O (22.47 km, 2 s); p1 CAY (26.08 km, 1 s); p2 O (27.68 km, 1 s); p3 CAY (30.12 km, 1 s); p4 O (49.76 km, 13 s) |
| Lost Shoe Creek | gnis:18015 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (4.02 km, 1 s); p1 O (14.41 km, 4 s) |
| Wap Creek | gnis:1806 | stream | 5 | 5 | p0 (1) | closed today | yes | p4 (open) | a | p0* CT (24.32 km, 3 s); p1 CT (28.12 km, 3 s); p2 CAY (28.25 km, 3 s); p3 CT (33.4 km, 1 s); p4 O (53.17 km, 8 s) |
| Gordon River | gnis:18167 | stream | 3 | 3 | p0 (1) | closed all year | NO | p2 (closed today) | a | p0* CAY (0.16 km, 1 s); p1 O (12.27 km, 6 s); p2 CT (50.11 km, 2 s) |
| Carmanah Creek | gnis:18401 | stream | 4 | 4 | p0 (1) | closed all year | yes | p3 (open) | a | p0* CAY (0.82 km, 1 s); p1 O (2.16 km, 1 s); p2 CAY (2.62 km, 1 s); p3 O (24.73 km, 1 s) |
| Evans Creek | gnis:18490 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (18.75 km, 12 s); p1 CAY (19.3 km, 1 s); p2 O (24.63 km, 5 s) |
| Granby River | gnis:18775 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (50.75 km, 26 s); p1 O (125.51 km, 9 s) |
| Festuca Creek | gnis:19048 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (17.63 km, 4 s); p1 CAY (19.37 km, 1 s); p2 O (21 km, 1 s) |
| Findlay Creek | gnis:19077 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (18.47 km, 2 s); p1 O (25.82 km, 1 s); p2 O (82.73 km, 12 s) |
| Jones Creek | gnis:19104 | stream | 2 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (9.56 km, 6 s); p2 O (12.24 km, 2 s) |
| Jordan River | gnis:19117 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (closed today) |  | p0* CAY (4.29 km, 1 s); p1 CT (42.14 km, 8 s) |
| Jordan River | gnis:19118 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (26.94 km, 3 s); p1 O (33.67 km, 3 s) |
| Young Creek | gnis:1914 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (10.59 km, 1 s); p1 O (37.36 km, 2 s) |
| Jump Creek | gnis:19172 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (17.32 km, 5 s); p1 CAY (17.35 km, 1 s) |
| Lyall Creek | gnis:19214 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (0.35 km, 1 s); p1 CAY (3.35 km, 1 s) |
| Lynn Creek | gnis:19230 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (5.25 km, 1 s); p1 CAY (5.6 km, 1 s); p2 O (19.28 km, 8 s) |
| Asher Creek | gnis:19619 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (3.79 km, 2 s); p1 O (18.97 km, 2 s) |
| Attichika Creek | gnis:19629 | stream | 5 | 5 | p0 (1) | open | yes | p4 (open) | b | p0* O (7.59 km, 6 s); p1 CAY (8.59 km, 1 s); p2 O (19.66 km, 2 s); p3 CAY (20.66 km, 1 s); p4 O (45.17 km, 13 s) |
| Eaglenest Creek | gnis:19719 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (40.43 km, 5 s); p1 CAY (45.44 km, 1 s); p2 O (52.88 km, 3 s) |
| Canim River | gnis:1992 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (4.33 km, 1 s); p1 O (38.99 km, 4 s) |
| Fording River | gnis:19955 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (20.2 km, 4 s); p1 CAY (77.34 km, 11 s) |
| Coquihalla River | gnis:19983 | stream | 3 | 3 | p0 (1) | open | yes | p2 (closed today) | b | p0* O (7.9 km, 2 s); p1 CAY (8.85 km, 1 s); p2 CT (64.71 km, 5 s) |
| Coquitlam River | gnis:19986 | stream | 3 | 3 | p1 (1) | open | yes | p3 (open) |  | p1* O (7.03 km, 2 s); p2 O (16.31 km, 1 s); p3 O (46.92 km, 10 s) |
| Mahood River | gnis:2001 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (2.9 km, 1 s); p1 O (5.82 km, 1 s) |
| Cormier Creek | gnis:20010 | stream | 5 | 5 | p1 (1) | open | yes | p5 (open) | b | p1* O (53.63 km, 62 s); p2 CAY (56.4 km, 2 s); p3 O (56.5 km, 1 s); p4 CAY (58.57 km, 6 s); p5 O (72.34 km, 28 s) |
| Heber River | gnis:20215 | stream | 3 | 3 | p0 (1) | closed all year | yes | p2 (closed today) |  | p0* CAY (1.3 km, 1 s); p1 CT (6.72 km, 3 s); p2 CT (27.96 km, 13 s) |
| Sunbeam Creek | gnis:20290 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (5.42 km, 1 s); p1 CAY (6.59 km, 1 s) |
| St. Leon Creek | gnis:20482 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (3.33 km, 1 s); p1 O (24.38 km, 2 s) |
| Salmon River | gnis:20523 | stream | 4 | 4 | p1 (1) | open | yes | p4 (closed all year) | b | p1* O (12.63 km, 2 s); p2 CAY (26.17 km, 1 s); p3 CAY (27.87 km, 1 s); p4 CAY (34.2 km, 9 s) |
| Salmo River | gnis:20528 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (12.26 km, 2 s); p1 O (22.25 km, 2 s); p2 O (62.25 km, 14 s) |
| Sand Creek | gnis:20573 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (14.85 km, 1 s); p1 O (42.92 km, 2 s) |
| Sandhill Creek | gnis:20576 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (4.78 km, 1 s); p1 O (6.84 km, 1 s) |
| Sandstone Creek | gnis:20595 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.55 km, 1 s); p1 O (5.44 km, 1 s) |
| Hellroaring Creek | gnis:20676 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (5.14 km, 1 s); p1 O (31.05 km, 4 s) |
| Sutton Creek | gnis:20883 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (2.39 km, 1 s); p1 CAY (2.67 km, 1 s); p2 O (16.12 km, 1 s) |
| Petitot River | gnis:20973 | stream | 5 | 5 | p0 (1) | open | yes | p8 (open) |  | p0* O (63.27 km, 2 s); p2 O (72.06 km, 1 s); p4 O (75.41 km, 1 s); p6 O (100.19 km, 2 s); p8 O (296.67 km, 16 s) |
| Burnt Bridge Creek | gnis:2100 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (11.6 km, 2 s); p1 O (22.97 km, 10 s) |
| Phyllis Creek | gnis:21016 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (5.5 km, 3 s); p1 O (5.54 km, 1 s) |
| Pierce Creek | gnis:21034 | stream | 3 | 3 | p0 (1) | closed all year | yes | p2 (closed all year) |  | p0* CAY (1.59 km, 1 s); p1 CAY (2.12 km, 1 s); p2 CAY (5.7 km, 1 s) |
| Kliki Damen Creek | gnis:2106 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (2.11 km, 1 s); p1 CAY (4.31 km, 3 s); p2 O (11.05 km, 3 s) |
| Nicomekl River | gnis:21100 | stream | 4 | 3 | p1 (1) | open | yes | p4 (closed all year) | b | p1* O (4.78 km, 1 s); p2 O (4.8 km, 1 s); p3 O (24.25 km, 2 s); p4 CAY (36.59 km, 3 s) |
| Nicomen Slough | gnis:21105 | stream | 3 | 1 | p0 (1) | open | yes | p2 (open) |  | p0* O (3.37 km, 1 s); p1 O (20.56 km, 12 s); p2 O (22.75 km, 3 s) |
| Nimpkish River | gnis:21141 | stream | 3 | 3 | p1 (1) | open | yes | p3 (open) | b | p1* O (69.38 km, 11 s); p2 CAY (88.64 km, 1 s); p3 O (117.02 km, 10 s) |
| Ningunsaw River | gnis:21160 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (12.93 km, 23 s); p1 CAY (22.71 km, 10 s); p2 O (50.89 km, 16 s) |
| San Juan River | gnis:21201 | stream | 4 | 4 | p0 (1) | open | yes | p4 (closed all year) | b | p0* O (1.15 km, 5 s); p2 CAY (6.02 km, 4 s); p3 O (30.02 km, 4 s); p4 CAY (56.27 km, 2 s) |
| Old Kootenay River Channel | gnis:2123 | stream | 3 | 3 | p0 (1) | open | yes | p0 (open) |  | p0* O (11.38 km, 1 s); p1 O (- km, 1 s); p2 O (- km, 2 s) |
| Hiding Creek | gnis:21385 | stream | 3 | 3 | p0 (1) | open | yes | p4 (open) |  | p0* O (34.31 km, 2 s); p2 O (35.29 km, 1 s); p4 O (50.46 km, 1 s) |
| Hitchie Creek | gnis:21438 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (5.37 km, 2 s); p1 O (9.8 km, 1 s) |
| Hobiton Creek | gnis:21456 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (1.01 km, 1 s); p1 CAY (11.68 km, 2 s) |
| Hummingbird Creek | gnis:21657 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (12.62 km, 1 s); p1 O (13.9 km, 3 s) |
| Thelwood Creek | gnis:21681 | stream | 2 | 2 | p0 (1) | closed all year | NO | p1 (closed all year) |  | p0* CAY (0.38 km, 2 s); p1 CAY (19.85 km, 8 s) |
| Theodosia River | gnis:21687 | stream | 2 | 2 | p0 (1) | open | yes | p0 (open) | b | p0* O (27.45 km, 9 s); p1 CT (- km, 2 s) |
| Oyster River | gnis:21875 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (7.15 km, 1 s); p1 O (50.22 km, 8 s); p2 O (55.87 km, 3 s) |
| Page Creek | gnis:21902 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (1.72 km, 1 s); p1 O (2.06 km, 1 s); p2 O (5.43 km, 4 s) |
| Pamela Creek | gnis:21942 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (9.02 km, 3 s); p1 O (16.65 km, 5 s) |
| Bulkley River | gnis:2195 | stream | 7 | 5 | p0 (1) | open | yes | p6 (closed all year) | b | p0* O (54.86 km, 5 s); p1 O (55.29 km, 3 s); p2 O (157.68 km, 21 s); p3 O (163.91 km, 6 s); p4 O (164.91 km, 1 s); p5 CAY (165.01 km, 1 s); p6 CAY (281.51 km, 44 s) |
| Seymour River | gnis:21993 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (14.35 km, 3 s); p1 O (73.8 km, 12 s) |
| Shaft Creek | gnis:22009 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (0.09 km, 1 s); p1 CAY (3.35 km, 1 s) |
| Shawatum Creek | gnis:22066 | stream | 3 | 3 | p0 (1) | closed today | yes | p2 (closed today) |  | p0* CT (2.07 km, 1 s); p1 CAY (3.22 km, 1 s); p2 CT (10.7 km, 5 s) |
| Grayling River | gnis:2214 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (57.93 km, 27 s); p1 CAY (64.35 km, 2 s); p2 O (133.09 km, 52 s) |
| Dry Gulch Creek | gnis:22184 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (2.21 km, 1 s); p1 CAY (6 km, 1 s) |
| Duncan River | gnis:22255 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (8.9 km, 8 s); p1 CAY (9.9 km, 1 s); p2 O (115.76 km, 9 s) |
| Hunlen Creek | gnis:22290 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.69 km, 2 s); p1 O (32.7 km, 16 s) |
| Hyland Creek | gnis:22355 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (1.17 km, 1 s); p1 CAY (6 km, 1 s) |
| Wahleach Creek | gnis:22472 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (9.64 km, 2 s); p1 CAY (19.87 km, 1 s) |
| Walbran Creek | gnis:22491 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.86 km, 1 s); p1 O (29.74 km, 2 s) |
| Wapiti River | gnis:22559 | stream | 2 | 2 | p1 (1) | open | yes | p3 (open) |  | p1* O (169.96 km, 2 s); p3 O (274.4 km, 44 s) |
| Patsuk Creek | gnis:22688 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (1.04 km, 1 s); p1 CAY (5.31 km, 1 s); p2 O (18.09 km, 1 s) |
| Paul Creek | gnis:22702 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (16.74 km, 1 s); p1 O (46.97 km, 6 s) |
| Pothole Creek | gnis:22737 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (7.19 km, 4 s); p1 O (10.19 km, 1 s); p2 O (27.34 km, 10 s) |
| Powell River | gnis:22753 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed today) | b | p0* O (0.83 km, 1 s); p1 CT (84.64 km, 12 s) |
| Powers Creek | gnis:22758 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (5.55 km, 1 s); p1 O (32.05 km, 7 s) |
| Shegunia River | gnis:22777 | stream | 3 | 1 | p0 (1) | open | yes | p2 (open) |  | p0* O (3.35 km, 1 s); p1 O (3.45 km, 1 s); p2 O (47.94 km, 4 s) |
| Guichon Creek | gnis:2286 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (34.17 km, 3 s); p1 O (88.19 km, 6 s) |
| Swift Creek | gnis:23077 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (4.66 km, 1 s); p1 CAY (4.8 km, 1 s); p2 O (26.8 km, 1 s) |
| Kispiox River | gnis:2308 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (13.69 km, 1 s); p1 O (163.9 km, 26 s) |
| Tahltan River | gnis:23163 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (0.85 km, 2 s); p1 CAY (1.21 km, 1 s); p2 O (92.68 km, 22 s) |
| Pine River | gnis:23217 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (170.9 km, 44 s); p1 O (290.33 km, 33 s) |
| Pinkut Creek | gnis:23229 | stream | 3 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (1.46 km, 1 s); p1 O (1.49 km, 1 s); p2 O (87.07 km, 21 s) |
| Nitinat River | gnis:23318 | stream | 8 | 6 | p1 (1) | open | yes | p8 (closed all year) | b | p1* O (28.46 km, 2 s); p2 CT (28.66 km, 1 s); p3 O (29.7 km, 1 s); p4 O (30.03 km, 1 s); p5 O (32.22 km, 1 s); p6 O (32.31 km, 1 s); p7 O (40.92 km, 1 s); p8 CAY (79.55 km, 3 s) |
| Noons Creek | gnis:23366 | stream | 4 | 4 | p1 (1) | closed all year | yes | p3 (closed all year) | a | p1* CAY (6.71 km, 2 s); p2 CAY (8.07 km, 3 s); p3 CAY (8.11 km, 2 s); p4 O (- km, 1 s) |
| Norns Creek | gnis:23396 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (2.34 km, 5 s); p1 O (29.77 km, 6 s) |
| North Obsidian Creek | gnis:23461 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (1.21 km, 1 s); p1 O (12.32 km, 5 s) |
| Koch Creek | gnis:2349 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (2.45 km, 2 s); p1 O (41.05 km, 2 s) |
| Homan River | gnis:23788 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (5.24 km, 2 s); p1 O (36.1 km, 15 s) |
| Obsidian Creek | gnis:24154 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (13.36 km, 1 s); p1 O (20.8 km, 2 s) |
| Scott Creek | gnis:24243 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (closed all year) |  | p0* CAY (8.79 km, 3 s); p1 CAY (9.41 km, 1 s) |
| Dog Creek | gnis:24390 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (20.97 km, 2 s); p1 O (22.36 km, 1 s); p2 O (24.55 km, 5 s) |
| Horsefly River | gnis:24531 | stream | 3 | 3 | p0 (1) | closed today | yes | p2 (open) | a | p0* CT (41.99 km, 10 s); p1 CT (67.63 km, 3 s); p2 O (135.02 km, 18 s) |
| Houlgate Creek | gnis:24596 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (1.46 km, 1 s); p1 O (1.59 km, 1 s) |
| Murray River | gnis:2463 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (170.69 km, 63 s); p1 O (172.69 km, 2 s); p2 O (227.68 km, 18 s) |
| Taylor Creek | gnis:24655 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (0.77 km, 1 s); p1 O (1.15 km, 1 s); p2 O (27.22 km, 51 s) |
| Telkwa River | gnis:24733 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (27.87 km, 15 s); p1 O (72.83 km, 25 s) |
| Optimist Creek | gnis:24844 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (0.73 km, 1 s); p1 CAY (1.9 km, 1 s) |
| Sergeant Creek | gnis:24969 | stream | 2 | 2 | p1 (1) | open | yes | p3 (open) |  | p1* O (2.33 km, 1 s); p3 O (12.94 km, 1 s) |
| Serpentine River | gnis:24978 | stream | 4 | 3 | p0 (1) | open | yes | p3 (closed all year) | b | p0* O (3.47 km, 1 s); p1 O (3.49 km, 1 s); p2 O (17.1 km, 3 s); p3 CAY (29.39 km, 3 s) |
| Rees Creek | gnis:24999 | stream | 2 | 2 | p0 (1) | closed all year | NO | p1 (closed all year) |  | p0* CAY (1.35 km, 4 s); p1 CAY (11.06 km, 2 s) |
| Liumchen Creek | gnis:2500 | stream | 3 | 3 | p0 (1) | closed all year | yes | p2 (closed all year) | a | p0* CAY (1 km, 1 s); p1 O (8.43 km, 1 s); p2 CAY (11.46 km, 1 s) |
| Warwick Creek | gnis:25205 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (0.84 km, 1 s); p1 CAY (0.87 km, 1 s) |
| Goldstream River | gnis:2521 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (closed all year) |  | p0* CAY (4.7 km, 1 s); p1 CAY (16.07 km, 5 s) |
| Sustut River | gnis:25386 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (32.6 km, 12 s); p1 CAY (123.45 km, 24 s) |
| Tatsatua Creek | gnis:25405 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (28.27 km, 12 s); p1 O (57.27 km, 6 s) |
| Tatshenshini River | gnis:25410 | stream | 2 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (102.48 km, 44 s); p2 O (179.87 km, 19 s) |
| Pass Creek | gnis:25438 | stream | 2 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (0.33 km, 1 s); p2 O (4.03 km, 3 s) |
| Shorts Creek | gnis:25455 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (3.62 km, 1 s); p1 O (35.7 km, 10 s) |
| Shuswap River | gnis:25478 | stream | 12 | 8 | p0 (1) | open | yes | p11 (open) | b | p0* O (5.29 km, 1 s); p1 CAY (24.79 km, 1 s); p2 O (63.63 km, 7 s); p3 O (63.73 km, 1 s); p4 O (93.62 km, 10 s); p5 O (133.08 km, 15 s); p6 O (135.16 km, 1 s); p7 O (136.5 km, 2 s); p8 O (168.64 km, 11 s); p9 O (220.07 km, 11 s); p10 CAY (222.68 km, 3 s); p11 O (247.84 km, 2 s) |
| McKenzie Creek | gnis:25582 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (1.83 km, 2 s); p1 O (3.02 km, 1 s) |
| McKinley Creek | gnis:25591 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (8.09 km, 3 s); p1 O (53.46 km, 6 s) |
| Sproat River | gnis:25640 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (1.87 km, 1 s); p1 O (2.64 km, 1 s) |
| Spuzzum Creek | gnis:25658 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (0.18 km, 1 s); p1 O (0.26 km, 1 s); p2 O (30.41 km, 2 s) |
| Lussier River | gnis:2567 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (11.4 km, 1 s); p1 O (43.3 km, 3 s); p2 O (81.42 km, 4 s) |
| Squamish River | gnis:25671 | stream | 3 | 2 | p1 (1) | closed all year | NO | p3 (open) | a | p1* CAY (11.59 km, 2 s); p2 O (14.18 km, 24 s); p3 O (91.61 km, 96 s) |
| Somass River | gnis:25707 | stream | 5 | 2 | p1 (1) | open | yes | p5 (open) |  | p1* O (6.83 km, 1 s); p2 O (7.14 km, 1 s); p3 O (7.8 km, 1 s); p4 O (9.2 km, 1 s); p5 O (9.51 km, 1 s) |
| Sooke River | gnis:25725 | stream | 4 | 4 | p1 (1) | open | yes | p4 (open) | b | p1* O (6.95 km, 1 s); p2 CAY (7.15 km, 1 s); p3 O (8.65 km, 1 s); p4 O (19.91 km, 2 s) |
| Sootheran Creek | gnis:25729 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (0.03 km, 1 s); p1 O (3.91 km, 1 s); p2 O (3.97 km, 1 s) |
| Riske Creek | gnis:25822 | stream | 5 | 5 | p0 (1) | open | yes | p4 (open) |  | p0* O (23.05 km, 3 s); p1 O (36.11 km, 2 s); p2 O (37.23 km, 4 s); p3 O (39.27 km, 12 s); p4 O (46.01 km, 7 s) |
| Robertson Creek | gnis:25870 | stream | 2 | 2 | p0 (1) | open | yes | p0 (open) |  | p0* O (5.14 km, 2 s); p1 O (- km, 1 s) |
| Tod Creek | gnis:25942 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (2.68 km, 1 s); p1 O (3.48 km, 3 s); p2 O (10.62 km, 8 s) |
| Weaver Creek | gnis:25951 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (2.66 km, 1 s); p1 CAY (5.05 km, 1 s) |
| West Leech River | gnis:26081 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (11.02 km, 1 s); p1 O (11.19 km, 1 s) |
| West Road (Blackwater) River | gnis:26104 | stream | 6 | 6 | p0 (1) | closed today | yes | p5 (closed today) |  | p0* CT (0.01 km, 3 s); p1 CT (30.76 km, 3 s); p2 CT (209.82 km, 72 s); p3 CT (234.41 km, 2 s); p4 CT (248.76 km, 2 s); p5 CT (276.88 km, 7 s) |
| Sumas River | gnis:26216 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (5.58 km, 1 s); p1 O (32.73 km, 4 s) |
| Salmon River | gnis:2628 | stream | 4 | 4 | p1 (1) | open | yes | p4 (closed all year) | b | p1* O (11.67 km, 2 s); p2 O (36.66 km, 6 s); p3 CAY (61.33 km, 6 s); p4 CAY (87.94 km, 2 s) |
| Silverhope Creek | gnis:26290 | stream | 3 | 3 | p0 (1) | open | yes | p2 (closed today) | b | p0* O (7.63 km, 2 s); p1 CAY (9.16 km, 2 s); p2 CT (44.33 km, 18 s) |
| Similkameen River | gnis:26324 | stream | 3 | 3 | p1 (1) | open | yes | p3 (open) |  | p1* O (199.38 km, 48 s); p2 O (200.99 km, 1 s); p3 O (243 km, 11 s) |
| Sinclair Creek | gnis:26343 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (3.56 km, 2 s); p1 CAY (20.17 km, 1 s) |
| McLeod River | gnis:26408 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (28.69 km, 5 s); p1 O (40.65 km, 4 s); p2 O (81.58 km, 23 s) |
| McNeil Creek | gnis:26434 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (3.72 km, 2 s); p1 O (3.97 km, 1 s); p2 O (5.11 km, 1 s) |
| South Salmo River | gnis:26508 | stream | 2 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (16.01 km, 2 s); p2 O (30.13 km, 1 s) |
| Spanish Creek | gnis:26553 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (19.18 km, 2 s); p1 O (37.5 km, 4 s); p2 O (38.55 km, 1 s) |
| Shale Creek | gnis:2676 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (9.14 km, 1 s); p1 CAY (10.39 km, 1 s) |
| Duck Creek | gnis:2686 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (5.6 km, 1 s); p1 O (17.69 km, 1 s) |
| White River | gnis:26897 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (50.33 km, 6 s); p1 O (99.6 km, 11 s) |
| Paput Creek | gnis:2700 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (0.12 km, 1 s); p1 O (3.42 km, 1 s); p2 O (4.63 km, 1 s) |
| Ptarmigan Creek | gnis:27021 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (4.17 km, 1 s); p1 CAY (4.91 km, 1 s); p2 O (44.28 km, 3 s) |
| Puntledge River | gnis:27060 | stream | 9 | 8 | p0 (1) | open | yes | p8 (open) | b | p0* O (2.21 km, 1 s); p1 CAY (2.41 km, 1 s); p2 O (3.93 km, 1 s); p3 CAY (4 km, 1 s); p4 CAY (4.48 km, 1 s); p5 O (7.01 km, 1 s); p6 CAY (11.09 km, 2 s); p7 O (37.83 km, 4 s); p8 O (51.01 km, 3 s) |
| Skagit River | gnis:27159 | stream | 3 | 3 | p0 (1) | closed today | yes | p2 (closed today) |  | p0* CT (223.11 km, 7 s); p1 CAY (225.71 km, 1 s); p2 CT (255.29 km, 3 s) |
| Skaist River | gnis:27164 | stream | 2 | 2 | p0 (1) | closed today | yes | p1 (closed today) |  | p0* CT (6.77 km, 2 s); p1 CT (21.81 km, 5 s) |
| Skookumchuck Creek | gnis:27202 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (29.58 km, 2 s); p1 O (36.37 km, 1 s); p2 O (64.97 km, 2 s) |
| McRae Creek | gnis:27258 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (2.33 km, 1 s); p1 O (30.27 km, 3 s) |
| Swift River | gnis:2728 | stream | 2 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (122.55 km, 30 s); p2 O (123.02 km, 2 s) |
| Meager Creek | gnis:27287 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (6.07 km, 2 s); p1 O (8.64 km, 1 s); p2 O (32.92 km, 2 s) |
| Meldrum Creek | gnis:27317 | stream | 7 | 7 | p0 (1) | open | yes | p6 (open) |  | p0* O (11.94 km, 1 s); p1 O (18.19 km, 16 s); p2 O (33.58 km, 25 s); p3 O (34.95 km, 7 s); p4 O (35.51 km, 1 s); p5 O (64.35 km, 37 s); p6 O (65.18 km, 1 s) |
| Smith Falls Creek | gnis:27351 | stream | 4 | 4 | p0 (1) | closed all year | yes | p3 (closed all year) |  | p0* CAY (2.29 km, 1 s); p1 CAY (2.61 km, 1 s); p2 CAY (2.67 km, 1 s); p3 CAY (4.02 km, 5 s) |
| Snehumption Creek | gnis:27390 | stream | 2 | 2 | p1 (1) | open | NO | p2 (open) |  | p1* O (- km, 3 s); p2 O (18.58 km, 6 s) |
| Snow Creek | gnis:27411 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (7.67 km, 2 s); p1 O (25.2 km, 3 s) |
| Round Creek | gnis:27514 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (7.16 km, 1 s); p1 O (11.92 km, 1 s) |
| Ruby Creek | gnis:27550 | stream | 3 | 3 | p0 (1) | open | yes | p2 (closed all year) | b | p0* O (10.49 km, 2 s); p1 CAY (10.59 km, 1 s); p2 CAY (15.09 km, 1 s) |
| Trematon Creek | gnis:27638 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (0.84 km, 1 s); p1 CAY (0.92 km, 1 s); p2 O (2.42 km, 2 s) |
| Trépanier Creek | gnis:27648 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (5.38 km, 1 s); p1 O (32.58 km, 5 s) |
| Wiggs Creek | gnis:27700 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (0.03 km, 1 s); p1 CAY (1.3 km, 1 s); p2 O (1.55 km, 1 s) |
| Wigwam River | gnis:27703 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (26.35 km, 2 s); p1 O (56.45 km, 3 s) |
| Tamihi Creek | gnis:2773 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.66 km, 1 s); p1 O (13.79 km, 1 s) |
| Qualcho Creek | gnis:27799 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (40.35 km, 10 s); p1 O (41.54 km, 1 s); p2 O (48.04 km, 3 s) |
| Qualicum River | gnis:27804 | stream | 3 | 3 | p1 (1) | closed all year | yes | p3 (open) | a | p1* CAY (1.18 km, 1 s); p2 O (1.37 km, 1 s); p3 O (31.89 km, 3 s) |
| Quesnel River | gnis:27867 | stream | 8 | 6 | p0 (1) | open | yes | p7 (open) | b | p0* O (0.83 km, 1 s); p1 O (4.33 km, 1 s); p2 O (85.69 km, 9 s); p3 O (96.65 km, 2 s); p4 O (107.08 km, 1 s); p5 CAY (107.18 km, 1 s); p6 O (108.93 km, 1 s); p7 O (109.08 km, 1 s) |
| Quinsam River | gnis:27883 | stream | 4 | 4 | p0 (1) | open | yes | p3 (open) | b | p0* O (3.31 km, 2 s); p1 CAY (3.77 km, 1 s); p2 O (41.49 km, 5 s); p3 O (60.89 km, 7 s) |
| Slack Creek | gnis:27909 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (2.31 km, 1 s); p1 O (8.47 km, 1 s) |
| Adam River | gnis:28136 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (5.27 km, 5 s); p1 O (19.27 km, 1 s); p2 O (60.7 km, 5 s) |
| Aldworth Creek | gnis:28151 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.8 km, 1 s); p1 O (6.12 km, 1 s) |
| Artlish River | gnis:28190 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed today) | b | p0* O (11.53 km, 5 s); p1 CT (28.23 km, 1 s) |
| Trisle Creek | gnis:28455 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.42 km, 1 s); p1 O (3.68 km, 1 s) |
| Trout Creek | gnis:28474 | stream | 4 | 4 | p0 (1) | closed all year | yes | p3 (open) | a | p0* CAY (4.47 km, 3 s); p1 O (10.73 km, 1 s); p2 CAY (12.46 km, 1 s); p3 O (81.47 km, 14 s) |
| Wilson Creek | gnis:28553 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (17.65 km, 1 s); p1 O (41.25 km, 10 s) |
| Ram Creek | gnis:28715 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (3.36 km, 1 s); p1 CAY (4.9 km, 1 s); p2 O (12.53 km, 5 s) |
| Violet Creek | gnis:28895 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (5.03 km, 1 s); p1 CAY (7.14 km, 1 s); p2 O (14.22 km, 1 s) |
| Michel Creek | gnis:28953 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (12.56 km, 2 s); p1 O (50.97 km, 5 s) |
| Michigan Creek | gnis:28961 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.64 km, 1 s); p1 O (6.32 km, 1 s) |
| Simpson River | gnis:2902 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (8.63 km, 1 s); p1 O (32.05 km, 4 s) |
| Millar Creek | gnis:29025 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.19 km, 1 s); p1 O (4.96 km, 1 s) |
| Bonanza River | gnis:29055 | stream | 2 | 2 | p0 (1) | closed today | yes | p1 (open) | a | p0* CT (0.22 km, 1 s); p1 O (30.89 km, 9 s) |
| Schmidt Creek | gnis:29131 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (0.01 km, 1 s); p1 CAY (0.3 km, 1 s); p2 O (10.42 km, 2 s) |
| Toquaht River | gnis:2915 | stream | 3 | 3 | p1 (1) | open | yes | p3 (closed today) | b | p1* O (5.68 km, 1 s); p2 O (7.3 km, 1 s); p3 CT (22.93 km, 4 s) |
| Whatshan River | gnis:2918 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (7.6 km, 2 s); p1 CAY (52.16 km, 6 s) |
| White River | gnis:2923 | stream | 4 | 4 | p0 (1) | open | yes | p3 (closed today) | b | p0* O (0.74 km, 1 s); p1 CAY (0.94 km, 1 s); p2 O (1.14 km, 1 s); p3 CT (51.61 km, 4 s) |
| Woodbury Creek | gnis:29329 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.77 km, 1 s); p1 O (23.07 km, 1 s) |
| Skeena River | gnis:2936 | stream | 21 | 13 | p1 (1) | open | yes | p21 (open) | b | p1* O (63.48 km, 44 s); p2 CAY (66.56 km, 6 s); p3 O (67.92 km, 2 s); p4 CAY (- km, 1 s); p5 O (108.51 km, 55 s); p6 O (115.77 km, 8 s); p7 O (123.96 km, 4 s); p8 O (131.65 km, 7 s); p9 O (132.16 km, 1 s); p10 O (139.97 km, 3 s); p11 O (157.26 km, 7 s); p12 O (208.31 km, 12 s); p13 O (219.91 km, 2 s); p14 O (227.04 km, 2 s); p15 O (229.5 km, 3 s); p16 O (284.31 km, 11 s); p17 O (284.78 km, 2 s); p18 O (285.64 km, 1 s); p19 O (335.4 km, 10 s); p20 O (337.74 km, 1 s); p21 O (625.72 km, 40 s) |
| Megin River | gnis:2939 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.15 km, 2 s); p1 O (37.42 km, 4 s) |
| Whitetail Creek | gnis:2959 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (6.76 km, 1 s); p1 O (12.77 km, 2 s) |
| Mission Creek | gnis:29608 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (23.48 km, 2 s); p1 O (74.2 km, 7 s) |
| Vance Creek | gnis:29621 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (3.47 km, 2 s); p1 CAY (4.11 km, 1 s); p2 O (18.66 km, 1 s) |
| Verdant Creek | gnis:29687 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (6.19 km, 1 s); p1 CAY (22.1 km, 1 s) |
| Ulgako Creek | gnis:29716 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (0.01 km, 1 s); p1 O (10.04 km, 3 s); p2 O (16.02 km, 2 s) |
| Slesse Creek | gnis:2978 | stream | 3 | 3 | p0 (1) | closed all year | yes | p2 (closed all year) |  | p0* CAY (4.13 km, 3 s); p1 CAY (8.68 km, 2 s); p2 CAY (12.84 km, 1 s) |
| Tahsish River | gnis:29803 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.99 km, 1 s); p1 O (32.33 km, 3 s) |
| Tupper River | gnis:29883 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (33.3 km, 5 s); p1 CAY (33.43 km, 1 s) |
| Tusulko River | gnis:29926 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (22.81 km, 13 s); p1 O (34.22 km, 31 s) |
| Mitchell River | gnis:29965 | stream | 4 | 4 | p0 (1) | closed today | yes | p3 (open) | a | p0* CT (42.09 km, 2 s); p1 CAY (50.92 km, 3 s); p2 CAY (50.98 km, 1 s); p3 O (81.47 km, 4 s) |
| Wynton Creek | gnis:29996 | stream | 2 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (1.61 km, 1 s); p2 O (6.86 km, 3 s) |
| Yahk River | gnis:30003 | stream | 3 | 3 | p1 (1) | open | yes | p3 (open) | b | p1* O (8.02 km, 1 s); p2 CAY (8.96 km, 1 s); p3 O (39.78 km, 10 s) |
| Willow River | gnis:3003 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (155.03 km, 54 s); p1 O (159.34 km, 1 s); p2 O (220.61 km, 4 s) |
| Clanninick Creek | gnis:30031 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (3.19 km, 1 s); p1 CAY (4.89 km, 1 s); p2 O (13.79 km, 2 s) |
| Cluxewe River | gnis:30049 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (9.86 km, 4 s); p1 O (28.98 km, 1 s) |
| Deadman River | gnis:30592 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (46.33 km, 3 s); p1 O (116.36 km, 32 s) |
| Vedder River | gnis:3062 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (12.72 km, 4 s); p1 O (12.95 km, 1 s) |
| Keogh River | gnis:30697 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.35 km, 1 s); p1 O (33.26 km, 6 s) |
| Kiyu Creek | gnis:30715 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.01 km, 1 s); p1 O (10.16 km, 1 s) |
| Klaskish River | gnis:30722 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.59 km, 1 s); p1 O (16.27 km, 3 s) |
| Kokish River | gnis:30726 | stream | 7 | 7 | p0 (1) | open | yes | p6 (open) | b | p0* O (1.35 km, 2 s); p1 CAY (1.85 km, 1 s); p2 O (2.08 km, 1 s); p3 CT (10.76 km, 1 s); p4 CAY (11.15 km, 1 s); p5 CT (11.59 km, 1 s); p6 O (27.95 km, 7 s) |
| Marble River | gnis:30821 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (7.73 km, 5 s); p1 O (20.37 km, 3 s); p2 O (44.19 km, 7 s) |
| Eve River | gnis:31112 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (12.42 km, 2 s); p1 O (32.2 km, 22 s) |
| Zymoetz River | gnis:3137 | stream | 7 | 6 | p0 (1) | open | yes | p6 (open) | b | p0* O (0.59 km, 1 s); p1 O (18.32 km, 11 s); p2 CAY (19.4 km, 2 s); p3 O (46.92 km, 45 s); p4 O (110.1 km, 24 s); p5 CAY (113.1 km, 4 s); p6 O (143.46 km, 19 s) |
| Chuckwalla River | gnis:31700 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (13.33 km, 16 s); p1 CAY (13.63 km, 2 s); p2 O (46.88 km, 13 s) |
| Wahpeeto Creek | gnis:31813 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (4.04 km, 1 s); p1 CAY (4.14 km, 1 s); p2 O (19.15 km, 4 s) |
| Nass River | gnis:3206 | stream | 5 | 3 | p1 (1) | open | yes | p5 (open) |  | p1* O (66.23 km, 30 s); p2 O (78.04 km, 8 s); p3 O (189.24 km, 16 s); p4 O (190.44 km, 1 s); p5 O (406.39 km, 27 s) |
| Okanagan River | gnis:32069 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (145.47 km, 1 s); p1 O (167.72 km, 3 s); p2 O (197.7 km, 3 s) |
| Quatse River | gnis:32101 | stream | 4 | 2 | p0 (1) | open | yes | p3 (open) |  | p0* O (1.33 km, 1 s); p1 O (1.53 km, 1 s); p2 O (4.56 km, 1 s); p3 O (23.19 km, 2 s) |
| Thorne Creek | gnis:32114 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.5 km, 1 s); p1 O (34.28 km, 2 s) |
| Kitimat River | gnis:3225 | stream | 4 | 4 | p0 (1) | open | yes | p3 (open) |  | p0* O (4.3 km, 4 s); p1 O (5.05 km, 1 s); p2 O (38.18 km, 24 s); p3 O (98.21 km, 28 s) |
| Donald Creek | gnis:32460 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (14.47 km, 13 s); p1 O (20.2 km, 6 s) |
| Jim Creek | gnis:32586 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (55.14 km, 11 s); p1 O (73.66 km, 14 s) |
| Dorothy Creek | gnis:32619 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (19.3 km, 5 s); p1 O (19.51 km, 1 s) |
| Drizzle Creek | gnis:32639 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (6.47 km, 1 s); p1 CAY (11.41 km, 2 s); p2 O (11.55 km, 1 s) |
| Klinaklini River | gnis:3273 | stream | 3 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (47.39 km, 32 s); p1 O (173.16 km, 3 s); p2 O (255.39 km, 67 s) |
| Big Bar Creek | gnis:32961 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (39.22 km, 40 s); p1 O (55.21 km, 27 s) |
| Big Creek | gnis:33206 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (2.03 km, 4 s); p1 CAY (4.33 km, 2 s); p2 O (142.05 km, 26 s) |
| Graham Creek | gnis:33486 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (0.3 km, 1 s); p1 O (18.06 km, 22 s) |
| Canimred Creek | gnis:33683 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (19.5 km, 2 s); p1 O (19.83 km, 1 s); p2 O (37.93 km, 8 s) |
| Copper Creek | gnis:33991 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (7.68 km, 1 s); p1 O (13.09 km, 1 s); p2 O (33.32 km, 2 s) |
| Mamin River | gnis:34252 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (16.71 km, 6 s); p1 O (32.38 km, 2 s) |
| Lee Creek | gnis:34559 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (1.65 km, 1 s); p1 CAY (3.21 km, 1 s) |
| Spring Creek | gnis:34740 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (0.92 km, 1 s); p1 O (6.76 km, 3 s) |
| Yakoun River | gnis:3485 | stream | 3 | 2 | p0 (1) | open | yes | p2 (closed today) | b | p0* O (6.6 km, 2 s); p1 O (46.04 km, 4 s); p2 CT (61 km, 2 s) |
| Rayfield River | gnis:34877 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (25.44 km, 14 s); p1 O (46.04 km, 39 s); p2 O (50.36 km, 4 s) |
| Exstew River | gnis:3498 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (0.41 km, 1 s); p1 O (52.06 km, 22 s) |
| Exchamsiks River | gnis:3499 | stream | 3 | 2 | p0 (1) | closed all year | yes | p2 (open) | a | p0* CAY (0.04 km, 1 s); p1 O (0.23 km, 1 s); p2 O (57.97 km, 43 s) |
| Copper Creek | gnis:3527 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (29.1 km, 2 s); p1 O (29.75 km, 1 s); p2 O (30.27 km, 1 s) |
| Valenzuela Creek | gnis:35270 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (1.81 km, 1 s); p1 O (13.21 km, 12 s) |
| Tin Cup Creek | gnis:35480 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (1.43 km, 1 s); p1 O (12.76 km, 21 s); p2 O (18.29 km, 5 s) |
| Windy Creek | gnis:35563 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (13.62 km, 13 s); p1 O (20.7 km, 3 s) |
| Longfellow Creek | gnis:35968 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (1.79 km, 1 s); p1 CAY (1.85 km, 1 s) |
| Halfway River | gnis:36111 | stream | 3 | 3 | p0 (1) | open | yes | p1 (open) |  | p0* O (5 km, 3 s); p1 O (291.2 km, 300 s); p2 O (- km, 12 s) |
| Crooked River | gnis:36188 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (58.01 km, 9 s); p1 O (87.2 km, 13 s) |
| Cross River | gnis:36203 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.05 km, 1 s); p1 O (40.08 km, 11 s) |
| Cruickshank River | gnis:36224 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (closed all year) |  | p0* CAY (2.5 km, 1 s); p1 CAY (26.51 km, 2 s) |
| Haysport Creek | gnis:36570 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (3.86 km, 1 s); p1 CAY (4.3 km, 1 s) |
| Gingietl Creek | gnis:36750 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (0.1 km, 1 s); p1 CAY (7.52 km, 1 s) |
| Sleigh Creek | gnis:36937 | stream | 7 | 7 | p0 (1) | open | yes | p12 (open) |  | p0* O (2.74 km, 1 s); p2 O (3.41 km, 1 s); p4 O (3.58 km, 1 s); p6 O (3.86 km, 1 s); p8 O (4.36 km, 1 s); p10 O (14.84 km, 1 s); p12 O (46.59 km, 23 s) |
| Columbia River | gnis:37414 | stream | 8 | 8 | p1 (1) | open | yes | p8 (open) | b | p1* O (1223.72 km, 6 s); p2 O (1224.67 km, 1 s); p3 O (1232.08 km, 1 s); p4 O (1460.38 km, 2 s); p5 CAY (1468.51 km, 1 s); p6 O (1740.06 km, 1 s); p7 O (1933.35 km, 124 s); p8 O (1936.48 km, 2 s) |
| Comox Creek | gnis:37437 | stream | 2 | 2 | p0 (1) | closed all year | NO | p1 (closed all year) |  | p0* CAY (11.97 km, 1 s); p1 CAY (14.45 km, 5 s) |
| Will Creek | gnis:37496 | stream | 4 | 4 | p0 (1) | open | yes | p3 (closed all year) | b | p0* O (17.39 km, 9 s); p1 CAY (17.57 km, 1 s); p2 O (17.63 km, 1 s); p3 CAY (19.4 km, 5 s) |
| Russell Creek | gnis:37607 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (0.02 km, 2 s); p1 CAY (1.02 km, 3 s); p2 O (9.37 km, 1 s) |
| Price Creek | gnis:37711 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (4.07 km, 13 s); p1 O (9.38 km, 23 s) |
| Cayoosh Creek | gnis:37767 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (1.77 km, 3 s); p1 O (70 km, 20 s) |
| Bridge Creek | gnis:37782 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (2.25 km, 1 s); p1 CAY (5.95 km, 1 s) |
| Whitefoot Creek | gnis:37908 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (9.47 km, 1 s); p1 CAY (10.72 km, 1 s) |
| Cariboo River | gnis:37911 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (68.71 km, 22 s); p1 O (145.65 km, 12 s); p2 O (163.1 km, 6 s) |
| Burton (Trout) Creek | gnis:37939 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (8.07 km, 1 s); p1 O (24.07 km, 1 s) |
| Zymagotitz River | gnis:3812 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (0.78 km, 1 s); p1 O (43.91 km, 24 s) |
| Ksi X'anmas | gnis:3815 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (6.04 km, 6 s); p1 O (52.86 km, 22 s) |
| Williams Creek | gnis:3820 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (24.6 km, 1 s); p1 CAY (29.13 km, 1 s); p2 O (39.65 km, 2 s) |
| Ksi Sii Aks | gnis:3828 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed today) | b | p0* O (1.58 km, 1 s); p1 CT (45.7 km, 39 s) |
| Bonaparte River | gnis:38393 | stream | 3 | 3 | p0 (1) | closed all year | yes | p2 (open) | a | p0* CAY (3.41 km, 1 s); p1 O (161.44 km, 17 s); p2 O (194.39 km, 18 s) |
| MacMillan Creek | gnis:38420 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (9.82 km, 1 s); p1 O (18.18 km, 4 s) |
| Eugene Creek | gnis:38516 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (1.38 km, 1 s); p1 O (9.34 km, 10 s) |
| Airplane Creek | gnis:38698 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (4.87 km, 1 s); p1 CAY (7.53 km, 1 s) |
| Pouce Coupé River | gnis:38821 | stream | 3 | 3 | p1 (1) | open | yes | p4 (open) | b | p1* O (51.07 km, 1 s); p3 CAY (52.7 km, 1 s); p4 O (104.23 km, 4 s) |
| MacMillan Creek | gnis:38913 | stream | 2 | 2 | p1 (1) | open | yes | p1 (open) |  | p1* O (8.13 km, 14 s); p2 O (- km, 1 s) |
| Adams River | gnis:39257 | stream | 4 | 4 | p0 (1) | open | yes | p3 (open) |  | p0* O (2.96 km, 2 s); p1 O (3.36 km, 2 s); p2 O (12.46 km, 3 s); p3 O (200.93 km, 21 s) |
| Chemainus River | gnis:39267 | stream | 4 | 4 | p1 (1) | open | yes | p4 (open) | b | p1* O (10.63 km, 2 s); p2 O (12.82 km, 1 s); p3 CAY (12.92 km, 1 s); p4 O (61.95 km, 4 s) |
| Chimney Creek | gnis:39269 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (20.05 km, 15 s); p1 O (57.45 km, 21 s) |
| Evjen Creek | gnis:39314 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (0.99 km, 3 s); p1 O (6.98 km, 1 s) |
| Fraser River | gnis:39325 | stream | 23 | 19 | p1 (1) | open | yes | p23 (open) | b | p1* O (156.38 km, 34 s); p2 O (156.52 km, 1 s); p3 O (158.75 km, 2 s); p4 O (196.97 km, 4 s); p5 O (201.24 km, 1 s); p6 O (209.11 km, 1 s); p7 CAY (211.91 km, 1 s); p8 CAY (216.94 km, 1 s); p9 O (266.72 km, 4 s); p10 CT (267.99 km, 1 s); p11 O (325.51 km, 7 s); p12 O (327.88 km, 2 s); p13 CT (329.13 km, 1 s); p14 CT (329.38 km, 1 s); p15 O (333.6 km, 1 s); p16 O (338.42 km, 1 s); p17 O (418.4 km, 7 s); p18 O (452.7 km, 2 s); p19 O (481.91 km, 3 s); p20 O (538.44 km, 3 s); p21 O (674.47 km, 37 s); p22 O (703.75 km, 2 s); p23 O (1407.21 km, 113 s) |
| Kicking Horse River | gnis:39363 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (30.33 km, 7 s); p1 CAY (82.75 km, 15 s) |
| Nicola River | gnis:39400 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (100.56 km, 36 s); p1 O (214.85 km, 18 s) |
| North Alouette River | gnis:39404 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (4.69 km, 3 s); p1 O (18.7 km, 2 s) |
| Kitsumkalum River | gnis:3942 | stream | 5 | 3 | p0 (1) | open | yes | p4 (open) |  | p0* O (0.02 km, 1 s); p1 O (13.08 km, 20 s); p2 O (27.2 km, 3 s); p3 O (36.53 km, 5 s); p4 O (110.36 km, 90 s) |
| Robson Creek | gnis:39445 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.27 km, 1 s); p1 O (4.05 km, 1 s) |
| Salmon River | gnis:39454 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (67.53 km, 7 s); p1 O (146.87 km, 6 s) |
| Thompson River | gnis:39492 | stream | 12 | 5 | p0 (1) | closed today | yes | p11 (open) | a | p0* CT (7.14 km, 1 s); p1 CT (26.05 km, 1 s); p2 CT (46.32 km, 7 s); p3 CT (78.34 km, 9 s); p4 CT (80.68 km, 1 s); p5 CT (81.55 km, 1 s); p6 CT (93.02 km, 1 s); p7 CT (97.02 km, 1 s); p8 CT (112.99 km, 1 s); p9 CT (116.61 km, 1 s); p10 CT (118.11 km, 2 s); p11 O (159.41 km, 5 s) |
| Campbell River | gnis:39532 | stream | 11 | 10 | p1 (1) | open | yes | p11 (open) | b | p1* O (1.92 km, 1 s); p2 O (4.2 km, 2 s); p3 O (5.54 km, 1 s); p4 O (7.48 km, 1 s); p5 CAY (8.5 km, 1 s); p6 O (16.85 km, 1 s); p7 O (17.86 km, 1 s); p8 O (35.78 km, 1 s); p9 CAY (35.88 km, 1 s); p10 O (36.03 km, 1 s); p11 O (93.27 km, 2 s) |
| Ecstall River | gnis:3986 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (57.3 km, 13 s); p1 O (112.53 km, 31 s) |
| Khyex River | gnis:4076 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (1.52 km, 1 s); p1 O (51.02 km, 39 s) |
| Kasiks River | gnis:4078 | stream | 3 | 1 | p0 (1) | open | yes | p2 (open) |  | p0* O (0.19 km, 1 s); p1 O (17.65 km, 13 s); p2 O (41.39 km, 16 s) |
| Hays Creek | gnis:4152 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (0.14 km, 1 s); p1 CAY (4.49 km, 1 s) |
| Hegstrom Creek | gnis:43126 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (2.02 km, 1 s); p1 O (2.42 km, 1 s) |
| Bridge River | gnis:43978 | stream | 2 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (8.13 km, 1 s); p2 O (26.64 km, 1 s) |
| Charles Lee Creek | gnis:44227 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (3.86 km, 1 s); p1 O (4.1 km, 2 s); p2 O (9.87 km, 2 s) |
| Kalide Creek | gnis:44295 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.02 km, 1 s); p1 O (2.74 km, 1 s) |
| Cranberry River | gnis:4674 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (15.28 km, 7 s); p1 CAY (15.75 km, 2 s); p2 O (112.9 km, 13 s) |
| Lakelse River | gnis:4903 | stream | 4 | 3 | p0 (1) | open | yes | p3 (open) |  | p0* O (2 km, 1 s); p1 O (3.75 km, 1 s); p2 O (16.65 km, 2 s); p3 O (21.44 km, 1 s) |
| Chowade River | gnis:4944 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (20.88 km, 36 s); p1 O (85.73 km, 60 s) |
| Bridge River | gnis:4945 | stream | 5 | 5 | p0 (1) | open | yes | p4 (open) | b | p0* O (0.49 km, 1 s); p1 O (25.13 km, 1 s); p2 CAY (40.22 km, 6 s); p3 O (94.82 km, 4 s); p4 O (166.29 km, 22 s) |
| Peachland Creek | gnis:5012 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (2.29 km, 1 s); p1 O (35.2 km, 4 s) |
| Mohun Creek | gnis:5316 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (7.42 km, 1 s); p1 CAY (13.25 km, 12 s); p2 O (25.49 km, 6 s) |
| Woolsey Creek | gnis:5371 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (0.02 km, 1 s); p1 CAY (10.05 km, 1 s); p2 O (18.67 km, 1 s) |
| Tsitika River | gnis:5379 | stream | 5 | 5 | p0 (1) | closed all year | yes | p4 (closed today) | a | p0* CAY (2.49 km, 2 s); p1 O (7.27 km, 3 s); p2 CT (27.12 km, 2 s); p3 CAY (28.51 km, 1 s); p4 CT (45.96 km, 6 s) |
| Amor De Cosmos Creek | gnis:5480 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (2.67 km, 1 s); p1 CAY (3.5 km, 1 s); p2 O (33.6 km, 9 s) |
| Capilano River | gnis:5922 | stream | 3 | 3 | p1 (1) | open | yes | p3 (closed all year) | b | p1* O (5.5 km, 1 s); p2 CAY (6.08 km, 1 s); p3 CAY (34.8 km, 9 s) |
| Seymour River | gnis:6105 | stream | 3 | 3 | p1 (1) | closed all year | yes | p2 (open) | a | p1* CAY (19.05 km, 4 s); p2 O (41.12 km, 10 s); p3 O (- km, 1 s) |
| Old Wolf Creek | gnis:6277 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (0.09 km, 1 s); p1 O (0.12 km, 1 s); p2 O (6.15 km, 2 s) |
| Seton River | gnis:6291 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (4.35 km, 5 s); p1 O (30.21 km, 2 s) |
| Wolverine River | gnis:6547 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (3.12 km, 2 s); p1 O (8.51 km, 1 s); p2 O (17.44 km, 1 s) |
| Deena Creek | gnis:6912 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (8.83 km, 3 s); p1 O (17.88 km, 2 s) |
| Halfway River | gnis:7059 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (12.42 km, 1 s); p1 O (43.35 km, 9 s) |
| Scuzzy Creek | gnis:7238 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (6.08 km, 2 s); p1 O (6.35 km, 1 s); p2 O (29.07 km, 7 s) |
| Campbell River | gnis:7250 | stream | 4 | 4 | p1 (1) | open | yes | p4 (closed all year) | b | p1* O (0.76 km, 3 s); p2 O (0.96 km, 1 s); p3 O (7.48 km, 1 s); p4 CAY (29.39 km, 3 s) |
| Adskwatim Creek | gnis:7384 | stream | 2 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (42.05 km, 22 s); p2 O (53.13 km, 25 s) |
| Little Hay River | gnis:7388 | stream | 2 | 2 | p1 (1) | open | yes | p3 (open) |  | p1* O (76.62 km, 1 s); p3 O (101.28 km, 57 s) |
| Snowfall Creek | gnis:7441 | stream | 2 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (48.91 km, 37 s); p2 O (49.07 km, 1 s) |
| South Albert Creek | gnis:7473 | stream | 2 | 2 | p1 (1) | open | yes | p3 (open) |  | p1* O (7.63 km, 1 s); p3 O (48.47 km, 16 s) |
| Durrance Creek | gnis:7504 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (0.08 km, 1 s); p1 O (1.76 km, 3 s); p2 O (3.01 km, 2 s) |
| 150 Yard Creek | gnis:7513 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (2.38 km, 1 s); p1 O (2.58 km, 1 s) |
| Orange Juice Creek | gnis:7514 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.86 km, 1 s); p1 O (3.97 km, 1 s) |
| Tsocowis Creek | gnis:7515 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.33 km, 1 s); p1 O (4.69 km, 1 s) |
| Trestle Creek | gnis:7516 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.24 km, 1 s); p1 O (1.73 km, 1 s) |
| Adrenaline Creek | gnis:7523 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.82 km, 1 s); p1 O (3.7 km, 1 s) |
| Billy Goat Creek | gnis:7524 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.71 km, 1 s); p1 O (3.75 km, 3 s) |
| Coal Creek | gnis:7525 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.17 km, 1 s); p1 O (6.52 km, 1 s) |
| Cribs Creek | gnis:7526 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.93 km, 1 s); p1 O (5 km, 1 s) |
| Pitt River | gnis:7551 | stream | 3 | 3 | p1 (1) | open | yes | p3 (closed all year) | b | p1* O (20.78 km, 5 s); p2 O (76 km, 26 s); p3 CAY (105.9 km, 21 s) |
| Illecillewaet River | gnis:7586 | stream | 3 | 3 | p0 (1) | closed all year | yes | p2 (closed all year) | a | p0* CAY (38.48 km, 10 s); p1 O (52.95 km, 1 s); p2 CAY (77.07 km, 1 s) |
| Anzac River | gnis:7607 | stream | 4 | 4 | p0 (1) | open | yes | p3 (open) | b | p0* O (45.87 km, 13 s); p1 O (49.68 km, 1 s); p2 CT (50.68 km, 3 s); p3 O (82.46 km, 4 s) |
| Beaver River | gnis:7636 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (10.24 km, 3 s); p1 CAY (70.16 km, 8 s); p2 O (74.17 km, 4 s) |
| Coursier Creek | gnis:7678 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (1.25 km, 1 s); p1 CAY (7.86 km, 1 s) |
| Manson River | gnis:7712 | stream | 2 | 2 | p0 (1) | open | NO | p1 (open) |  | p0* O (85.98 km, 3 s); p1 O (120.68 km, 38 s) |
| Gagnon Creek | gnis:7733 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (3.86 km, 2 s); p1 O (34.76 km, 3 s) |
| Klanawa River | gnis:7741 | stream | 4 | 4 | p0 (1) | closed all year | yes | p3 (open) | a | p0* CAY (1.62 km, 1 s); p1 O (5.31 km, 1 s); p2 CAY (7 km, 1 s); p3 O (33.01 km, 3 s) |
| Chichouyenily Creek | gnis:7760 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (10.27 km, 1 s); p1 O (35.63 km, 3 s) |
| Stellako River | gnis:7836 | stream | 5 | 5 | p0 (1) | closed today | yes | p4 (closed today) |  | p0* CT (2.12 km, 2 s); p1 CT (7.95 km, 3 s); p2 CT (10.92 km, 2 s); p3 CT (14.67 km, 1 s); p4 CT (14.92 km, 1 s) |
| Nathan Creek | gnis:7892 | stream | 2 | 2 | p1 (1) | open | yes | p2 (closed all year) | b | p1* O (4.28 km, 2 s); p2 CAY (15.38 km, 1 s) |
| Canoe Creek | gnis:7936 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (14.13 km, 5 s); p1 O (14.8 km, 3 s) |
| Goosegrass Creek | gnis:7953 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (1.82 km, 1 s); p1 CAY (7.06 km, 1 s) |
| Baker Creek | gnis:7956 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (7.09 km, 2 s); p1 O (121.78 km, 14 s) |
| Caribou Creek | gnis:7966 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (0.98 km, 1 s); p1 O (10.47 km, 1 s); p2 O (36.58 km, 2 s) |
| St. Cyr Creek | gnis:8035 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (1.98 km, 1 s); p1 CAY (9.14 km, 1 s) |
| Summit Creek | gnis:8041 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (24.36 km, 2 s); p1 O (39.18 km, 1 s) |
| Palmer Creek | gnis:8098 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (2.75 km, 1 s); p1 CAY (5.89 km, 1 s) |
| Shelagyote River | gnis:8138 | stream | 2 | 2 | p0 (1) | open | NO | p1 (open) |  | p0* O (48.58 km, 1 s); p1 O (59.28 km, 44 s) |
| Homathko River | gnis:8173 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (60.87 km, 67 s); p1 O (173 km, 34 s) |
| Ospika River | gnis:8216 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (104.16 km, 89 s); p1 CAY (109.91 km, 4 s); p2 O (142.96 km, 17 s) |
| Shrimpton Creek | gnis:8235 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (14.73 km, 5 s); p1 O (19.19 km, 1 s); p2 O (20.36 km, 3 s) |
| Dutch Creek | gnis:8242 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (34.82 km, 4 s); p1 O (69.5 km, 5 s) |
| Weston Creek | gnis:8263 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (7.47 km, 1 s); p1 O (32.5 km, 3 s) |
| Omineca River | gnis:8264 | stream | 2 | 2 | p0 (1) | open | NO | p1 (open) |  | p0* O (134.41 km, 1 s); p1 O (262.5 km, 51 s) |
| Wiedenman Creek | gnis:8324 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (0.74 km, 1 s); p1 CAY (2.45 km, 1 s) |
| Wood River | gnis:8372 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed today) | b | p0* O (32.02 km, 13 s); p1 CT (37.22 km, 7 s) |
| Hoodas Creek | gnis:8456 | stream | 5 | 5 | p0 (1) | open | yes | p4 (open) | b | p0* O (1.67 km, 1 s); p1 CAY (2.77 km, 1 s); p2 O (3.84 km, 1 s); p3 CAY (4.32 km, 1 s); p4 O (9.71 km, 1 s) |
| Chilliwack River | gnis:8634 | stream | 5 | 4 | p0 (1) | open | yes | p4 (closed all year) | b | p0* O (0.2 km, 1 s); p1 O (11.99 km, 9 s); p2 O (23.33 km, 3 s); p3 CAY (58.65 km, 7 s); p4 CAY (60.41 km, 1 s) |
| Chef Creek | gnis:8643 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) | b | p0* O (1.73 km, 1 s); p1 CAY (2.83 km, 1 s); p2 O (7.67 km, 1 s) |
| South Thompson River | gnis:8675 | stream | 2 | 1 | p0 (1) | open | yes | p1 (open) |  | p0* O (19.64 km, 1 s); p1 O (59.97 km, 4 s) |
| Bonilla Creek | gnis:8899 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (1.11 km, 1 s); p1 O (3.58 km, 1 s) |
| Bonnevier Creek | gnis:8907 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (0.08 km, 1 s); p1 O (2.71 km, 1 s); p2 O (6.67 km, 1 s) |
| Ahbau Creek | gnis:9019 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (0.03 km, 4 s); p1 O (49.61 km, 3 s); p2 O (90.66 km, 5 s) |
| Boundary Creek | gnis:9067 | stream | 2 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (3.29 km, 1 s); p2 O (37.54 km, 4 s) |
| Boundary Creek | gnis:9069 | stream | 2 | 2 | p0 (1) | open | yes | p2 (open) |  | p0* O (16.07 km, 19 s); p2 O (24.87 km, 1 s) |
| Bowman Creek | gnis:9099 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.25 km, 1 s); p1 O (6.8 km, 1 s) |
| Bowron River | gnis:9100 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (182.71 km, 80 s); p1 O (237.59 km, 17 s) |
| Boyce Creek | gnis:9173 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (open) | a | p0* CAY (0.07 km, 1 s); p1 O (7.38 km, 1 s) |
| Brem River | gnis:9238 | stream | 4 | 4 | p0 (1) | open | yes | p3 (closed today) | b | p0* O (1.69 km, 2 s); p1 CAY (1.79 km, 1 s); p2 O (10.93 km, 5 s); p3 CT (25.53 km, 2 s) |
| Alexander Creek | gnis:9343 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (4.68 km, 1 s); p1 O (36.6 km, 7 s) |
| Ice River | gnis:9448 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (0.04 km, 1 s); p1 CAY (24.17 km, 14 s) |
| Leech River | gnis:9483 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (4.07 km, 1 s); p1 O (20.5 km, 3 s); p2 O (21.81 km, 1 s) |
| Alf Creek | gnis:9557 | stream | 2 | 2 | p0 (1) | open | yes | p1 (open) |  | p0* O (0.21 km, 1 s); p1 O (3.87 km, 3 s) |
| Alouette River | gnis:9630 | stream | 3 | 3 | p0 (1) | open | yes | p2 (closed all year) | b | p0* O (9.04 km, 1 s); p1 O (18.2 km, 3 s); p2 CAY (49.93 km, 4 s) |
| Brothers Creek | gnis:9694 | stream | 3 | 3 | p0 (1) | closed all year | yes | p2 (closed all year) |  | p0* CAY (8.75 km, 1 s); p1 CAY (8.78 km, 1 s); p2 CAY (8.8 km, 2 s) |
| Browns River | gnis:9726 | stream | 2 | 2 | p0 (1) | closed all year | yes | p1 (closed all year) |  | p0* CAY (24.12 km, 2 s); p1 CAY (28.58 km, 2 s) |
| East Liumchen Creek | gnis:9766 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (0.95 km, 1 s); p1 CAY (4.54 km, 2 s) |
| Inchawka Creek | gnis:9810 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (2.51 km, 1 s); p1 CAY (3.27 km, 1 s) |
| Incomappleux River | gnis:9814 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (49.24 km, 11 s); p1 CAY (74.32 km, 6 s) |
| Indianpoint Creek | gnis:9833 | stream | 3 | 3 | p0 (1) | open | yes | p2 (open) |  | p0* O (38.67 km, 10 s); p1 O (42.41 km, 1 s); p2 O (44.34 km, 1 s) |
| Lew Creek | gnis:9871 | stream | 2 | 2 | p0 (1) | open | yes | p1 (closed all year) | b | p0* O (0.92 km, 1 s); p1 CAY (6.89 km, 3 s) |
| Liard River | gnis:9891 | stream | 5 | 5 | p0 (1) | open | yes | p4 (open) | b | p0* O (698.93 km, 188 s); p1 CAY (705.37 km, 1 s); p2 O (796.36 km, 17 s); p3 O (797.04 km, 1 s); p4 O (882.4 km, 77 s) |
| Okanagan River Oxbows | wsc:300-432687-459615 | stream | 2 | 2 | p0 (1) | open | yes | p0 (open) | b | p0* O (4.51 km, 80 s); p1 CAY (- km, 9 s) |
| Whiteswan Lake outlet stream | wsc:300-625474-814939-312002 | stream | 2 | 2 | p0 (1) | closed today | yes | p1 (closed all year) |  | p0* CT (0.6 km, 1 s); p1 CAY (9.73 km, 2 s) |
