/* regs-v6 data: copied verbatim from regs-mock.html (page v36 words, read 2026-10-10). */
const FISHDIR = '../apps/mobile/assets/fish/';
// app/apps/mobile/assets/fish/manifest.json (subset: what these waters show)
const PICS = {
  EB:[{f:'brook-trout.webp',c:'Brook trout',cr:'Freshwater Fisheries Society of BC',s:'GoFishBC'}],
  GB:[{f:'brown-trout.webp',c:'Brown trout',cr:'Duane Raver / USFWS',s:'fws.gov'}],
  CT:[{f:'coastal-cutthroat-trout.webp',c:'Coastal cutthroat trout',cr:'Freshwater Fisheries Society of BC',s:'GoFishBC'},
      {f:'westslope-cutthroat-trout.webp',c:'Westslope cutthroat trout',cr:'Freshwater Fisheries Society of BC',s:'GoFishBC'}],
  DV:[{f:'dolly-varden.webp',c:'Dolly Varden',cr:'WDFW',s:'eRegulations WA'},
      {f:'bull-trout.webp',c:'Bull trout',cr:'Joel Sartore / National Geographic',s:'Commons'}],
  LT:[{f:'lake-trout.webp',c:'Lake trout',cr:'Sherman F. Denton',s:'Commons'}],
  RB:[{f:'rainbow-trout.webp',c:'Rainbow trout',cr:'Freshwater Fisheries Society of BC',s:'GoFishBC'}],
  ST:[{f:'steelhead-ocean-phase.webp',c:'Steelhead',ph:'ocean phase',cr:'Freshwater Fisheries Society of BC',s:'GoFishBC'},
      {f:'steelhead-spawning-phase.webp',c:'Steelhead',ph:'spawning phase',cr:'WDFW',s:'eRegulations WA'}],
  KO:[{f:'kokanee.webp',c:'Kokanee',cr:'Freshwater Fisheries Society of BC',s:'GoFishBC'},
      {f:'kokanee-spawning-phase.webp',c:'Kokanee',ph:'spawning phase',cr:'WDFW (sockeye male)',s:'eRegulations WA'}],
  LW:[{f:'lake-whitefish.webp',c:'Lake whitefish',cr:'Sherman F. Denton',s:'Commons'}],
  MW:[{f:'mountain-whitefish.webp',c:'Mountain whitefish',cr:'What To Fish',s:'whattofish.ca'}],
  LMB:[{f:'largemouth-bass.webp',c:'Largemouth bass',cr:'Duane Raver / USFWS',s:'Commons'}],
  SMB:[{f:'smallmouth-bass.webp',c:'Smallmouth bass',cr:'Freshwater Fisheries Society of BC',s:'GoFishBC'}],
  BCB:[{f:'black-crappie.webp',c:'Black crappie',cr:'Duane Raver / USFWS',s:'fws.gov'}],
  YP:[{f:'yellow-perch.webp',c:'Yellow perch',cr:'Freshwater Fisheries Society of BC',s:'GoFishBC'}],
  BB:[{f:'burbot.webp',c:'Burbot',cr:'NY Forest, Fish and Game Commission',s:'Commons'}],
  NP:[{f:'northern-pike.webp',c:'Northern pike',cr:'Timothy Knepp / USFWS',s:'fws.gov'}],
  WP:[{f:'walleye.webp',c:'Walleye',cr:'Duane Raver / USFWS',s:'fws.gov'}],
  WSG:[{f:'white-sturgeon.webp',c:'White sturgeon',cr:'Joseph Huët (Paris Museum)',s:'Commons'}],
  GREEN_STURGEON:[{f:'green-sturgeon.webp',c:'Green sturgeon',cr:'Joel Sartore / National Geographic Photo Ark',s:'DFO sturgeon ID guide'}],
  CULTUS_LAKE_SCULPIN:[{f:'coastrange-sculpin-cultus-pygmy-sculpin-stand-in.webp',c:'Coastrange sculpin',si:'Cultus pygmy sculpin',cr:'Jordan & Evermann (1900)',s:'Commons'}],
  NOOKSACK_DACE:[{f:'longnose-dace-nooksack-dace-stand-in.webp',c:'Longnose dace',si:'Nooksack dace',cr:'Jordan & Evermann (1900)',s:'Commons'}],
  SALISH_SUCKER:[{f:'longnose-sucker-salish-sucker-stand-in.webp',c:'Longnose sucker',si:'Salish sucker',cr:'Jordan & Evermann (1900)',s:'Commons'}],
};
// page v36: FISHART.SPECIES[*].tips and spName()
const FISH = {
  RB:{n:'Rainbow trout',tips:['Pink or red band along the side','Small black spots, mostly above the middle of the side','Rows of black spots on the tail']},
  ST:{n:'Steelhead',tips:['A rainbow trout that went to sea: bright silver, 50 cm or longer','Faint spots, mostly on the back and tail','Hatchery steelhead are missing this small fin (the adipose fin)']},
  CT:{n:'Cutthroat trout',tips:['Red or orange slash under the lower jaw (may be faint)','Large mouth, reaching well past the eye','Black spots all over, more toward the tail']},
  GB:{n:'Brown trout',tips:['Black and red spots, many with pale halos','Tail with few or no spots','Orange-rimmed adipose fin']},
  KO:{n:'Kokanee',tips:['No black spots on the sides','Dark blue back, bright silver sides','Long fin under the tail end (13 or more rays)']},
  DV:{n:'Dolly Varden/bull trout',tips:['Pale pink or cream spots, smaller than the pupil','No worm-like markings on the back fin','White front edges on the lower fins','Bull trout: big, broad, flat head']},
  LT:{n:'Lake trout',tips:['Pale spots all over a dark body, no red','Worm-like light markings on the back and back fin','Deeply forked tail']},
  EB:{n:'Brook trout',tips:['Worm-like markings on the back and back fin','Red spots with blue halos','Orange lower fins with white front edges']},
  LW:{n:'Lake whitefish',tips:['Large scales, silver sides','Small mouth under a blunt snout','Has an adipose fin; no spots']},
  MW:{n:'Mountain whitefish',tips:['Large scales, silver sides','Small mouth under a blunt snout','Has an adipose fin; no spots']},
  WSG:{n:'White sturgeon',tips:['Rows of bony plates, no scales','Barbels under a pointed snout','Upper lobe of the tail much longer']},
  BB:{n:'Burbot',tips:['One barbel under the chin','Two back fins, the second very long','Long, eel-like mottled body']},
  NP:{n:'Northern pike',tips:['Long, flat, duck-bill snout','Back fin set far back, near the tail','Rows of pale bean-shaped spots']},
  WP:{n:'Walleye',tips:['Big, glassy eye','Spiny front back fin','White tip on the lower tail lobe','Sharp, fang-like teeth']},
  YP:{n:'Yellow perch',tips:['Six to nine dark bars down the sides','Orange lower fins','Spiny front back fin; no fang-like teeth']},
  BCB:{n:'Black crappie',tips:['Deep, flat body covered in black speckles','Large, rounded back and bottom fins','Mouth reaches the front of the eye']},
  SMB:{n:'Smallmouth bass',tips:['Bronze-brown with faint vertical bars','Jaw reaches about the middle of the eye','Red or orange eye']},
  LMB:{n:'Largemouth bass',tips:['Dark stripe along the side','Jaw reaches past the back of the eye','Front and back fins almost separate']},
  GREEN_STURGEON:{n:'Green sturgeon',tips:[]},
  CULTUS_LAKE_SCULPIN:{n:'Cultus Lake sculpin',tips:[]},
  NOOKSACK_DACE:{n:'Nooksack dace',tips:[]},
  SALISH_SUCKER:{n:'Salish sucker',tips:[]},
};

const POSS = ['Possession: twice the daily quota (fish at home don’t count).','Carry limit'];
const ANYWHERE = ['No fishing: within 23 m downstream of the lower entrance to any fishway, canal, obstacle or leap',
  'No fishing: within a 100 m radius of any government facility operated for counting, passing or rearing fish (e.g., fishway, fish hatchery) unless otherwise designated'];
const ALWAYS = {
  duty:'Any fish snagged — even by accident — must be released.',
  groups:[['WHEN YOU RELEASE A FISH',['Gently, back in the water','Without harming it']],
    ['WHEN YOU KEEP A FISH',['It stays kept: no swapping for a bigger one','Don’t waste it','Don’t hold it alive (no stringer or livewell)','Leave head, tail and fins on until home','No canning or filleting until home','Don’t freeze fish into one block: each must stay countable','Each fish must stay identifiable and measurable']],
    ['CARRYING FISH',['Never more than your legal limit','Licence at hand while you travel with fish','Carrying someone else’s fish? Have their signed letter','Taking fish out of B.C.? Show the letter if asked','Given fish? Keep the signed letter until eaten']],
    ['NEVER',['Sell, buy or trade your catch','Keep or move live fish or crayfish','Release aquarium fish','Put gear in the water during a closure','Interfere with a fur trap','Cross private, posted or reserve land without permission']]]
};
const PROT13 = ['Cultus Lake sculpin','Enos Lake stickleback','Green sturgeon','Misty Lake stickleback','Nooksack dace','Paxton Lake stickleback','Rocky Mountain sculpin','Salish sucker','Shorthead sculpin','Vananda Creek stickleback','Vancouver lamprey','Western brook lamprey (Morrison Creek population)','White sturgeon (Nechako, Upper Fraser, Kootenay and Columbia populations)'];
const PROT12 = PROT13.filter(x => x !== 'Green sturgeon');

/* ---------- licence words shared by the waters (page v36 Licence tile) ---------- */
const DOC = {
  basic:{n:'Basic angling licence',w:'Needed to fish at all',p:'$41.15 a year · $11.43 a day · $22.86 for 8 days',base:1},
  st:{n:'Steelhead Conservation Surcharge Stamp',w:'Only if you fish for steelhead, even to release them',p:'$28.57 a year'},
  wsg:{n:'White Sturgeon Conservation Licence',w:'Only if you fish for white sturgeon, even to release them',p:'$28.57 a year · $9.14 a day · $17.14 for 8 days'},
  salmon:{n:'Conservation Surcharge Stamp for salmon',w:'Only if you keep salmon',p:'$17.14 a year'},
};
const SRC_HERE = ['TO FISH HERE',[['Anglers 16 and over need a basic angling licence to fish.','Province','basic angling licence'],
  ['Annual basic angling licence: at most 1 licence per licence year.','',''],
  ['Anglers 16 and over: produce your angling licence and photo ID when an officer asks.','Province','Produce your angling licence and photo ID when an officer asks · Not needed if you are a status First Nations person living in B.C.']]];
const SRC_SALMON = ['TO KEEP SALMON',[['Anglers 16 and over need a Conservation Surcharge Stamp for salmon to keep salmon (not kokanee).','Province','Conservation Surcharge Stamp for salmon']]];
const SRC_RECORD = ['WHEN YOU KEEP A FISH YOU MUST RECORD',[['Anglers 16 and over: carry your paper licence when keeping a fish whose retention you must record on your licence: hatchery steelhead, adult chinook, rainbow trout over 50 cm (Main body of Kootenay Lake), lake trout and Dolly Varden/bull trout over 60 cm (Shuswap Lake) or rainbow trout over 50 cm (Shuswap system).','Province','Carry your paper licence · Not needed if you are a status First Nations person living in B.C.']]];
const GUIDING = ['For every angler: You need an angling guide licence or an assistant angling guide licence to guide anglers.','Province','angling guide licence OR assistant angling guide licence'];
const OTHERS = ['For under 16, canadian, other province or from outside Canada: Non-residents or non-resident aliens under 16: to fish, be accompanied by someone 16 or over who holds the licences and stamps this fishing requires (any fish you keep count toward your companion\'s limit), or hold what non-residents or non-resident aliens 16 and over must hold (you keep your own quota).','Province','Nothing to buy: you’re exempt — be with someone who is 16 or older and holds what this fishing needs (fish you keep count toward their limit) OR meet the rules as 16 or older, canadian, other province or from outside Canada'];
const U16 = {tile:{v:'None needed',s:'Nothing extra'},head:'No licence needed to fish here',
  body:'Anglers under 16 who live in B.C. don’t need a basic angling licence. The fishing rules and limits still apply.'};
const WAYS_NO = ['Set lines','Spear or bow','Nets','Snagging','Chumming'];
const ANGLING = ['Angling (rod and line)','with a downrigger: attached to your line by a quick-release · with a light: submerged, attached to the line, within 1 m of the hook'];
const TRAPS = ['Crayfish traps','release anything in the trap that isn’t a crayfish'];
const LINE_COMMON = [['HOOK, LURE OR FLY PER LINE','1','one hook, or one lure, or one fly','Province']];
const LURES_WEIGHT = [['LURES','Any lure or fly','',''],['WEIGHT ON THE LINE','up to 1 kg','not when the weight is a downrigger’s','Province']];

/* ---------- one helper for the many "Region-wide n a day" rows ---------- */
const regionWide = (reg, n, txt, fish) => ({word:`${n} a day`, cls:'keep', big:String(n), unit:'a day', scope:`REGION ${reg}-WIDE`,
  say:`Shared with all of Region ${reg} today.`, fish,
  ladder:{ok:`✓ ${n} a day here, for all of Region ${reg} together.`, g:[[`Region ${reg}`,[[txt,'Daily limit']]]], other:[[`Region ${reg}`,[POSS]]]}});

/* ---------- VEDDER RIVER ---------- */
const V_R2 = {
  four:['Keep up to 4 trout and char a day, all kinds together.','Daily limit'],
  wild:['In streams, release every wild trout and char.','Release'],
  two:['In streams, keep up to 2 trout and char a day, all kinds together, hatchery only.','Lower daily limit'],
  size30:['In streams, keep only trout and char 30 cm or longer, hatchery only.','Size'],
  cap50:['Only 1 trout or char over 50 cm a day (not steelhead).','Extra cap'],
};
const vKinds = (p) => [
  {n:'Brook, brown and cutthroat trout', verb:'Keep up to', v:2, chips:['30 cm or longer','hatchery only'], note:'Only 1 over 50 cm.',
   ladder:{ok:'✓ Keep up to 2, 30 cm or longer. Only 1 over 50 cm. They count toward your 2.', g:[['Region 2',[V_R2.four,V_R2.wild,V_R2.two,V_R2.size30,V_R2.cap50]]]}},
  {n:'Dolly Varden, bull trout and lake trout', verb:'Keep', v:1, of:'of your 2', chips:['60 cm or longer','hatchery only'], note:'All are over 50 cm: each one uses your 1 fish over 50 cm.',
   ladder:{ok:'✓ Keep up to 1, 60 cm or longer. They count toward your 2.', g:[['Region 2',[V_R2.four,V_R2.wild,V_R2.two,V_R2.size30,['Keep only Dolly Varden, bull trout and lake trout 60 cm or longer.','Size'],V_R2.cap50,['Keep up to 1 Dolly Varden, bull trout or lake trout a day.','Extra cap']]]]}},
  p === 0 ?
  {n:'Rainbow trout', verb:'Keep up to', v:4, chips:['up to 50 cm','hatchery only'], note:'Rainbows over 50 cm count as steelhead.',
   ladder:{ok:'✓ Keep up to 4, up to 50 cm, hatchery only. Each one counts toward your 4.', g:[['Region 2',[V_R2.wild,V_R2.four]],['Vedder River',[['Keep up to 4 rainbow trout a day, hatchery only, Jul 1–Apr 30.','Daily limit']]]]}} :
  {n:'Rainbow trout', verb:'Keep up to', v:2, chips:['30–50 cm','hatchery only'], note:'Rainbows over 50 cm count as steelhead.',
   ladder:{ok:'✓ Keep up to 2, 30–50 cm. They count toward your 2.', g:[['Region 2',[V_R2.four,V_R2.wild,V_R2.two,V_R2.size30]]]}},
  {n:'Steelhead', verb:'Keep up to', v:2, chips:['over 50 cm','hatchery only'], note:'Under 50 cm, it’s a rainbow trout.',
   extras:['10 a year','Record it on your licence','Stop fishing for the day after 2'],
   ladder:{ok:'✓ Keep up to 2, over 50 cm. They count toward your 2.', g:[['All of B.C.',[['In streams, keep up to 10 steelhead a licence year.','Yearly limit'],['In streams, record each hatchery steelhead on your licence right away.','Record it']]],
     ['Region 2',[V_R2.four,V_R2.wild,V_R2.two,V_R2.size30,['Only 2 hatchery steelhead over 50 cm a day.','Extra cap'],['Once you’ve kept your daily limit of hatchery steelhead, stop fishing this water for the rest of the day.','Stop fishing']]]]}},
];
const vTrout = (p) => ({
  t:'Trout and char', scope:'ALL REGION 2 STREAMS', big:'2', unit:'a day',
  lead:'<b>2 a day in total, all kinds together.</b> Shared with every Region 2 stream you fish today.',
  qa:[['Kept 2 on another stream?','Keep 0 here'],['Kept some at a lake?','Those don’t count here, but stop at 4 for the whole day']],
  every:[{h:'<b>Hatchery only</b>, release wild ones', tell:1},{h:'<b>Only 1 fish over 50 cm in total</b> (steelhead don’t count)'}],
  note:'Steelhead are known to be in this water.',
  kindsHead: p === 0 ? 'EACH KIND' : 'EACH KIND · EVERY FISH COUNTS TOWARD THE 2',
  kinds: vKinds(p),
  fish: p === 0 ? ['EB','GB','CT','DV','LT','ST'] : ['EB','GB','CT','DV','LT','RB','ST'],
  src:{sub:'Vedder River · Oct 10. The more specific rule speaks first: this water, then from downstream, a named area, the region, then the province.',
   on:[['REGION','Trout/char: 4','Region 2 · region-wide','Sets the number'],['REGION','2 from streams (must be hatchery)','Region 2 · region-wide','Lowers the number'],
     ['REGION','1 over 50 cm','Region 2 · region-wide','A limit inside the total · for cutthroat trout, Dolly Varden/bull trout, brook trout, brown trout and lake trout'],
     ['REGION','Hatchery trout/char under 30 cm from streams','Region 2 · region-wide','Size limit'],['REGION','Possession quotas = 2 daily quotas','Region 2 · region-wide','Possession limit'],
     ['REGION','Wild trout/char from streams','Region 2 · region-wide','Release every one'],
     ['REGION','1 char (bull trout, Dolly Varden, or lake trout)','Region 2 · region-wide','A limit inside the total · for Dolly Varden/bull trout and lake trout'],
     ['REGION','none under 60 cm','Region 2 · region-wide','Size limit · for Dolly Varden/bull trout and lake trout'],
     ['REGION','2 hatchery steelhead over 50 cm allowed','Region 2 · region-wide','A limit inside the total · for steelhead'],
     ['REGION','When you have caught and retained your daily quota of hatchery steelhead from any water, you must stop fishing that water for the remainder of that day.','Region 2 · region-wide','Something you must do · for steelhead'],
     ['PROVINCE','The annual province-wide quota for hatchery steelhead is 10.','Provincial · in Regions 1, 2, 3, 5 and 6','Yearly limit · for steelhead'],
     ['PROVINCE','You must immediately record your retention of hatchery steelhead on your basic angling licence.','Provincial · in Regions 1, 2, 3, 5 and 6','Something you must do · for steelhead']],
   off:[['REGION','1 over 50 cm (2 hatchery steelhead over 50 cm allowed)','Region 2 · region-wide','Beaten by “1 over 50 cm” · for cutthroat trout, Dolly Varden/bull trout, brook trout, brown trout and lake trout'],
     ['REGION','All wild steelhead','Region 2 · region-wide','Says the same as “Wild trout/char from streams” · for steelhead'],
     ['PROVINCE','Possession quota — no more than twice the daily quota unless otherwise specified.','Provincial · province-wide','Beaten by “Possession quotas = 2 daily quotas”'],
     ['PROVINCE','All wild steelhead must be released.','Provincial · in Regions 1, 2, 3, 5 and 6','Beaten by “All wild steelhead” · for steelhead']]},
});
const vOther = [
  ['Kokanee',{word:'release', cls:'rel', say:'You can fish for them, but release every one. No keeping at any time of year. The whole water is closed Jun 1–30.', fish:['KO'],
    ladder:{ok:'✓ Release every one here.', g:[['Region 2',[['In streams, release every kokanee.','Release']]]], other:[['Region 2',[POSS]]], skip:[['Keep up to 5 kokanee a day.','Replaced by the Region 2 rule']]}}],
  ['Whitefish', regionWide(2,15,'Keep up to 15 whitefish a day, all kinds together.',['LW','MW'])],
  ['Bass', regionWide(2,20,'Keep up to 20 bass a day, all kinds together.',['LMB','SMB'])],
  ['Black crappie', regionWide(2,20,'Keep up to 20 black crappie a day.',['BCB'])],
  ['White sturgeon',{word:'release', cls:'rel', say:'You can fish for them, but release every one. No keeping at any time of year. The whole water is closed Jun 1–30.', fish:['WSG'],
    ladder:{ok:'✓ Release every one here.', g:[['Vedder River',[['Release every white sturgeon.','Release']]]], other:[['Region 2',[POSS]]], skip:[['No fishing for white sturgeon: every non-tidal water of the province; lifted on the licensed Fraser watershed (r4x)','Replaced by this river’s rule']]}}],
  ['Crayfish', regionWide(2,25,'Keep up to 25 crayfish a day.',[])],
  ['Protected species',{word:'closed', cls:'closed', say:'Don’t fish for them. If one bites, carefully release it.', which:PROT13,
    fish:['CULTUS_LAKE_SCULPIN','NOOKSACK_DACE','SALISH_SUCKER','GREEN_STURGEON'],
    ladder:{ok:'✓ Closed here.', g:[['Federal law',[['No fishing for Nooksack dace, Salish sucker, green sturgeon and Cultus Lake sculpin.','Closed']]]]}}],
];
const vGear = {
  tiles:{licence:{v:'Basic licence',s:'+ steelhead stamp and 2 more if needed'}, line:{v:'Single barbless hook',s:'1 line'},
    bait:{v:'Some bait OK',items:[['Worms',1],['Roe',1],['Insects',1],['Fish parts',0]]}, ways:{v:'Angling',s:'Also crayfish traps'},
    always:{v:'Handling fish',s:'21 rules that apply everywhere'}},
  glance:[['y','Basic licence','+ steelhead stamp: only if you fish for steelhead, even to release them'],['y','Single barbless hook · 1 line',''],
    ['y','Bait: worms, roe, insects',''],['n','Fish or fish parts: banned','roe is still OK'],['n','Snagging','Any fish snagged — even by accident — must be released.']],
  need:[DOC.basic,DOC.st,DOC.wsg,DOC.salmon],
  record:'Keep an adult chinook or hatchery steelhead? <b>Carry your paper licence</b> (16 and over) and record it right away.',
  where:[SRC_HERE,['TO FISH FOR STEELHEAD',[['Anglers 16 and over need a Steelhead Conservation Surcharge Stamp to fish for steelhead on streams (not on a Classified Water whose row says “Steelhead Stamp not required”, while it is in force).','This water','Steelhead Conservation Surcharge Stamp']]],
    ['TO FISH FOR WHITE STURGEON',[['Anglers 16 and over need a White Sturgeon Conservation Licence to fish for white sturgeon.','This water','White Sturgeon Conservation Licence']]],SRC_SALMON,SRC_RECORD],
  lines:[['LINES','1','','Province'],...LINE_COMMON,['HOOK POINTS','Single point only (no trebles)','on streams','Region'],['BARBS','Barbless only','on streams · pinch barbs flat or buy barbless','Region'],...LURES_WEIGHT],
  bait:[['y','Worms','allowed'],['y','Roe (fish eggs)','allowed'],['y','Water insects and crayfish','on streams'],['n','Fish or fish parts','banned · roe is still OK']],
  ways:{yes:[ANGLING,TRAPS], no:WAYS_NO},
};
const vClosedJun = {word:'CLOSED', when:'on Jun 15', soon:'Opens Jul 1', head:'Closed · opens again Jul 1',
  d:'No fishing for any game fish Jun 1–30.', list:[['No fishing for any game fish, Jun 1–30.','This water']], reopen:[701,'Opens again Jul 1. See the rules from then ›']};

/* ---------- KOOTENAY LAKE — LOWER WEST ARM ---------- */
const K_LWA = 'Kootenay Lake — Lower West Arm';
const kTrout = {
  t:'Trout and char', scope:'THIS WATER', big:'2', unit:'a day',
  lead:'<b>2 a day in total, all kinds together.</b> This lake’s own limit. Fish you kept elsewhere today count toward it too.',
  every:[{h:'<b>Only 1 cutthroat trout or rainbow trout over 50 cm</b>'}], everyHead:'',
  kindsHead:'EACH KIND · EVERY FISH COUNTS TOWARD THE 2',
  kinds:[
    {n:'Cutthroat and rainbow trout', verb:'Keep up to', v:2, chips:['any size','wild or hatchery'], note:'Only 1 over 50 cm.',
     ladder:{ok:'✓ Keep up to 2, any size. Only 1 over 50 cm. They count toward your 2.', g:[['Region 4',[['Only 1 rainbow trout or cutthroat trout over 50 cm a day.','Extra cap']]],[K_LWA,[['Keep up to 2 trout and char a day, all kinds together.','Daily limit']]]]}},
    {n:'Dolly Varden, bull trout', verb:'Keep', v:1, of:'of your 2', chips:['any size','wild or hatchery'],
     ladder:{ok:'✓ Keep up to 1, any size. They count toward your 2.', g:[[K_LWA,[['Keep up to 2 trout and char a day, all kinds together.','Daily limit'],['Keep up to 1 Dolly Varden or bull trout a day.','Extra cap']]]]}},
    {n:'Brook, brown and lake trout', verb:'Keep up to', v:2, chips:['any size','wild or hatchery']},
  ],
  fish:['EB','GB','CT','DV','LT','RB'],
  src:{sub:'Kootenay Lake — Lower West Arm · Oct 10. The more specific rule speaks first: this water, then from downstream, a named area, the region, then the province.',
   on:[['THIS WATER','Trout/char daily quota = 2 (only 1 bull trout)','Region 4 · for this water (Kootenay Lake — Lower West Arm)','Sets the number'],
     ['THIS WATER','only 1 bull trout','Region 4 · for this water (Kootenay Lake — Lower West Arm)','A limit inside the total · for Dolly Varden/bull trout'],
     ['REGION','Possession quotas = 2 daily quotas (See tables for exceptions)','Region 4 · region-wide','Possession limit'],
     ['REGION','1 rainbow trout or cutthroat trout over 50 cm','Region 4 · region-wide','A limit inside the total · for cutthroat trout and rainbow trout']],
   off:[['REGION','Trout/char: 5','Region 4 · region-wide','Beaten by “Trout/char daily quota = 2 (only 1 bull trout)”'],
     ['REGION','1 bull trout (Dolly Varden) of any size','Region 4 · region-wide','Beaten by “only 1 bull trout” · for Dolly Varden/bull trout'],
     ['PROVINCE','Possession quota — no more than twice the daily quota unless otherwise specified.','Provincial · province-wide','Beaten by “Possession quotas = 2 daily quotas (See tables for exceptions)”']]},
};
const K_WEEKENDS = 'Jan 3–4 and Jan 10–11 and Jan 17–18 and Jan 24–25 and Jan 31–Feb 1 and Feb 7–8 and Feb 14–15 and Feb 21–22 and Feb 28–Mar 1 and Mar 7–8 and Mar 14–15 and Mar 21–22 and Mar 28–29 and Apr 4–5 and Apr 11–12 and Apr 18–19 and Apr 25–26 and May 2–3 and May 9–10 and May 16–17 and May 23–24 and May 30–31 and Jun 6–7 and Jun 13–14 and Jun 20–21 and Jun 27–28 and Jul 4–5 and Jul 11–12 and Jul 18–19 and Jul 25–26 and Aug 1–2 and Aug 8–9 and Aug 15–16 and Aug 22–23 and Aug 29–30 and Sep 5–6 and Sep 12–13 and Sep 19–20 and Sep 26–27 and Oct 3–4 and Oct 10–11 and Oct 17–18 and Oct 24–25 and Oct 31–Nov 1 and Nov 7–8 and Nov 14–15 and Nov 21–22 and Nov 28–29 and Dec 5–6 and Dec 12–13 and Dec 19–20 and Dec 26–27';
const kOther = (weekend) => [
  ['Kokanee', weekend ?
    {word:'5 a day', cls:'keep', big:'5', unit:'a day', scope:'THIS WATER', fish:['KO'],
     year:{say:'You can keep them until Oct 11. Then release only from Oct 12.', list:K_WEEKENDS},
     ladder:{ok:'✓ 5 a day here.', g:[[K_LWA,[['Keep up to 5 kokanee a day.','Daily limit'],['Keep up to 5 kokanee a day.','Also applies']]]], other:[['Region 4',[POSS]]],
       skip:[['Keep up to 15 kokanee a day.','Replaced by this lake’s rule'],['Only 5 kokanee over 30 cm a day.','Not needed here']]}} :
    {word:'release', cls:'rel', fish:['KO'],
     say:'You can fish for them, but release every one until Oct 16. You can keep them from Oct 17.', year:{list:K_WEEKENDS},
     ladder:{ok:'✓ Release every one here.', g:[[K_LWA,[['Release every kokanee.','Release']]]], other:[['Region 4',[POSS]]],
       skip:[['Keep up to 15 kokanee a day.','Replaced by this lake’s rule'],['Only 5 kokanee over 30 cm a day.','Replaced by this lake’s rule']]}}],
  ['Whitefish', regionWide(4,15,'Keep up to 15 whitefish a day, all kinds together.',['LW','MW'])],
  ['Bass',{word:'no limit', cls:'keep', big:'∞', unit:'No daily limit', fish:['LMB','SMB'],
    ladder:{ok:'✓ No daily limit here.', g:[[K_LWA,[['No daily limit for bass.','Daily limit']]]], other:[['Region 4',[POSS]]],
      skip:[['No fishing for bass, yellow perch, northern pike and walleye: the Kootenay Region, except waters listed in the tables','Replaced by this lake’s rule'],['No fishing for bass.','Replaced by this lake’s rule']]}}],
  ['Yellow perch',{word:'no limit', cls:'keep', big:'∞', unit:'No daily limit', fish:['YP'],
    ladder:{ok:'✓ No daily limit here.', g:[[K_LWA,[['No daily limit for yellow perch.','Daily limit']]]], other:[['Region 4',[POSS]]],
      skip:[['No fishing for bass, yellow perch, northern pike and walleye: the Kootenay Region, except waters listed in the tables','Replaced by this lake’s rule'],['No fishing for yellow perch.','Replaced by this lake’s rule']]}}],
  ['Burbot',{word:'release', cls:'rel', say:'You can fish for them, but release every one. No keeping at any time of year.', fish:['BB'],
    ladder:{ok:'✓ Release every one here.', g:[[K_LWA,[['Release every burbot.','Release']]]], other:[['Region 4',[POSS]]], skip:[['Keep up to 2 burbot a day.','Replaced by this lake’s rule']]}}],
  ['Northern pike and walleye',{word:'closed', cls:'closed', say:'Don’t fish for them. If one bites, carefully release it. All year.', fish:['NP','WP'],
    ladder:{ok:'✓ Closed here.', g:[['Region 4',[['No fishing for bass, yellow perch, northern pike and walleye: the Kootenay Region, except waters listed in the tables','Closed']]]], other:[['Region 4',[POSS]]]}}],
  ['White sturgeon',{word:'closed', cls:'closed', say:'Don’t fish for them. If one bites, carefully release it. All year.', fish:['WSG'],
    ladder:{ok:'✓ Closed here.', g:[['Region 4',[['No fishing for white sturgeon.','Closed']]]], other:[['Region 4',[POSS]]]}}],
  ['Crayfish', regionWide(4,25,'Keep up to 25 crayfish a day.',[])],
  ['Protected species',{word:'closed', cls:'closed', say:'Don’t fish for them. If one bites, carefully release it.', which:PROT12,
    fish:['CULTUS_LAKE_SCULPIN','NOOKSACK_DACE','SALISH_SUCKER'],
    ladder:{ok:'✓ Closed here.', g:[['Federal law',[['No fishing for Cultus Lake sculpin, Enos Lake stickleback, Misty Lake stickleback, Nooksack dace, Paxton Lake stickleback, Rocky Mountain sculpin, shorthead sculpin, Salish sucker, Vananda Creek stickleback, vancouver lamprey, western brook lamprey (Morrison Creek population) and white sturgeon (Nechako, Upper Fraser, Kootenay and Columbia populations).','Closed']]]]}}],
];
const kGear = {
  tiles:{licence:{v:'Basic licence',s:'+ salmon stamp if needed'}, line:{v:'Trebles and barbs OK',s:'1 line (2 alone in a boat)'},
    bait:{v:'Some bait OK',items:[['Worms',1],['Roe',1],['Insects',0],['Fish parts',0]]}, ways:{v:'Angling',s:'Also ice fishing, crayfish traps'},
    always:{v:'Handling fish',s:'21 rules that apply everywhere'}},
  glance:[['y','Basic licence','+ salmon stamp: only if you keep salmon'],['y','Trebles and barbs OK','1 line (2 while alone in a boat)'],
    ['y','Bait: worms, roe',''],['n','Water insects and crayfish: not on lakes',''],['n','Fish or fish parts: banned','roe is still OK'],['n','Snagging','Any fish snagged — even by accident — must be released.']],
  need:[DOC.basic,DOC.salmon],
  record:'Keep an adult chinook? <b>Carry your paper licence</b> (16 and over) and record it right away.',
  where:[SRC_HERE,SRC_SALMON,SRC_RECORD],
  lines:[['LINES','1','2 while alone in a boat','Province'],...LINE_COMMON,['HOOK POINTS','Trebles allowed','any hook',''],['BARBS','Barbs allowed','',''],...LURES_WEIGHT],
  bait:[['y','Worms','allowed'],['y','Roe (fish eggs)','allowed'],['n','Water insects and crayfish','not on lakes'],['n','Fish or fish parts','banned · roe is still OK']],
  ways:{yes:[ANGLING,['Ice fishing','lines: 1 · hook, lure or fly per line: 1 · Remove your ice hut before breakup · Warn others of your ice hole'],TRAPS], no:WAYS_NO},
};

/* ---------- COWICHAN RIVER ---------- */
const cClosed = {word:'CLOSED', when:'today', soon:'Opens Nov 16', head:'Closed · opens again Nov 16',
  d:'No fishing for any game fish Jul 4–Nov 15. Together these closures cover it:',
  list:[['No fishing for any game fish, Jul 4–14.','This water'],['No fishing for any game fish, Jul 15–Aug 31.','This water'],
    ['No fishing for any game fish from streams, Jul 15–Aug 31.','Named area'],['No fishing for any game fish, Sep 1–Nov 15.','This water']],
  reopen:[1116,'Opens again Nov 16. See the rules from then ›']};

/* ---------- the waters ---------- */
const WATERS = {
  vedder:{name:'Vedder River', kind:'stream', region:'REGION 2', map:'vedder', status:'open',
    parts:[{t:'From the mouth up to the Vedder Crossing Bridge', n:5, strip:[['k',151],['c',30],['k',184]]},
           {t:'From the Vedder Crossing Bridge up to the orange triangular boundary signs (200 m upstream of Vedder Crossing bridge)', n:1, strip:[['k',120],['c',61],['k',184]]}],
    frames:[{p:0,a:701,b:1231,regs:{groups:[vTrout(0)],other:vOther},gear:vGear},
            {p:0,a:601,b:630,closed:vClosedJun},
            {p:1,a:701,b:1231,regs:{groups:[vTrout(1)],other:vOther},gear:vGear}],
    caps:['Jul 1–Dec 31 (both parts)','Jun 1–30 (mouth part, closed)'], jump:[[1010,'Oct 10'],[615,'Jun 15']]},
  klwa:{name:K_LWA, short:'Kootenay L. — Lower West Arm', kind:'lake', region:'REGION 4', map:'klwa', status:'open',
    parts:[{t:'Whole lake · one set of rules', n:1, strip:[['k',365]], one:1}],
    frames:[{p:0,a:1010,b:1011,whenTxt:'today (Sat)',regs:{times:[['Kokanee: release Mon–Fri; 5 a day Sat–Sun','today (Sat–Sun): 5 a day']],groups:[kTrout],other:kOther(true)},gear:kGear},
            {p:0,a:1012,b:1016,whenTxt:'(Mon–Fri)',regs:{times:[['Kokanee: release Mon–Fri; 5 a day Sat–Sun','today (Mon–Fri): release']],groups:[kTrout],other:kOther(false)},gear:kGear}],
    caps:['Oct 10–11 (Sat–Sun)','Oct 12–16 (Mon–Fri)'], jump:[[1010,'Sat Oct 10'],[1012,'Mon Oct 12']]},
  cowichan:{name:'Cowichan River', kind:'stream', region:'REGION 1', map:'cowichan', status:'closed',
    parts:[{t:'From the mouth up to the CNR Trestle', n:9, strip:[['k',184],['c',135],['k',46]]},
           {t:'From the CNR Trestle up to the CNR Bridge', n:1},{t:'From the CNR Bridge up to the Greendale Trestle', n:1},
           {t:'From the Greendale Trestle up to the Cowichan Lake outlet', n:2},{t:'Side channel (from the mouth up to a lake outlet)', n:1}],
    frames:[{p:0,a:901,b:1115,closed:cClosed}],
    caps:['Sep 1–Nov 15 (mouth part, closed)'], jump:[[1010,'Oct 10']]},
};
// Cowichan's region is not printed on the page's card; the foot shows what the page shows.
WATERS.cowichan.region = '';
