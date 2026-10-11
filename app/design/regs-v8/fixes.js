/* regs-v8 fixes, applied over data.js. Every object here is a design-written line: it carries src:'design' and
   cite:'p.NN', where NN is the PRINTED page of the 2025–2027 Synopsis. The page renders these as
   data-src="design" and data-cite="p.NN".

   Sources:
   p.6  licences: steelhead stamp, paper licence, under 16 resident vs non-resident.
   p.8  bait (roe 1 kg), snagging.
   p.13 Region 1 general rules: summer stream closure Jul 15–Aug 31 (MU 1-1 to 1-6), single barbless,
        bait ban, trout 4 / 2 from streams hatchery / 1 over 50 (2 hatchery steelhead over 50 allowed),
        release wild trout and all char.
   p.15 Cowichan River row: No Fishing Jul 4–14 (printed in red) and Jul 15–Aug 31; weir→Greendale
        Nov 15–Apr 15; fly only upstream of the CNR Mile 66 Trestle Sep 1–Nov 15; no fishing below Mile 66
        Sep 1–Nov 15; fly only Greendale Trestle → CNR Bridge (no dates, so all year).
   p.21 Region 2 quotas: trout/char 4; 1 over 50 (2 hatchery steelhead allowed); 2 from streams (hatchery);
        release wild trout/char from streams; none under 30 cm from streams; char none under 60 cm;
        stop after the steelhead daily quota; 10 steelhead a year.
   p.22 Chilliwack/Vedder row: below Vedder Crossing (a) fly only, bait ban, hatchery rainbow ≤50 cm and
        hatchery cutthroat catch and release May 1–31; (b) No Fishing Jun 1–30; (c) hatchery rainbow any
        length ≤50 cm: 4 a day, Jul 1–Apr 30.
   p.34, p.37 Kootenay Lake West Arm.
   p.80 definitions: steelhead, snagging, hatchery.                                                    */
var SHORT_FIX, SNAG, U16BOTH, CITE = {};
(function(){
  const DIM_ = [31,28,31,30,31,30,31,31,30,31,30,31];
  const mdOf = n => { let m = 0; while (n > DIM_[m]) { n -= DIM_[m]; m++; } return (m+1)*100 + n; };
  const L = (t, cite) => ({t, src:'design', cite});
  const V = WATERS.vedder;
  const old0 = vTrout(0), old1 = vTrout(1);
  const lad = (g, name) => (g.kinds.find(k => k.n === name) || {}).ladder;

  /* ---------- Vedder: steelhead card ---------- */
  const steel = {src:'design', cite:'p.21', v:2, unit:'hatchery a day', ladder: lad(old0,'Steelhead'),
    lines:[L('Over 50 cm with the adipose fin clipped. Wild steelhead: release.','p.21, p.80'),
           L('They count toward your trout and char for the day.','p.21')],
    duty:L('Stop fishing this water after 2 · 10 a year · Record each one right away on a paper licence','p.6, p.21'),
    stamp:L('To fish for them at all, even catch-and-release, you need the steelhead stamp.','p.6')};

  /* ---------- Vedder part 1 (mouth → Vedder Crossing), Jul 1–Apr 30 ---------- */
  const g0 = {design:1, cite:'p.21–22', t:'Trout and char', scope:'Region 2 · Vedder River', big:'4', unit:'a day',
    lead:'<b>Up to 4 trout and char a day</b>, hatchery only. Wild trout and char from streams: release every one.',
    every:[{h:'<b>Hatchery only</b>, release wild ones', tell:1, cite:'p.21'},
           {h:'<b>At most 2 from streams</b>, but hatchery rainbows 50 cm or less don’t count toward the 2', cite:'p.21–22'},
           {h:'<b>Only 1 over 50 cm</b> (2 hatchery steelhead over 50 cm allowed)', cite:'p.21'}],
    kinds:[
      {n:'Rainbow trout', v:4, of:'a day', chips:['no minimum size','50 cm or less','hatchery'], cite:'p.22',
       note:'4 a day Jul 1–Apr 30; May 1–31: release. Not in the 2-from-streams count. Over 50 cm it’s a steelhead.', ladder: lad(old0,'Rainbow trout')},
      {n:'Brook, brown and cutthroat trout', v:2, of:'a day', chips:['30 cm or longer','hatchery'], cite:'p.21',
       note:'From streams. Only 1 over 50 cm.', ladder: lad(old0,'Brook, brown and cutthroat trout')},
      {n:'Dolly Varden, bull trout and lake trout', v:1, of:'a day', chips:['60 cm or longer','hatchery'], cite:'p.21',
       note:'Each one is over 50 cm, so it uses your 1 over 50 cm.', ladder: lad(old0,'Dolly Varden, bull trout and lake trout')}],
    src: old0.src};
  const size0 = L('Sizes: rainbow 50 cm or less, no minimum (4) · other trout 30 cm or longer · char 60 cm or longer · only 1 over 50 cm (steelhead: 2)','p.21–22');

  /* ---------- Vedder part 1 in May: fly only, bait ban, rainbow ≤50 and cutthroat catch and release ---------- */
  const gMay = {design:1, cite:'p.22', t:'Trout and char', scope:'Vedder River · May 1–31', big:'4', unit:'a day',
    lead:'<b>Hatchery rainbow (50 cm or less) and hatchery cutthroat: catch and release in May.</b> Other hatchery trout and char count toward 4 a day. Wild ones: release.',
    every:[{h:'<b>Hatchery only</b>, release wild ones', tell:1, cite:'p.21'},
           {h:'<b>At most 2 from streams</b>', cite:'p.21'},
           {h:'<b>Only 1 over 50 cm</b> (2 hatchery steelhead over 50 cm allowed)', cite:'p.21'}],
    kinds:[
      {n:'Rainbow trout', v:0, of:'release', chips:['50 cm or less','hatchery'], cite:'p.22', note:'Catch and release May 1–31. Over 50 cm it’s a steelhead.', ladder: lad(old0,'Rainbow trout')},
      {n:'Cutthroat trout', codes:['CT'], v:0, of:'release', chips:['hatchery'], cite:'p.22', note:'Catch and release May 1–31.'},
      {n:'Brook and brown trout', codes:['EB','GB'], v:2, of:'a day', chips:['30 cm or longer','hatchery'], cite:'p.21', note:'From streams. Only 1 over 50 cm.'},
      {n:'Dolly Varden, bull trout and lake trout', v:1, of:'a day', chips:['60 cm or longer','hatchery'], cite:'p.21', ladder: lad(old0,'Dolly Varden, bull trout and lake trout')}]};
  const mayGear = Object.assign({}, vGear, {
    tiles:Object.assign({}, vGear.tiles, {bait:{v:'Bait ban (May 1–31)'}, ways:{v:'Fly fishing only',s:'May 1–31'}}),
    bait:[['n','Worms','bait ban, May 1–31'],['n','Roe (fish eggs)','bait ban, May 1–31'],['n','Water insects and crayfish','bait ban, May 1–31'],['n','Fish or fish parts','banned']],
    ways:{yes:[['Fly fishing','fly fishing only, May 1–31']], no:['Spin or bait fishing (fly only in May)',...WAYS_NO]},
    baitWhen:L('May 1–31: bait ban, fly fishing only. Bait is OK again from Jul 1 (Jun 1–30 closed).','p.22'),
    roe:L('Roe: no more than 1 kg in possession','p.8'), stampNote:L('The steelhead stamp is needed to fish for steelhead, whether you keep or release them.','p.6'), src:'design'});

  /* ---------- Vedder part 2 (above Vedder Crossing) ---------- */
  const g1 = {design:1, cite:'p.21', t:'Trout and char', scope:'Region 2 streams', big:'2', unit:'a day from streams',
    lead:'<b>Up to 2 trout and char a day from streams</b> (within the Region 2 total of 4), hatchery only. Wild ones: release.',
    every:[{h:'<b>Hatchery only</b>, release wild ones', tell:1, cite:'p.21'},
           {h:'<b>Only 1 over 50 cm</b> (2 hatchery steelhead over 50 cm allowed)', cite:'p.21'}],
    kinds:[
      {n:'Rainbow trout', v:2, of:'a day', chips:['30 cm or longer','50 cm or less','hatchery'], cite:'p.21', note:'Over 50 cm it’s a steelhead.', ladder: lad(old1,'Rainbow trout')},
      {n:'Brook, brown and cutthroat trout', v:2, of:'a day', chips:['30 cm or longer','hatchery'], cite:'p.21', note:'Only 1 over 50 cm.', ladder: lad(old1,'Brook, brown and cutthroat trout')},
      {n:'Dolly Varden, bull trout and lake trout', v:1, of:'a day', chips:['60 cm or longer','hatchery'], cite:'p.21', ladder: lad(old1,'Dolly Varden, bull trout and lake trout')}],
    src: old1.src};
  const size1 = L('Sizes: trout 30 cm or longer · char 60 cm or longer · only 1 over 50 cm (steelhead: 2)','p.21');

  const vg = Object.assign({}, vGear, {
    tiles:Object.assign({}, vGear.tiles, {bait:{v:'Bait OK today', items:vGear.tiles.bait.items}}),
    baitWhen:L('Not May 1–31: bait ban and fly fishing only then.','p.22'),
    roe:L('Roe: no more than 1 kg in possession','p.8'),
    stampNote:L('The steelhead stamp is needed to fish for steelhead, whether you keep or release them.','p.6')});
  const regs0 = {groups:[g0], other:vOther, steel, size:size0};
  V.frames = [
    {p:0, a:101, b:430, regs:regs0, gear:vg, ans:L('Keep up to 4 trout and char a day, hatchery only. Steelhead: 2 hatchery a day.','p.21–22')},
    {p:0, a:501, b:531, regs:{groups:[gMay], other:vOther, steel, size:L('Sizes: brook/brown trout 30 cm or longer · char 60 cm or longer · only 1 over 50 cm (steelhead: 2)','p.21')}, gear:mayGear,
     ans:L('Open, fly fishing only, bait ban. Hatchery rainbow (50 cm or less) and cutthroat: catch and release. Steelhead: 2 hatchery a day.','p.22')},
    V.frames[1],
    {p:0, a:701, b:1231, regs:regs0, gear:vg, ans:L('Keep up to 4 trout and char a day, hatchery only. Steelhead: 2 hatchery a day.','p.21–22')},
    {p:1, a:101, b:430, regs:{groups:[g1], other:vOther, steel, size:size1}, gear:vg, ans:L('Keep up to 2 trout and char a day from this stream, hatchery only. Steelhead: 2 hatchery a day.','p.21')},
    {p:1, a:701, b:1231, regs:{groups:[g1], other:vOther, steel, size:size1}, gear:vg, ans:L('Keep up to 2 trout and char a day from this stream, hatchery only. Steelhead: 2 hatchery a day.','p.21')},
  ];
  V.parts[0].strip = [['k',120],['f',31],['c',30],['k',184]];
  V.parts[0].fwin = L('fly fishing only, bait ban','p.22');
  V.parts[0].closures = [L('No fishing for any game fish, Jun 1–30','p.22')];

  /* ---------- licence ---------- */
  U16BOTH = [
    {who:'B.C. resident', head:'No licence or stamp needed', body:'Fishes with his own daily limit. All the rules still apply.', src:'design', cite:'p.6'},
    {who:'Not a B.C. resident', head:'No licence only with a licensed adult', body:'With someone 16 or over who holds the licence and stamps this fishing needs, and the fish he keeps count in that adult’s limit. For his own limit, he buys his own licence and the steelhead stamp.', src:'design', cite:'p.6'}];

  /* ---------- Kootenay LWA ---------- */
  const K = WATERS.klwa;
  K.where = L('From McDonalds Landing (Six Mile) to Corra Linn Dam','p.34');
  K.frames.forEach((f, i) => {
    const ko = f.regs.other.find(([n]) => n === 'Kokanee')[1];
    ko.combined = L('Combined with the Upper West Arm: no more than 5 kokanee a day across both.','p.37');
    if (i === 1) ko.zero = L('Mon–Fri: catch and release only, keep 0.','p.37');
  });

  /* ---------- Cowichan ---------- */
  const C = WATERS.cowichan, T = C.parts;
  C.mapKey = [0,2,3,4,1];
  T[0].t = 'From the mouth up to the CNR Trestle (Mile 66)';
  const jul = [L('No fishing, Jul 4–14 (Cowichan River)','p.15'), L('No fishing, Jul 15–Aug 31 (Cowichan River, and the Region 1 summer stream closure)','p.13, p.15')];
  T[0].strip = [['k',184],['c',135],['k',46]];
  T[0].closures = [...jul, L('No fishing below the CNR Mile 66 Trestle, Sep 1–Nov 15','p.15')];
  T[1].strip = [['k',184],['c',59],['f',76],['k',46]];             // Mile 66 → CNR Bridge: fly only Sep 1–Nov 15
  T[1].fwin = L('fly fishing only','p.15'); T[1].closures = jul;
  T[2].strip = [['f',184],['c',59],['f',122]];                     // CNR Bridge → Greendale: fly only all year
  T[2].fwin = L('fly fishing only (all year)','p.15'); T[2].closures = jul;
  T[3].strip = [['c',105],['k',79],['c',59],['f',75],['c',47]];    // Greendale → lake outlet
  T[3].fwin = L('fly fishing only','p.15');
  T[3].closures = [L('No fishing from the weir to Greendale Trestle, Nov 15–Apr 15','p.15'), ...jul];
  T[4].strip = [['k',184],['c',59],['k',122]];                     // side channel: the river's closures (placement unverified)
  T[4].closures = jul;
  T[4].flag = L('Where this side channel meets the river (above or below Mile 66) isn’t confirmed. Check the signs: below Mile 66 it is closed Sep 1–Nov 15.','p.15');
  SHORT_FIX = {cowichan:['Mouth → Mile 66 Trestle','Mile 66 → CNR Bridge','CNR Bridge → Greendale','Greendale → lake outlet','Side channel']};
  C.split = {src:'design', cite:'p.15', below:0, above:1};

  const cTrout = {design:1, cite:'p.13', t:'Trout', scope:'Region 1 streams', big:'2', unit:'a day from streams',
    lead:'<b>2 hatchery trout a day from streams.</b> Wild trout, wild steelhead and all char: release every one.',
    every:[{h:'<b>Hatchery only</b>, release wild ones', tell:1, cite:'p.13'},
           {h:'<b>Only 1 over 50 cm</b> (2 hatchery steelhead over 50 cm allowed)', cite:'p.13'}],
    kinds:[{n:'Rainbow, cutthroat and brown trout, steelhead', codes:['RB','CT','GB','ST'], v:2, of:'a day', chips:['no minimum size','hatchery'], cite:'p.13', note:'All count toward the 2.'},
           {n:'Brook trout, Dolly Varden, lake trout', codes:['EB','DV','LT'], v:0, of:'release', chips:['all char: release'], cite:'p.13'}]};
  const cOther = [['Kokanee',{word:'release', cls:'rel', say:'You can fish for them, but release every one.', fish:['KO']}],
    ['Bass',{word:'no limit', cls:'keep', big:'∞', unit:'No daily limit', fish:['LMB','SMB']}],
    ['Yellow perch',{word:'no limit', cls:'keep', big:'∞', unit:'No daily limit', fish:['YP']}],
    ['White sturgeon',{word:'closed', cls:'closed', say:'Don’t fish for them. If one bites, carefully release it.', fish:['WSG']}],
    ['Crayfish',{word:'25 a day', cls:'keep', big:'25', unit:'a day', scope:'REGION 1-WIDE', fish:[]}],
    ['Protected species',{word:'closed', cls:'closed', say:'Don’t fish for them. If one bites, carefully release it.', which:PROT12, fish:['CULTUS_LAKE_SCULPIN','NOOKSACK_DACE','SALISH_SUCKER'], ladder:{ok:'Closed here.', g:[['Federal law',[['No fishing for protected species.','Closed']]]]}}]];
  const cSize = L('Sizes: no minimum for hatchery trout in streams · only 1 over 50 cm (steelhead: 2)','p.13');
  const cGear = fly => ({src:'design',
    tiles:{licence:{v:'Basic licence',s:'+ steelhead stamp if you fish for steelhead'}, line:{v:'Single barbless hook',s:'1 line'},
      bait:{v:'Bait ban'}, ways: fly ? {v:'Fly fishing only',s:fly} : {v:'Angling',s:'Also crayfish traps'},
      always:{v:'Handling fish',s:'21 rules that apply everywhere'}},
    need:[DOC.basic, DOC.st, DOC.salmon],
    record:'Keep a hatchery steelhead? <b>Carry your paper licence</b> (16 and over) and record it right away.',
    where:[SRC_HERE, SRC_SALMON, SRC_RECORD],
    lines:[['LINES','1','','Province'],...LINE_COMMON,['HOOK POINTS','Single point only (no trebles)','all streams of Region 1','Region'],['BARBS','Barbless only','pinch barbs flat or buy barbless','Region'],...LURES_WEIGHT],
    bait:[['n','Worms','bait ban (all Region 1 streams)'],['n','Roe (fish eggs)','bait ban'],['n','Water insects and crayfish','bait ban'],['n','Fish or fish parts','banned']],
    baitWhen:L('All year, in all Region 1 streams.','p.13'),
    ways: fly ? {yes:[['Fly fishing', fly],TRAPS], no:['Spin or bait fishing',...WAYS_NO]} : {yes:[ANGLING,TRAPS], no:WAYS_NO},
    stampNote:L('The steelhead stamp is needed to fish for steelhead, whether you keep or release them.','p.6')});
  // one frame per open segment of each part's strip; closed days fall through to the strip
  const frames = [C.frames[0]];
  C.frames[0].ans = L('No fishing for any game fish below the Mile 66 Trestle, Sep 1–Nov 15 (closed since Jul 4).','p.15');
  T.forEach((p, i) => { let x = 0; (p.strip||[]).forEach(([s,n]) => { const a = mdOf(x+1), b = mdOf(x+n); x += n;
    if (s === 'c' || (i === 0 && a >= 901 && b <= 1115)) return;
    const fly = s === 'f' ? (i === 2 ? 'all year, Greendale Trestle to CNR Bridge' : 'Sep 1–Nov 15, upstream of Mile 66') : '';
    frames.push({p:i, a, b, regs:{groups:[cTrout], other:cOther, size:cSize, flag:p.flag}, gear:cGear(fly),
      ans:L((fly ? 'Open, fly fishing only. ' : 'Open. ') + 'Bait ban, single barbless hook. Keep 2 hatchery trout a day from streams.', fly ? 'p.13, p.15' : 'p.13')}); }); });
  C.frames = frames;

  /* ---------- snagging ---------- */
  SNAG = L('Snagged a fish (hooked anywhere but the mouth)? Release it right away, even if it was an accident.','p.8, p.80');
})();
