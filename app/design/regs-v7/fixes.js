/* regs-v7 fixes, applied over data.js after angler round 1.
   The wording comes from the fisheval answer key (Synopsis 2025–2027, printed pages) and from page v36's taps.
   Every field written here carries src:'design', and the page renders it with data-src="design" so it can be traced.

   1. Vedder trout/char. v36's "2 a day in total / Keep 0 here" lead is a template bug: the r9 lift
      (4 hatchery rainbow ≤50 cm) was ignored. The book (p.21–22) says up to 4 trout and char a day.
   2. Steelhead gets its own card: 2 hatchery a day, stop after 2, 10 a year, record on a paper licence.
   3. Vedder part 1: May 1–31 fly fishing only and bait ban (p.22 (a)). Roe: 1 kg (p.8).
   4. Cowichan: split at the CNR Mile 66 Trestle (p.15); upstream parts are open, fly fishing only,
      from v36 taps cow2–cow4. The side channel follows v36 cow1 (open, bait ban); its placement is a
      known data flag.
   5. Kootenay LWA: combined 5 kokanee with the Upper West Arm; boundaries (p.34, p.37).
   6. Under-16 non-resident path (p.6).                                                              */
(function(){
  const D = 'design';
  const V = WATERS.vedder;
  const old0 = vTrout(0), old1 = vTrout(1);
  const lad = (g, name) => (g.kinds.find(k => k.n === name) || {}).ladder;

  /* ---- 1 + 2: Vedder trout and char, and the steelhead card ---- */
  const steel = {src:D, v:2, unit:'hatchery a day', codes:['ST'], ladder: lad(old0,'Steelhead'),
    lines:[
      'Over 50 cm with the adipose fin clipped. Wild steelhead: release.',
      '<b>Once you have kept 2, stop fishing this water</b> for the rest of the day.',
      '<b>10 a year</b> (per licence year).',
      '<b>Record each one on your paper licence right away.</b>',
      'Counts toward your trout and char for the day.'],
    stamp:'To fish for them at all, even catch-and-release, you need the steelhead stamp.'};
  const g0 = {src:D, t:'Trout and char', scope:'Region 2 · Vedder River', big:'4', unit:'a day',
    lead:'<b>Up to 4 trout and char a day</b>, hatchery only. Wild trout and char from streams: release every one.',
    every:[{h:'<b>Hatchery only</b>, release wild ones', tell:1},
           {h:'<b>At most 2 from streams</b>, but hatchery rainbows 50 cm or less don’t count toward the 2'},
           {h:'<b>Only 1 over 50 cm</b> (2 hatchery steelhead over 50 cm allowed)'}],
    kindsHead:'Each kind',
    kinds:[
      {n:'Rainbow trout', v:4, of:'a day', chips:['any length, 50 cm or less','hatchery','Jul 1–Apr 30'],
       note:'The 30 cm minimum and the 2-from-streams cap don’t apply. Over 50 cm it’s a steelhead.', ladder: lad(old0,'Rainbow trout')},
      {n:'Brook, brown and cutthroat trout', v:2, of:'a day', chips:['none under 30 cm','hatchery'],
       note:'From streams. Only 1 over 50 cm.', ladder: lad(old0,'Brook, brown and cutthroat trout')},
      {n:'Dolly Varden, bull trout and lake trout', v:1, of:'a day', chips:['none under 60 cm','hatchery'],
       note:'Each one is over 50 cm, so it uses your 1 over 50 cm.', ladder: lad(old0,'Dolly Varden, bull trout and lake trout')}],
    src: old0.src, design:1};
  const g1 = {src:D, t:'Trout and char', scope:'Region 2 streams', big:'2', unit:'a day from streams',
    lead:'<b>Up to 2 trout and char a day from streams</b> (within the Region 2 total of 4), hatchery only. Wild ones: release.',
    every:[{h:'<b>Hatchery only</b>, release wild ones', tell:1},
           {h:'<b>Only 1 over 50 cm</b> (2 hatchery steelhead over 50 cm allowed)'}],
    kindsHead:'Each kind',
    kinds:[
      {n:'Rainbow trout', v:2, of:'a day', chips:['30–50 cm','hatchery'], note:'Over 50 cm it’s a steelhead.', ladder: lad(old1,'Rainbow trout')},
      {n:'Brook, brown and cutthroat trout', v:2, of:'a day', chips:['none under 30 cm','hatchery'], note:'Only 1 over 50 cm.', ladder: lad(old1,'Brook, brown and cutthroat trout')},
      {n:'Dolly Varden, bull trout and lake trout', v:1, of:'a day', chips:['none under 60 cm','hatchery'], ladder: lad(old1,'Dolly Varden, bull trout and lake trout')}],
    src: old1.src, design:1};
  V.frames[0].regs.groups = [g0]; V.frames[0].regs.steel = steel;
  V.frames[0].ans = {src:D, line:'Keep up to 4 trout and char a day, hatchery only. Steelhead: 2 hatchery a day.'};
  V.frames[2].regs.groups = [g1]; V.frames[2].regs.steel = steel;
  V.frames[2].ans = {src:D, line:'Keep up to 2 trout and char a day from this stream, hatchery only. Steelhead: 2 hatchery a day.'};

  /* ---- 3: May 1–31 fly fishing only + bait ban on the mouth part; roe 1 kg ---- */
  V.parts[0].strip = [['k',120],['f',31],['c',30],['k',184]];
  V.parts[0].fwin = {src:D, label:'fly fishing only, bait ban'};
  const vg = V.frames[0].gear;
  vg.baitWhen = {src:D, t:'Not May 1–31', s:'May 1–31: bait ban, fly fishing only.'};
  vg.roe = {src:D, t:'Roe: no more than 1 kg in possession', s:'Carry a receipt if you bought it.'};
  vg.tiles = Object.assign({}, vg.tiles, {bait:{v:'Bait OK today', items:vg.tiles.bait.items}});

  /* ---- 6: licence words for steelhead + under-16 by residency ---- */
  vg.stampNote = {src:D, t:'The steelhead stamp is needed to fish for steelhead, whether you keep or release them.'};
  window.U16BY = {
    bc:{src:D, head:'No licence or stamp needed', body:'B.C. residents under 16 fish without a licence or stamp, with their own daily limit. All the rules still apply.'},
    non:{src:D, head:'No licence, but only with a licensed adult', body:'A non-resident under 16 may fish without a licence when with someone 16 or over who holds the licence and stamps this fishing needs. Any fish he keeps count in that adult’s limit. To keep his own limit, he buys his own licence (and the steelhead stamp to fish for steelhead).'}};

  /* ---- 5: Kootenay LWA ---- */
  const K = WATERS.klwa;
  K.where = {src:D, t:'From McDonalds Landing (Six Mile) to Corra Linn Dam'};
  K.frames.forEach((f, i) => {
    const ko = f.regs.other.find(([n]) => n === 'Kokanee')[1];
    ko.combined = {src:D, t:'Combined with the Upper West Arm: no more than 5 kokanee a day in total across both.'};
    if (i === 1) ko.zero = {src:D, t:'Mon–Fri: catch and release only, keep 0.'};
  });

  /* ---- 4: Cowichan, split at the CNR Mile 66 Trestle ---- */
  const C = WATERS.cowichan;
  const T = C.parts; // [mouth→CNR Trestle, CNR Trestle→CNR Bridge, CNR Bridge→Greendale, Greendale→lake outlet, side channel]
  C.mapKey = [0,2,3,4,1];
  T[0].t = 'From the mouth up to the CNR Trestle (Mile 66)';
  T[0].fwin = null;
  const up = [['k',184],['c',59],['f',76],['k',46]];                          // closed Jul 4–Aug 31, fly only Sep 1–Nov 15
  T[1].strip = up; T[2].strip = up;
  T[3].strip = [['c',105],['k',79],['c',59],['f',75],['c',47]];               // + weir to Greendale closed Nov 15–Apr 15
  T[1].fwin = T[2].fwin = T[3].fwin = {src:D, label:'fly fishing only'};
  T[4].strip = null;
  C.split = {src:D, below:'Closed below the CNR Mile 66 Trestle', up:'Upstream of it: open, fly fishing only (Sep 1–Nov 15).'};
  const cGear = {src:D,
    tiles:{licence:{v:'Basic licence',s:'+ steelhead stamp if you fish for steelhead'}, line:{v:'Single barbless hook',s:'1 line'},
      bait:{v:'Bait ban',items:[['Worms',0],['Roe',0],['Insects',0],['Fish parts',0]]}, ways:{v:'Fly fishing only',s:'Sep 1–Nov 15, upstream of Mile 66'},
      always:{v:'Handling fish',s:'21 rules that apply everywhere'}},
    need:[DOC.basic, DOC.st, DOC.salmon],
    record:'Keep a hatchery steelhead? <b>Carry your paper licence</b> (16 and over) and record it right away.',
    where:[SRC_HERE, SRC_SALMON, SRC_RECORD],
    lines:[['LINES','1','','Province'],...LINE_COMMON,['HOOK POINTS','Single point only (no trebles)','all streams of Region 1','Region'],['BARBS','Barbless only','pinch barbs flat or buy barbless','Region'],...LURES_WEIGHT],
    bait:[['n','Worms','bait ban'],['n','Roe (fish eggs)','bait ban'],['n','Water insects and crayfish','bait ban'],['n','Fish or fish parts','banned']],
    ways:{yes:[['Fly fishing','fly fishing only, Sep 1–Nov 15'],TRAPS], no:['Spin or bait fishing (fly only now)',...WAYS_NO]}};
  const cRegs = {src:D,
    groups:[{design:1, t:'Trout', scope:'Region 1 streams', big:'2', unit:'a day from streams',
      lead:'<b>2 hatchery trout a day from streams.</b> Wild trout: release every one.',
      every:[{h:'<b>Hatchery only</b>, release wild ones', tell:1},{h:'<b>Only 1 brown or cutthroat over 50 cm</b>'}],
      kindsHead:'Each kind',
      kinds:[{n:'Rainbow, cutthroat and brown trout, steelhead', codes:['RB','CT','GB','ST'], v:2, of:'a day', chips:['hatchery'], note:'All count toward the 2.'},
             {n:'Brook trout, Dolly Varden, lake trout', codes:['EB','DV','LT'], v:0, of:'release', chips:['release every one']}]}],
    other:[['Kokanee',{word:'release', cls:'rel', say:'You can fish for them, but release every one.', fish:['KO']}],
      ['Bass',{word:'no limit', cls:'keep', big:'∞', unit:'No daily limit', fish:['LMB','SMB']}],
      ['Yellow perch',{word:'no limit', cls:'keep', big:'∞', unit:'No daily limit', fish:['YP']}],
      ['White sturgeon',{word:'closed', cls:'closed', say:'Don’t fish for them. If one bites, carefully release it.', fish:['WSG']}],
      ['Crayfish',{word:'25 a day', cls:'keep', big:'25', unit:'a day', scope:'REGION 1-WIDE', fish:[]}],
      ['Protected species',{word:'closed', cls:'closed', say:'Don’t fish for them. If one bites, carefully release it.', which:PROT12, fish:['CULTUS_LAKE_SCULPIN','NOOKSACK_DACE','SALISH_SUCKER'], ladder:{ok:'Closed here.', g:[['Federal law',[['No fishing for protected species.','Closed']]]]}}]]};
  [1,2,3].forEach(p => C.frames.push({p, a:901, b:1114, src:D, regs:cRegs, gear:cGear,
    ans:{src:D, line:'Open, fly fishing only. Bait ban, single barbless hook. Keep 2 hatchery trout a day.'}}));
  C.frames.push({p:4, a:101, b:1231, src:D, regs:cRegs, gear:Object.assign({}, cGear, {tiles:Object.assign({}, cGear.tiles, {ways:{v:'Angling',s:'Bait ban'}}), ways:{yes:[ANGLING,TRAPS], no:WAYS_NO}}),
    ans:{src:D, line:'Open. Bait ban, single barbless hook. Keep 2 hatchery trout a day.'}});
  C.frames[0].ans = {src:D, line:'No fishing for any game fish below the Mile 66 Trestle, Jul 4–Nov 15.'};
  SHORT_FIX = {cowichan:['Mouth → Mile 66 Trestle','Mile 66 → CNR Bridge','CNR Bridge → Greendale','Greendale → lake outlet','Side channel']};

  /* ---- 12: what "snagged" means (p.80) ---- */
  window.SNAG = {src:D, t:'Snagged a fish (hooked anywhere but the mouth)? Release it right away, even if it was an accident.'};
})();
var SHORT_FIX;
