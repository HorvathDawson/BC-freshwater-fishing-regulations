/* regs-v9: data loaded over data.js. Two kinds of line live here.
   (a) More of page v36, read from the page on 2026-10-10. These carry src:'v36':
       - Vedder mouth part on May 15 (card rows, gear tiles).
       - Cowichan upstream parts on Oct 10 (card and gear tiles).
   (b) Corrections where the answer key (fisheval/answer_key.md) says v36 is wrong. These carry
       src:'design' and cite:'p.NN' (the printed Synopsis page):
       - Vedder trout/char total and group sentence (Q1, Q3: 4 a day; v36's "2 a day in total / Keep 0 here"
         ignores the r9 lift).
       - The char row's "of your 2".
       - Under-16 non-resident wording (Q5).
       - Fly-only windows on the strip (p.15, p.22). The owner asked to keep their look.
   Everything else is data.js as-is. */
var V9 = {};
(function(){
  const D = (t, cite) => ({t, src:'design', cite});
  const V = WATERS.vedder;

  /* ---- (b) Vedder mouth part: the group total and sentence ---- */
  function fixTotal(g){
    g.big = '4'; g.totalSrc = D('', 'p.21–22');
    g.lead = '<b>Up to 4 trout and char a day, all kinds together.</b> At most 2 from streams, except hatchery rainbow 50 cm or less.';
    g.leadSrc = D('', 'p.21–22');
    g.qa = null;
    g.kindsHead = 'EACH KIND · EVERY FISH COUNTS TOWARD THE 4';
    g.kinds.forEach(k => { if (k.of === 'of your 2') { k.of = 'a day'; k.ofSrc = D('', 'p.21'); } });
    return g;
  }
  const oct = fixTotal(vTrout(0));
  V.frames[0].regs.groups = [oct];

  /* ---- (a) Vedder mouth part, May 1–31, as page v36 shows it on May 15 ---- */
  const may = fixTotal(vTrout(0));
  const byName = n => may.kinds.find(k => k.n === n);
  const relMay = {verb:'Release', v:0, rel:'Release every one until May 31', note:'Then the whole water is closed Jun 1–30. You can keep them from Jul 1.'};
  may.kinds = [
    Object.assign({}, byName('Brook, brown and cutthroat trout'), {n:'Brook and brown trout'}),
    byName('Dolly Varden, bull trout and lake trout'),
    Object.assign({}, byName('Rainbow trout'), relMay, {chips:[], note:relMay.note + ' Rainbows over 50 cm count as steelhead.'}),
    byName('Steelhead'),
    Object.assign({n:'Cutthroat trout', chips:[]}, relMay, {ladder:null})];
  const KC = window.KCODES_EXTRA = {'Brook and brown trout':['EB','GB'], 'Cutthroat trout':['CT']};
  const mayGear = Object.assign({}, vGear, {src:'v36',
    tiles:{licence:vGear.tiles.licence, line:{v:'Single barbless hook', s:'1 line · Fly fishing only: no float or sinker'},
      bait:{v:'Bait ban', s:'Lures and flies only'}, ways:{v:'Angling', s:'Nothing else'}, always:vGear.tiles.always},
    lines:vGear.lines.filter(([l]) => l !== 'LURES' && l !== 'WEIGHT ON THE LINE'),
    bait:[['n','Worms','bait ban'],['n','Roe (fish eggs)','bait ban'],['n','Water insects and crayfish','bait ban'],['n','Fish or fish parts','bait ban']],
    baitNote:'No natural bait of any kind while the ban runs.',
    ways:{yes:[['Angling (rod and line)','fly fishing only: nothing but the fly on the line, no float, sinker or attractor']], no:['Set lines','Spear or bow','Crayfish traps','Nets','Snagging','Chumming']}});
  V.frames.push({p:0, a:501, b:531, src:'v36', soon:'Closed from Jun 1', regs:{groups:[may], other:vOther}, gear:mayGear});
  V.frames.push({p:0, a:101, b:430, regs:{groups:[oct], other:vOther}, gear:vGear});

  /* ---- fly-only windows on the strips (design, so the look the owner liked stays) ---- */
  V.parts[0].strip = [['k',120],['f',31],['c',30],['k',184]];
  V.parts[0].fwin = D('fly fishing only, bait ban', 'p.22');

  /* ---- (a) Cowichan upstream parts on Oct 10, from page v36 ---- */
  const C = WATERS.cowichan, P = C.parts;
  C.mapKey = [0,2,3,4,1];
  C.short = ['Mouth → CNR Trestle','CNR Trestle → CNR Bridge','CNR Bridge → Greendale','Greendale → lake outlet','Side channel'];
  P[1].strip = [['k',184],['c',59],['f',76],['k',46]];
  P[2].strip = [['f',184],['c',59],['f',122]];
  P[3].strip = [['c',105],['k',79],['c',59],['f',75],['c',47]];
  P[4].strip = [['k',184],['c',59],['k',122]];
  P[1].fwin = D('fly fishing only','p.15'); P[2].fwin = D('fly fishing only (all year)','p.15'); P[3].fwin = D('fly fishing only','p.15');
  C.split = {at:'CNR Mile 66 Trestle', below:0, above:1};
  const cTrout = {src:'v36', t:'Trout', scope:'ALL REGION 1, EXCLUDING HAIDA GWAII STREAMS', big:'2', unit:'a day',
    lead:'<b>2 a day in total, all kinds together.</b> Shared with every Region 1, excluding Haida Gwaii stream you fish today.',
    qa:[['Kept 2 on another stream?','Keep 0 here'],['Kept some at a lake?','Those don’t count here, but stop at 4 for the whole day']],
    every:[{h:'<b>Only 1 fish over 50 cm in total</b> (steelhead don’t count)'}], note:'Steelhead are known to be in this water.',
    kindsHead:'EACH KIND · EVERY FISH COUNTS TOWARD THE 2',
    kinds:[{n:'Brown and cutthroat trout', verb:'Keep up to', v:2, chips:['any size','hatchery only'], note:'Only 1 over 50 cm.'},
           {n:'Rainbow trout', verb:'Keep up to', v:2, chips:['up to 50 cm','hatchery only'], note:'Rainbows over 50 cm count as steelhead.'},
           {n:'Steelhead', verb:'Keep up to', v:2, chips:['over 50 cm','hatchery only'], note:'Under 50 cm, it’s a rainbow trout.', extras:['10 a year','Record it on your licence']}]};
  KC['Brown and cutthroat trout'] = ['GB','CT'];
  const cOther = [
    ['Char',{word:'release', cls:'rel', say:'You can fish for them, but release every one. No keeping at any time of year. The whole water is closed Jul 4–Aug 31.', fish:['EB','DV','LT']}],
    ['Kokanee',{word:'release', cls:'rel', say:'You can fish for them, but release every one.', fish:['KO']}],
    ['Bass',{word:'no limit', cls:'keep', big:'∞', unit:'No daily limit', fish:['LMB','SMB']}],
    ['Yellow perch',{word:'no limit', cls:'keep', big:'∞', unit:'No daily limit', fish:['YP']}],
    ['White sturgeon',{word:'closed', cls:'closed', say:'Don’t fish for them. If one bites, carefully release it.', fish:['WSG']}],
    ['Crayfish',{word:'25 a day', cls:'keep', big:'25', unit:'a day', fish:[]}],
    ['Protected species',{word:'closed', cls:'closed', say:'Don’t fish for them. If one bites, carefully release it.', which:PROT12, fish:['CULTUS_LAKE_SCULPIN','NOOKSACK_DACE','SALISH_SUCKER']}]];
  const cGear = {src:'v36',
    tiles:{licence:{v:'Basic licence', s:'+ steelhead stamp and 1 more if needed'}, line:{v:'Single barbless hook', s:'1 line · Fly fishing only: no float or sinker'},
      bait:{v:'Bait ban', s:'Lures and flies only'}, ways:{v:'Angling', s:'Nothing else'}, always:{v:'Handling fish', s:'21 rules that apply everywhere'}},
    need:[DOC.basic, DOC.st, DOC.salmon], record:'Keep an adult chinook or hatchery steelhead? <b>Carry your paper licence</b> (16 and over) and record it right away.',
    where:[SRC_HERE, SRC_SALMON, SRC_RECORD],
    lines:[['LINES','1','','Province'],...LINE_COMMON,['HOOK POINTS','Single point only (no trebles)','on streams','Region'],['BARBS','Barbless only','on streams · pinch barbs flat or buy barbless','Region']],
    bait:[['n','Worms','bait ban'],['n','Roe (fish eggs)','bait ban'],['n','Water insects and crayfish','bait ban'],['n','Fish or fish parts','bait ban']],
    baitNote:'No natural bait of any kind while the ban runs.',
    ways:{yes:[['Angling (rod and line)','fly fishing only: nothing but the fly on the line, no float, sinker or attractor']], no:['Set lines','Spear or bow','Crayfish traps','Nets','Snagging','Chumming']}};
  [1,2,3].forEach(p => C.frames.push({p, a:901, b:1114, src:'v36', regs:{groups:[cTrout], other:cOther}, gear:cGear}));
  C.frames.push({p:4, a:901, b:1231, src:'v36', regs:{groups:[cTrout], other:cOther}, gear:Object.assign({}, cGear, {tiles:Object.assign({}, cGear.tiles, {line:{v:'Single barbless hook', s:'1 line'}})})});

  /* ---- (b) under 16, both outcomes ---- */
  V9.u16 = [
    {who:'B.C. resident', head:'No licence or stamp needed', body:'Fishes with his own daily limit. The fishing rules and limits still apply.', src:'design', cite:'p.6'},
    {who:'Not a B.C. resident', head:'No licence only with a licensed adult', body:'With someone 16 or older who holds the licence and stamps this fishing needs; any fish he keeps count toward that adult’s limit. For his own limit he buys his own licence (and the steelhead stamp to fish for steelhead).', src:'design', cite:'p.6'}];
})();
