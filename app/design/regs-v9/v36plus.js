/* regs-v9: data loaded over data.js, taken from the REGENERATED page v36 (answers 2.5), read 2026-10-11.
   Each trout/char row now carries `group` (total, sub-caps, lifted kinds, one sentence: `says`) and
   `elsewhere` (the kept-elsewhere line and its questions). The Vedder is one part (mouth → Vedder Crossing).
   The Vedder May 15 rows come from the data (F13).

   Two design lines remain. The data still lacks them; both carry src:'design' and a page cite:
   - Fly-only windows on the open-days strip (p.15, p.22). The strip in the data has only open and closed.
   - "(Mile 66)" after the Cowichan's CNR Trestle (p.15). The data names it "CNR Trestle" only.
   See PARITY.md. */
var V9 = {};
(function(){
  const D = (t, cite) => ({t, src:'design', cite});
  const V = WATERS.vedder;

  /* ---------- Vedder: one part now ---------- */
  V.parts = [Object.assign(V.parts[0], {n:5})];
  V.frames = V.frames.filter(f => f.p === 0);
  const reg2 = {says:'Shared with every Region 2 stream you fish today.'};
  function g25(g, says, ask, kindsHead){
    g.total = 4; g.big = '4'; g.unit = 'a day';
    g.says = says; g.elsewhere = {says:reg2.says, ask}; g.kindsHead = kindsHead; g.lead = null; g.qa = null;
    return g;
  }
  const oct = g25(vTrout(0),
    'Up to 4 trout and char a day, steelhead included; at most 2 from streams (hatchery rainbow trout 50 cm or less excepted); at most 1 Dolly Varden/bull trout or lake trout; at most 2 over 50 cm; at most 1 over 50 cm, not counting steelhead.',
    [['Kept 2 on another stream?','Keep 0 here, except hatchery rainbow trout 50 cm or less: up to 4 in all'],
     ['Kept some at a lake?','Those don’t count toward the 2 from streams, but stop at 4 for the whole day']], 'Each kind');
  V.frames[0].regs.groups = [oct];
  V.frames.push({p:0, a:101, b:430, regs:{groups:[oct], other:vOther}, gear:vGear});

  /* Vedder May 1–31 (F13) */
  const may = g25(vTrout(0),
    'Up to 4 trout and char a day, steelhead included; at most 2 from streams; at most 1 Dolly Varden/bull trout or lake trout; at most 2 over 50 cm; at most 1 over 50 cm, not counting steelhead.',
    [['Kept 2 on another stream?','Keep 0 here'],['Kept some at a lake?','Those don’t count toward the 2 from streams, but stop at 4 for the whole day']],
    'Each kind · every fish counts toward the 2');
  const byName = n => may.kinds.find(k => k.n === n);
  const relMay = {verb:'Release', v:0, rel:'Release every one until May 31', note:'Then the whole water is closed Jun 1–30. You can keep them from Jul 1.'};
  may.kinds = [
    Object.assign({}, byName('Brook, brown and cutthroat trout'), {n:'Brook and brown trout'}),
    byName('Dolly Varden, bull trout and lake trout'),
    Object.assign({}, byName('Rainbow trout'), relMay, {chips:[], note:relMay.note + ' Rainbows over 50 cm count as steelhead.'}),
    byName('Steelhead'),
    Object.assign({n:'Cutthroat trout', chips:[]}, relMay, {ladder:null})];
  const KC = window.KCODES_EXTRA = {'Brook and brown trout':['EB','GB'], 'Cutthroat trout':['CT'], 'Brown and cutthroat trout':['GB','CT']};
  const mayGear = Object.assign({}, vGear, {
    tiles:{licence:vGear.tiles.licence, line:{v:'Single barbless hook', s:'1 line · Fly fishing only: no float or sinker'},
      bait:{v:'Bait ban', s:'Lures and flies only'}, ways:{v:'Angling', s:'Nothing else'}, always:vGear.tiles.always},
    lines:vGear.lines.filter(([l]) => l !== 'LURES' && l !== 'WEIGHT ON THE LINE'),
    bait:[['n','Worms','bait ban'],['n','Roe (fish eggs)','bait ban'],['n','Water insects and crayfish','bait ban'],['n','Fish or fish parts','bait ban']],
    baitNote:'No natural bait of any kind while the ban runs.',
    ways:{yes:[['Angling (rod and line)','fly fishing only: nothing but the fly on the line, no float, sinker or attractor']], no:['Set lines','Spear or bow','Crayfish traps','Nets','Snagging','Chumming']}});
  V.frames.push({p:0, a:501, b:531, regs:{groups:[may], other:vOther}, gear:mayGear});
  V.parts[0].strip = [['k',120],['f',31],['c',30],['k',184]];
  V.parts[0].fwin = D('fly fishing only, bait ban','p.22');

  /* ---------- Kootenay LWA: group + elsewhere from the data ---------- */
  WATERS.klwa.frames.forEach(f => { const g = f.regs.groups[0];
    g.total = 2; g.says = 'Up to 2 trout and char a day; at most 1 Dolly Varden/bull trout.';
    g.elsewhere = {says:'This lake’s own limit. Fish you kept elsewhere today count toward it too.', ask:[]}; g.lead = null; });

  /* ---------- Cowichan ---------- */
  const C = WATERS.cowichan, P = C.parts;
  C.mapKey = [0,2,3,4,1];
  C.short = ['Mouth → CNR Trestle','CNR Trestle → CNR Bridge','CNR Bridge → Greendale','Greendale → lake outlet','Side channel'];
  P[1].strip = [['k',184],['c',59],['f',76],['k',46]];
  P[2].strip = [['f',184],['c',59],['f',122]];
  P[3].strip = [['c',105],['k',79],['c',59],['f',75],['c',47]];
  P[4].strip = [['k',184],['c',59],['k',122]];
  P[1].fwin = D('fly fishing only','p.15'); P[2].fwin = D('fly fishing only (all year)','p.15'); P[3].fwin = D('fly fishing only','p.15');
  C.split = {at:'CNR Trestle', atNote:D('Mile 66','p.15'), below:0, above:1};
  const cTrout = {t:'Trout', scope:'ALL REGION 1, EXCLUDING HAIDA GWAII STREAMS', total:4, big:'4', unit:'a day',
    says:'Up to 4 trout a day, steelhead included; at most 2 from streams; at most 2 over 50 cm; at most 1 over 50 cm, not counting steelhead.',
    elsewhere:{says:'Shared with every Region 1, excluding Haida Gwaii stream you fish today.', ask:[['Kept 2 on another stream?','Keep 0 here'],['Kept some at a lake?','Those don’t count toward the 2 from streams, but stop at 4 for the whole day']]},
    every:[{h:'<b>Only 1 fish over 50 cm in total</b> (steelhead don’t count)'}], note:'Steelhead are known to be in this water.',
    kindsHead:'Each kind · every fish counts toward the 2',
    kinds:[{n:'Brown and cutthroat trout', verb:'Keep up to', v:2, chips:['any size','hatchery only'], note:'Only 1 over 50 cm.'},
           {n:'Rainbow trout', verb:'Keep up to', v:2, chips:['up to 50 cm','hatchery only'], note:'Rainbows over 50 cm count as steelhead.'},
           {n:'Steelhead', verb:'Keep up to', v:2, chips:['over 50 cm','hatchery only'], note:'Under 50 cm, it’s a rainbow trout.', extras:['10 a year','Record it on your licence']}]};
  const charSay = x => `You can fish for them, but release every one. No keeping at any time of year.${x}`;
  const cOther = x => [
    ['Char',{word:'release', cls:'rel', say:charSay(x), fish:['EB','DV','LT']}],
    ['Kokanee',{word:'release', cls:'rel', fish:['KO']}],
    ['Bass',{word:'no limit', cls:'keep', big:'∞', unit:'No daily limit', fish:['LMB','SMB']}],
    ['Yellow perch',{word:'no limit', cls:'keep', big:'∞', unit:'No daily limit', fish:['YP']}],
    ['White sturgeon',{word:'closed', cls:'closed', fish:['WSG']}],
    ['Crayfish',{word:'25 a day', cls:'keep', big:'25', unit:'a day', fish:[]}],
    ['Protected species',{word:'closed', cls:'closed', say:'Don’t fish for them. If one bites, carefully release it.', which:PROT12, fish:['CULTUS_LAKE_SCULPIN','NOOKSACK_DACE','SALISH_SUCKER']}]];
  const cGear = fly => ({
    tiles:{licence:{v:'Basic licence', s:'+ steelhead stamp and 1 more if needed'}, line:{v:'Single barbless hook', s: fly ? '1 line · Fly fishing only: no float or sinker' : '1 line'},
      bait:{v:'Bait ban', s:'Lures and flies only'}, ways: fly ? {v:'Angling', s:'Nothing else'} : {v:'Angling', s:'Also crayfish traps'}, always:{v:'Handling fish', s:'21 rules that apply everywhere'}},
    need:[DOC.basic, DOC.st, DOC.salmon], record:'Keep an adult chinook or hatchery steelhead? <b>Carry your paper licence</b> (16 and over) and record it right away.',
    where:[SRC_HERE, SRC_SALMON, SRC_RECORD],
    lines:[['LINES','1','','Province'],...LINE_COMMON,['HOOK POINTS','Single point only (no trebles)','on streams','Region'],['BARBS','Barbless only','on streams · pinch barbs flat or buy barbless','Region'], ...(fly ? [] : LURES_WEIGHT)],
    bait:[['n','Worms','bait ban'],['n','Roe (fish eggs)','bait ban'],['n','Water insects and crayfish','bait ban'],['n','Fish or fish parts','bait ban']],
    baitNote:'No natural bait of any kind while the ban runs.',
    ways: fly ? {yes:[['Angling (rod and line)','fly fishing only: nothing but the fly on the line, no float, sinker or attractor']], no:['Set lines','Spear or bow','Crayfish traps','Nets','Snagging','Chumming']}
              : {yes:[ANGLING, TRAPS], no:WAYS_NO}});
  [1,2].forEach(p => C.frames.push({p, a:901, b:1114, regs:{groups:[cTrout], other:cOther(' The whole water is closed Jul 4–Aug 31.')}, gear:cGear(true)}));
  C.frames.push({p:3, a:901, b:1114, regs:{groups:[cTrout], other:cOther(' The whole water is closed Nov 15–Apr 15. The whole water is closed Jul 4–Aug 31.')}, gear:cGear(true)});
  C.frames.push({p:4, a:901, b:1231, regs:{groups:[cTrout], other:cOther(' The whole water is closed Jul 4–Aug 31.')}, gear:cGear(false)});

  /* ---------- under 16, both residency outcomes: the data's words ---------- */
  V9.u16 = [{who:'B.C. resident', head:U16.head, body:U16.body},
            {who:'Not a B.C. resident', head:'Nothing to buy: you’re exempt — with a licensed adult', body:OTHERS[0].replace(/^For under 16, canadian, other province or from outside Canada: /,'')}];
})();
