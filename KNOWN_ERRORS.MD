Kettle river is missing segment - This is because of 500m zone buffer it gets some of the segments but not all there...
Similkameen River cut off early? - this is because we only match in the zone... need to figure out how to handle this more generally. 
 - this should be fixable by making it so if a stream (same watershed) is represented in a different zone with regulations but not in another zone then it should get the combined regulations from all zones that the stream has regulations in maybe? 
chilliwack lake doesnt get rid of stream segments
Refactor geo exporter to make it human readable and vetted. 
make sure tributaries enrichment only gets ributaries of provided edges does not travel up mainstem giving more then we want...


another issue - not a tributary of chealhis but instead one of pit river... think same issue as the it travels up mainstem instead of just regs provided
===
Morris Creek
Listed as: MORRIS CREEK
REGULATIONS
CHEHALIS RIVER
===

why is this entry 

====
Kootenay River
Listed as:
KOOTENAY RIVER (downstream of Idaho border)
KOOTENAY RIVER (upstream of Koocanusa Reservoir)
===

a tributary of columbia lake? splitting the river into 2 segments...s

because of a small edge type 2300 connector (secondary channel canal...)

python -m fwa_modules.tools.diagnose_lake_stream_exclusion --linear-id 700740026

python -m fwa_modules.tools.diagnose_lake_stream_exclusion --waterbody-key 329083341

  Stream #70:
    Linear ID: 700741702
    GNIS Name:
    Blue Line Key: 356237819
    Stream Order: 2
    Stream Magnitude: 2
    Edge Type: 1450
    Length (m): 476.73

  Stream #71:
    Linear ID: 700738245
    GNIS Name: Paleface Creek
    Blue Line Key: 356358165
    Stream Order: 4
    Stream Magnitude: 119
    Edge Type: 1450
    Length (m): 600.01

  Stream #72:
    Linear ID: 700733317
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 372
    Edge Type: 1200
    Length (m): 114.06

  Stream #73:
    Linear ID: 700733998
    GNIS Name:
    Blue Line Key: 356338773
    Stream Order: 2
    Stream Magnitude: 2
    Edge Type: 1450
    Length (m): 701.07

  Stream #74:
    Linear ID: 700735061
    GNIS Name:
    Blue Line Key: 356337720
    Stream Order: 2
    Stream Magnitude: 2
    Edge Type: 1450
    Length (m): 699.87

  Stream #75:
    Linear ID: 700738552
    GNIS Name:
    Blue Line Key: 356128287
    Stream Order: 1
    Stream Magnitude: 1
    Edge Type: 1450
    Length (m): 645.94

  Stream #76:
    Linear ID: 700733607
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 365
    Edge Type: 1200
    Length (m): 90.52

  Stream #77:
    Linear ID: 700740026
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 173
    Edge Type: 1200
    Length (m): 664.22

  Stream #78:
    Linear ID: 700739263
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 175
    Edge Type: 1200
    Length (m): 76.13

  Stream #79:
    Linear ID: 700733762
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 362
    Edge Type: 1200
    Length (m): 116.91

  Stream #80:
    Linear ID: 700736305
    GNIS Name:
    Blue Line Key: 356256137
    Stream Order: 2
    Stream Magnitude: 2
    Edge Type: 1450
    Length (m): 677.00

  Stream #81:
    Linear ID: 700738615
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 180
    Edge Type: 1200
    Length (m): 57.78

  Stream #82:
    Linear ID: 700736102
    GNIS Name:
    Blue Line Key: 356289142
    Stream Order: 1
    Stream Magnitude: 1
    Edge Type: 1450
    Length (m): 678.79

  Stream #83:
    Linear ID: 700737109
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 320
    Edge Type: 1200
    Length (m): 156.39

  Stream #84:
    Linear ID: 700733259
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 375
    Edge Type: 1200
    Length (m): 92.50

  Stream #85:
    Linear ID: 700741933
    GNIS Name:
    Blue Line Key: 356306032
    Stream Order: 2
    Stream Magnitude: 4
    Edge Type: 1450
    Length (m): 475.66

  Stream #86:
    Linear ID: 700733253
    GNIS Name: Cupola Creek
    Blue Line Key: 356332374
    Stream Order: 3
    Stream Magnitude: 15
    Edge Type: 1450
    Length (m): 768.59

  Stream #87:
    Linear ID: 700737921
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 305
    Edge Type: 1200
    Length (m): 299.58

  Stream #88:
    Linear ID: 700738246
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 301
    Edge Type: 1200
    Length (m): 55.25

  Stream #89:
    Linear ID: 700733795
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 361
    Edge Type: 1200
    Length (m): 42.13

  Stream #90:
    Linear ID: 700742034
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 4
    Stream Magnitude: 55
    Edge Type: 1200
    Length (m): 86.30

  Stream #91:
    Linear ID: 700738939
    GNIS Name:
    Blue Line Key: 356251564
    Stream Order: 1
    Stream Magnitude: 1
    Edge Type: 1450
    Length (m): 591.15

  Stream #92:
    Linear ID: 700739138
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 177
    Edge Type: 1200
    Length (m): 134.19

  Stream #93:
    Linear ID: 700733694
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 363
    Edge Type: 1200
    Length (m): 215.13

  Stream #94:
    Linear ID: 700738381
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 182
    Edge Type: 1200
    Length (m): 129.19

  Stream #95:
    Linear ID: 700733261
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 390
    Edge Type: 1200
    Length (m): 134.12

  Stream #96:
    Linear ID: 700739207
    GNIS Name:
    Blue Line Key: 356120283
    Stream Order: 1
    Stream Magnitude: 1
    Edge Type: 1450
    Length (m): 619.59

  Stream #97:
    Linear ID: 700734169
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 348
    Edge Type: 1200
    Length (m): 69.65

  Stream #98:
    Linear ID: 700734168
    GNIS Name:
    Blue Line Key: 356263644
    Stream Order: 2
    Stream Magnitude: 2
    Edge Type: 1450
    Length (m): 700.71

  Stream #99:
    Linear ID: 700734301
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 345
    Edge Type: 1200
    Length (m): 61.98

  Stream #100:
    Linear ID: 700738171
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 303
    Edge Type: 1200
    Length (m): 56.48

  Stream #101:
    Linear ID: 700733489
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 367
    Edge Type: 1200
    Length (m): 65.64

  Stream #102:
    Linear ID: 700733313
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 395
    Edge Type: 1200
    Length (m): 579.14

  Stream #103:
    Linear ID: 700738297
    GNIS Name:
    Blue Line Key: 356208766
    Stream Order: 1
    Stream Magnitude: 1
    Edge Type: 1450
    Length (m): 615.70

  Stream #104:
    Linear ID: 700737191
    GNIS Name:
    Blue Line Key: 356068256
    Stream Order: 1
    Stream Magnitude: 1
    Edge Type: 1450
    Length (m): 699.78

  Stream #105:
    Linear ID: 700738940
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 178
    Edge Type: 1200
    Length (m): 62.88

  Stream #106:
    Linear ID: 700733573
    GNIS Name:
    Blue Line Key: 356300515
    Stream Order: 1
    Stream Magnitude: 1
    Edge Type: 1450
    Length (m): 410.99

  Stream #107:
    Linear ID: 700734300
    GNIS Name:
    Blue Line Key: 356211532
    Stream Order: 2
    Stream Magnitude: 2
    Edge Type: 1450
    Length (m): 725.89

  Stream #108:
    Linear ID: 700736354
    GNIS Name:
    Blue Line Key: 356237321
    Stream Order: 2
    Stream Magnitude: 3
    Edge Type: 1450
    Length (m): 659.31

  Stream #109:
    Linear ID: 700740506
    GNIS Name:
    Blue Line Key: 356291529
    Stream Order: 1
    Stream Magnitude: 1
    Edge Type: 1450
    Length (m): 552.02

  Stream #110:
    Linear ID: 700740044
    GNIS Name:
    Blue Line Key: 356301616
    Stream Order: 4
    Stream Magnitude: 19
    Edge Type: 1450
    Length (m): 550.49

  Stream #111:
    Linear ID: 700733258
    GNIS Name: Frances Creek
    Blue Line Key: 356196175
    Stream Order: 2
    Stream Magnitude: 2
    Edge Type: 1450
    Length (m): 810.92

  Stream #112:
    Linear ID: 700740208
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 154
    Edge Type: 1200
    Length (m): 111.44

  Stream #113:
    Linear ID: 700734121
    GNIS Name:
    Blue Line Key: 356291994
    Stream Order: 2
    Stream Magnitude: 7
    Edge Type: 1450
    Length (m): 693.89

  Stream #114:
    Linear ID: 700733909
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 360
    Edge Type: 1200
    Length (m): 138.66

  Stream #115:
    Linear ID: 700736355
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 327
    Edge Type: 1200
    Length (m): 152.61

  Stream #116:
    Linear ID: 700742138
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 4
    Stream Magnitude: 52
    Edge Type: 1200
    Length (m): 76.41

  Stream #117:
    Linear ID: 700733488
    GNIS Name:
    Blue Line Key: 356186752
    Stream Order: 1
    Stream Magnitude: 1
    Edge Type: 1450
    Length (m): 722.36

  Stream #118:
    Linear ID: 700741687
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 4
    Stream Magnitude: 62
    Edge Type: 1200
    Length (m): 43.21

  Stream #119:
    Linear ID: 700736719
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 324
    Edge Type: 1200
    Length (m): 269.73

  Stream #120:
    Linear ID: 700738553
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 181
    Edge Type: 1200
    Length (m): 131.89

  Stream #121:
    Linear ID: 700734364
    GNIS Name: Chilliwack River
    Blue Line Key: 380887781
    Stream Order: 5
    Stream Magnitude: 343
    Edge Type: 1200
    Length (m): 69.67

────────────────────────────────────────────────────────────────────────────────
REGULATION MAPPING STATUS
────────────────────────────────────────────────────────────────────────────────

  700734794 ():
    Has regulations: False

  700733559 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700734674 ():
    Has regulations: False

  700733811 ():
    Has regulations: False

  700733908 ():
    Has regulations: False

  700741907 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700737728 ():
    Has regulations: False

  700736077 ():
    Has regulations: False

  700735915 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700740786 ():
    Has regulations: False

  700738614 ():
    Has regulations: False

  700733452 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700734627 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700738169 ():
    Has regulations: False

  700738121 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700734197 ():
    Has regulations: False

  700738604 ():
    Has regulations: False

  700738370 ():
    Has regulations: False

  700733558 ():
    Has regulations: False

  700740207 (Depot Creek):
    Has regulations: False

  700734405 ():
    Has regulations: False

  700737962 (Paleface Creek):
    Has regulations: False

  700739171 ():
    Has regulations: False

  700736202 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733270 ():
    Has regulations: False

  700734738 ():
    Has regulations: False

  700733794 ():
    Has regulations: False

  700734795 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700740669 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733271 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700736932 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700739002 ():
    Has regulations: False

  700733262 ():
    Has regulations: False

  700734269 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733748 ():
    Has regulations: False

  700733294 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733999 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700736103 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700742251 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700741178 ():
    Has regulations: False

  700736865 ():
    Has regulations: False

  700738404 ():
    Has regulations: False

  700739421 ():
    Has regulations: False

  700734675 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700741699 ():
    Has regulations: False

  700733356 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700739172 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700738865 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700741622 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733606 ():
    Has regulations: False

  700742273 ():
    Has regulations: False

  700741925 ():
    Has regulations: False

  700740353 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700734617 ():
    Has regulations: False

  700737963 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700737033 ():
    Has regulations: False

  700734739 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700738190 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700734084 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700742035 ():
    Has regulations: False

  700737108 ():
    Has regulations: False

  700741056 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700741859 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700734257 ():
    Has regulations: False

  700734122 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700734756 ():
    Has regulations: False

  700733451 ():
    Has regulations: False

  700737192 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700737583 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700741702 ():
    Has regulations: False

  700738245 (Paleface Creek):
    Has regulations: False

  700733317 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733998 ():
    Has regulations: False

  700735061 ():
    Has regulations: False

  700738552 ():
    Has regulations: False

  700733607 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700740026 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700739263 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733762 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700736305 ():
    Has regulations: False

  700738615 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700736102 ():
    Has regulations: False

  700737109 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733259 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700741933 ():
    Has regulations: False

  700733253 (Cupola Creek):
    Has regulations: False

  700737921 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700738246 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733795 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700742034 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700738939 ():
    Has regulations: False

  700739138 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733694 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700738381 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733261 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700739207 ():
    Has regulations: False

  700734169 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700734168 ():
    Has regulations: False

  700734301 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700738171 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733489 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733313 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700738297 ():
    Has regulations: False

  700737191 ():
    Has regulations: False

  700738940 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733573 ():
    Has regulations: False

  700734300 ():
    Has regulations: False

  700736354 ():
    Has regulations: False

  700740506 ():
    Has regulations: False

  700740044 ():
    Has regulations: False

  700733258 (Frances Creek):
    Has regulations: False

  700740208 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700734121 ():
    Has regulations: False

  700733909 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700736355 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700742138 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700733488 ():
    Has regulations: False

  700741687 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700736719 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700738553 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

  700734364 (Chilliwack River):
    Has regulations: True
    Regulation count: 6
    Regulation IDs: reg_0249_rule0, reg_0249_rule1, reg_0249_rule2, reg_0249_rule3, reg_0249_rule4...

────────────────────────────────────────────────────────────────────────────────
MERGED GROUPS ANALYSIS
────────────────────────────────────────────────────────────────────────────────

Found 0 groups containing streams from this lake:

────────────────────────────────────────────────────────────────────────────────
LINKED WATERBODY KEYS
────────────────────────────────────────────────────────────────────────────────

Waterbody key 329083341 in linked_waterbody_keys: False
Total linked waterbody keys: 909
Sample linked keys: 329354793, 329459219, 329225688, 329060857, 329524078, 329563842, 329321033, 329026676, 212095241, 329537190... 
