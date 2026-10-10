-- THE REGS STORE (`regs.sqlite`, format regs-store/1, phase S0). ONE definition of the format.
--
-- Built by `pipeline/deliver/store/build.py` from the answers file (answers/2) and the export pair it
-- pairs with; `decode.py` rebuilds `ui-rules-answers.json` from it BYTE-IDENTICAL and the export
-- subset it carries equal by value. Integer keys throughout. A table keyed by one INTEGER PRIMARY KEY
-- is a rowid table (the key IS the b-tree key; SQLite advises rowid tables for rows holding blobs);
-- a table with a composite or text key is WITHOUT ROWID.
--
-- A BLOB column `j` holds one JSON value as compact UTF-8 (`json.dumps(v, ensure_ascii=False,
-- separators=(",", ":"))`, key order kept). Under `meta.blob_codec` 'deflate' / 'zdict' a blob may
-- instead start with byte 0x01 (raw deflate, wbits -15) or 0x02 (raw deflate with the table's
-- dictionary in `zdict`); a JSON text never starts with either byte.

-- digests, format, layout (the key order of the answers file and its sections)
CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID;

-- interned strings (labels, run sentences, place headings, hints, item ids, kinds, lists as JSON)
CREATE TABLE str (id INTEGER PRIMARY KEY, text TEXT NOT NULL);

-- whole JSON values read once (spec, schemas, fish, moments, glossary, the sections' static
-- tables, licences, species, conduct …): k = 'answers.<key>' | '<section>.<key>' | 'export.<key>'
CREATE TABLE static (k TEXT PRIMARY KEY, j BLOB NOT NULL) WITHOUT ROWID;

-- per-table deflate dictionaries (meta.blob_codec = 'zdict' only)
CREATE TABLE zdict (tbl TEXT PRIMARY KEY, d BLOB NOT NULL) WITHOUT ROWID;

-- the answers' interned segment start lists and segment-moment lists (JSON arrays of integers)
CREATE TABLE seglist (seg INTEGER PRIMARY KEY, starts TEXT NOT NULL);
CREATE TABLE segmom  (mom INTEGER PRIMARY KEY, ix TEXT NOT NULL);

-- ONE ROW PER ANSWERS KEY. akey = the answers file's key index + 1 (0 is "no key" in section_akey).
CREATE TABLE akey (
    akey            INTEGER PRIMARY KEY CHECK (akey >= 1),
    ruleset         INTEGER NOT NULL,
    licensing_set   INTEGER,
    steelhead_water INTEGER NOT NULL CHECK (steelhead_water IN (0, 1)),
    steelhead       INTEGER CHECK (steelhead IN (1, 2)),          -- 1 known, 2 possible (the bundle's code)
    steelhead_rules INTEGER NOT NULL CHECK (steelhead_rules IN (0, 1)),
    province_except INTEGER NOT NULL REFERENCES str(id),          -- JSON list text
    home_region     INTEGER NOT NULL REFERENCES str(id),          -- JSON list text
    kind            INTEGER NOT NULL REFERENCES str(id),
    tidal           INTEGER NOT NULL CHECK (tidal IN (0, 1)),
    seg             INTEGER NOT NULL REFERENCES seglist(seg),
    mom             INTEGER REFERENCES segmom(mom),
    nseg            INTEGER NOT NULL CHECK (nseg >= 1));

-- akey x segment -> one frame id per section (the answers' own interned indexes). A segment index s
-- already separates the moments of a day (answers 2.1: a start repeats once per moment group).
CREATE TABLE cell (
    akey    INTEGER NOT NULL REFERENCES akey(akey),
    s       INTEGER NOT NULL CHECK (s >= 0),
    ladder  INTEGER NOT NULL REFERENCES ladder_frame(id),
    rows    INTEGER NOT NULL REFERENCES rows_frame(id),
    gear    INTEGER NOT NULL REFERENCES gear_frame(id),
    licence INTEGER NOT NULL REFERENCES licence_frame(id),
    display INTEGER NOT NULL REFERENCES display_frame(id),
    PRIMARY KEY (akey, s)) WITHOUT ROWID;

-- THE DEDUPLICATED FRAME FAMILIES, id = the answers file's index into that table
CREATE TABLE ladder_verdict (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
CREATE TABLE ladder_frame   (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
CREATE TABLE rows_decided   (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
CREATE TABLE rows_row       (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
CREATE TABLE rows_frame     (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
CREATE TABLE gear_frame     (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
CREATE TABLE licence_hold   (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
CREATE TABLE licence_answer (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
CREATE TABLE licence_doc    (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
CREATE TABLE licence_frame  (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
CREATE TABLE display_frame  (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
-- display's static per-rule table, aligned with the export's rules (rix)
CREATE TABLE display_rule   (id INTEGER PRIMARY KEY, j BLOB NOT NULL);

-- THE NAMED WATERS. item_ord = the bundle's item.ord (the answers' and the export's water order).
-- `shown` 1: the water is in display.waters (then `picker` / `unresolved` are its). The export's
-- words: `name`, `kind`, `sections`, `outside_bc`, `entries` (entry refs, JSON), `steelhead`
-- (1 known, 2 possible, NULL absent) and `x` (the rest, JSON, NULL when there is none).
CREATE TABLE item (
    item_ord   INTEGER PRIMARY KEY,
    item_id    INTEGER NOT NULL REFERENCES str(id),
    shown      INTEGER NOT NULL CHECK (shown IN (0, 1)),
    picker     INTEGER REFERENCES picker(id),
    unresolved TEXT,
    name       TEXT NOT NULL,
    kind       INTEGER NOT NULL REFERENCES str(id),
    sections   INTEGER NOT NULL,
    outside_bc INTEGER NOT NULL,
    entries    TEXT NOT NULL,
    steelhead  INTEGER CHECK (steelhead IN (1, 2)),
    x          BLOB);
CREATE TABLE picker (id INTEGER PRIMARY KEY, j BLOB NOT NULL);

-- THE PARTS, columnar (replaces display.waters[item].parts): one row per export part.
-- akey NULL: the part has no rule set (answers `parts[item][i]` null). `ord` NULL: display's part is
-- null (outside B.C.); else ord/label/runs/place/hint/km/closed_all_year/paper_licence are display's.
-- `xp` the part's export flags (interned JSON), `sections` its export count, `xruns` its export runs.
CREATE TABLE part (
    item_ord        INTEGER NOT NULL REFERENCES item(item_ord),
    part_ix         INTEGER NOT NULL CHECK (part_ix >= 0),
    akey            INTEGER REFERENCES akey(akey),
    ord             INTEGER,
    label_s         INTEGER REFERENCES str(id),
    runs_s          INTEGER REFERENCES str(id),
    place_s         INTEGER REFERENCES str(id),
    hint_s          INTEGER REFERENCES str(id),
    km              REAL,
    closed_all_year INTEGER CHECK (closed_all_year IN (0, 1)),
    paper_licence   TEXT,
    xp              INTEGER NOT NULL REFERENCES xpart(id),
    sections        INTEGER NOT NULL,
    xruns           INTEGER NOT NULL REFERENCES runs(id),
    PRIMARY KEY (item_ord, part_ix)) WITHOUT ROWID;
CREATE TABLE runs (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
CREATE TABLE xpart (id INTEGER PRIMARY KEY, j BLOB NOT NULL);

-- SECTION HANDLE -> AKEY, u16 little-endian, 4096 handles per block: handle h is at block h >> 12,
-- byte 2 * (h & 4095). 0 = no key yet (S0 fills every section a named part covers; S1 the rest).
-- An absent block is all zero.
CREATE TABLE section_akey (block INTEGER PRIMARY KEY, b BLOB NOT NULL CHECK (length(b) = 8192));

-- THE EXPORT WORDS the answers lack (what a page reads: page_data.py's slice). Rule / licensing ids
-- are `entry_id::rule_id` / `entry_id#record_id`, rebuilt from `j`. Rule sets hold rule / licensing
-- refs as integers.
CREATE TABLE rule    (rix INTEGER PRIMARY KEY, j BLOB NOT NULL);
CREATE TABLE lic     (lix INTEGER PRIMARY KEY, j BLOB NOT NULL);
CREATE TABLE entry   (e INTEGER PRIMARY KEY, id TEXT NOT NULL, j BLOB NOT NULL);
CREATE TABLE ruleset (set_id INTEGER PRIMARY KEY, j BLOB NOT NULL);
CREATE TABLE lset    (set_id INTEGER PRIMARY KEY, j BLOB NOT NULL);
