-- Built AFTER the rows, so each index is constructed once rather than maintained per insert.
CREATE INDEX alias_by_text   ON alias(alias);
CREATE INDEX item_by_name    ON item(name);
CREATE INDEX item_by_parent  ON item(part_of) WHERE part_of IS NOT NULL;
CREATE INDEX section_by_item ON item_section(ord);
CREATE INDEX item_by_section ON item_section(sid);
-- Both new tables are WITHOUT ROWID and keyed the way they are read — section -> set, then
-- set -> rules — so the primary keys ARE the indexes and neither needs another. The one
-- direction that has no key is "which sections does this rule cover", which the app never
-- asks: a rule is reached FROM a section, never swept for. Adding it would cost more than
-- the whole ruleset table.
CREATE INDEX ruleset_by_entry ON ruleset(entry_id, rule_id);
CREATE INDEX release_by_name ON release(name);
CREATE INDEX chart_by_item   ON chart(item_id);
-- gauge_clim is a ROWID table (see schema.sql: a WITHOUT ROWID blob spills to overflow
-- pages and more than doubles), so the lookup key has to be a real index.
CREATE UNIQUE INDEX clim_by_station ON gauge_clim(station, parameter);
CREATE INDEX place_by_osm    ON place(osm);
CREATE INDEX place_by_name   ON place(name);
