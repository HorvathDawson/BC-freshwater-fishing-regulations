-- Built AFTER the rows, so each index is constructed once rather than maintained per insert.
CREATE INDEX alias_by_text   ON alias(alias);
CREATE INDEX item_by_name    ON item(name);
CREATE INDEX section_by_item ON item_section(item_id);
CREATE INDEX item_by_section ON item_section(section_id);
CREATE INDEX rule_by_scope   ON rule_section(scope_kind, scope_id);
CREATE INDEX rule_by_entry   ON rule_section(entry_id);
CREATE INDEX release_by_name ON release(name);
CREATE INDEX chart_by_item   ON chart(item_id);
CREATE INDEX water_by_place  ON place_water(place_id, km);
CREATE INDEX place_by_osm    ON place(osm);
CREATE INDEX place_by_name   ON place(name);
