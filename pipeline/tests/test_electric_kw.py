"""T209 (owner 2026-10-28): electric_only rules store the kW their words print; the words stay."""
from pipeline.regs.parsing import io
from pipeline.tools.electric_kw import fill_rule, kw_in_words


def _r(verbatim, **kw):
    return {"type": "vessel_rule", "aspect": "propulsion", "level": "electric_only",
            "verbatim": verbatim, **kw}


def test_reads_the_number_from_the_words():
    for w in ("Electric motor only - max 7.5 kW", "electric motor only (max 7.5 kW)",
              "electric motor only - max 7.5 kW, Oct 1-Apr 30", "Electric motor only - max. 5 kW"):
        assert kw_in_words(w) in (7.5, 5.0), w
    assert kw_in_words("Electric motor only") is None
    assert kw_in_words("max 7.5 kW here, max 10 kW there") is None


def test_fills_only_an_empty_electric_only_rule():
    r = _r("Electric motor only - max 7.5 kW")
    assert fill_rule(r) == 7.5 and r["max_power_kw"] == 7.5
    assert r["verbatim"] == "Electric motor only - max 7.5 kW"
    assert fill_rule(_r("Electric motor only - max 7.5 kW", max_power_kw=5.0)) is None
    assert fill_rule(_r("Electric motor only")) is None
    capped = {"type": "vessel_rule", "aspect": "propulsion", "level": "power_capped",
              "verbatim": "engine power restriction - max 7.5 kW (10 hp)", "max_power_kw": 7.5}
    assert fill_rule(capped) is None


def test_catalogue_holds_the_number_on_every_electric_only_rule():
    n = 0
    for path in io.entries_dir().glob("region-*.json"):
        for e in io.read_entryfile(path).values():
            for r in e.get("rules") or []:
                if r.get("type") == "vessel_rule" and r.get("level") == "electric_only":
                    n += 1
                    assert r.get("max_power_kw") == kw_in_words(r["verbatim"]), r.get("rule_id")
    assert n >= 162
