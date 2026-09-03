"""Date-window parsing (pipeline/regs/parsing/dates.py)."""

from pipeline.regs.parsing.dates import DateWindow, date_parse_errors, parse_date_window


def test_parse_range_variants():
    assert parse_date_window("Apr 1 - Jun 30") == DateWindow(4, 1, 6, 30)
    assert parse_date_window("Jan 1 to Dec 31") == DateWindow(1, 1, 12, 31)
    assert parse_date_window("Sept 1 – Oct 15") == DateWindow(9, 1, 10, 15)      # en-dash + Sept
    assert parse_date_window("April 1 through June 30") == DateWindow(4, 1, 6, 30)


def test_parse_single_date():
    assert parse_date_window("May 15") == DateWindow(5, 15, 5, 15)


def test_cross_year_window():
    w = parse_date_window("Nov 1 - Mar 31")
    assert w == DateWindow(11, 1, 3, 31) and w.crosses_year is True
    assert parse_date_window("Apr 1 - Jun 30").crosses_year is False


def test_invalid_dates_are_flagged():
    # bad day for the month, garbled month, or unparseable format
    assert parse_date_window("Jun 31") is None            # June has 30 days
    assert parse_date_window("Febtober 3") is None
    assert parse_date_window("sometime in spring") is None
    errs = date_parse_errors(["Apr 1 - Jun 30", "Jun 31"])
    assert len(errs) == 1 and "Jun 31" in errs[0]
