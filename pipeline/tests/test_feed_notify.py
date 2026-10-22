"""Telling the OWNER once that an upstream update is due (owner 2026-10-22 late). Never an end user.

A notice from the feed index's `maintenance` list is sent the first time it appears, deduped by kind + latest in a
state file beside the feed output (never inside it: the feed directory is what gets synced to the public origin).
The URL comes from FEED_NOTIFY_URL; unset, nothing is sent and one console line says notifications are off. A failed
send is not recorded, so the next run retries it, and it never fails the feed. Every test injects the sender.
"""

from __future__ import annotations

import json

from pipeline.gauges.feed import notify

HYDAT = {"kind": "hydat", "since": "2026-10-22T21:00:00Z", "current": "20260717", "latest": "20261020",
         "action": "scripts/update-hydat.sh"}
ROSTER = {"kind": "roster", "since": "2026-10-22T21:00:00Z", "current": "data/source/bc_hydrometric_stations.json",
          "latest": "+08NM139 -08EG012", "action": "rm x && y"}


class Sender:
    def __init__(self, fail: bool = False):
        self.sent: list[tuple[str, str]] = []
        self.fail = fail

    def __call__(self, url: str, text: str) -> None:
        if self.fail:
            raise OSError("connection refused")
        self.sent.append((url, text))


def test_the_message_is_short_plain_text_with_the_command():
    assert notify.message(HYDAT) == (
        "HYDAT 20261020 is out; the gauge feed is on 20260717. Run: scripts/update-hydat.sh")
    assert notify.message(ROSTER) == (
        "ECCC's real-time station list differs from data/source/bc_hydrometric_stations.json "
        "(+08NM139 -08EG012). Run: rm x && y")


def test_the_state_file_sits_beside_the_feed_not_in_it(tmp_path):
    out = tmp_path / "feeds"
    assert notify.state_path(out) == tmp_path / "feeds.notified.json"


def test_sends_each_notice_once(tmp_path, capsys):
    out = tmp_path / "feeds"
    send = Sender()
    got = notify.send_due([HYDAT, ROSTER], out, url="https://ntfy.sh/x", sender=send)
    assert got == {"sent": ["hydat:20261020", "roster:+08NM139 -08EG012"], "failed": [], "off": False}
    assert send.sent == [("https://ntfy.sh/x", notify.message(HYDAT)), ("https://ntfy.sh/x", notify.message(ROSTER))]
    again = notify.send_due([HYDAT, ROSTER], out, url="https://ntfy.sh/x", sender=send)
    assert again == {"sent": [], "failed": [], "off": False}
    assert len(send.sent) == 2
    assert json.loads(notify.state_path(out).read_text()) == {"sent": ["hydat:20261020", "roster:+08NM139 -08EG012"]}


def test_a_new_latest_is_sent_again(tmp_path):
    out = tmp_path / "feeds"
    send = Sender()
    notify.send_due([HYDAT], out, url="u", sender=send)
    notify.send_due([dict(HYDAT, latest="20270115")], out, url="u", sender=send)
    assert [t for _, t in send.sent] == [notify.message(HYDAT), notify.message(dict(HYDAT, latest="20270115"))]


def test_a_failed_send_is_retried_next_run_and_never_raises(tmp_path, capsys):
    out = tmp_path / "feeds"
    got = notify.send_due([HYDAT], out, url="u", sender=Sender(fail=True))
    assert got == {"sent": [], "failed": ["hydat:20261020"], "off": False}
    assert not notify.state_path(out).exists()
    assert "notify: hydat:20261020 not sent (connection refused); retried next run" in capsys.readouterr().err
    send = Sender()
    assert notify.send_due([HYDAT], out, url="u", sender=send)["sent"] == ["hydat:20261020"]
    assert len(send.sent) == 1


def test_unset_url_sends_nothing_and_says_so(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("FEED_NOTIFY_URL", raising=False)
    send = Sender()
    got = notify.send_due([HYDAT], tmp_path / "feeds", sender=send)
    assert got == {"sent": [], "failed": [], "off": True}
    assert send.sent == []
    assert capsys.readouterr().out == "  notify: off (FEED_NOTIFY_URL unset); 1 notice in index.json\n"
    assert not notify.state_path(tmp_path / "feeds").exists()


def test_the_url_comes_from_the_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("FEED_NOTIFY_URL", "https://ntfy.sh/owner-topic")
    send = Sender()
    notify.send_due([HYDAT], tmp_path / "feeds", sender=send)
    assert send.sent == [("https://ntfy.sh/owner-topic", notify.message(HYDAT))]


def test_nothing_due_sends_nothing_even_when_off(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("FEED_NOTIFY_URL", raising=False)
    assert notify.send_due([], tmp_path / "feeds", sender=Sender()) == {"sent": [], "failed": [], "off": True}
    assert capsys.readouterr().out == ""


def test_an_unreadable_state_file_resends_rather_than_crashing(tmp_path):
    out = tmp_path / "feeds"
    notify.state_path(out).write_text("{not json")
    send = Sender()
    assert notify.send_due([HYDAT], out, url="u", sender=send)["sent"] == ["hydat:20261020"]


def test_the_body_shape_follows_the_url():
    # ntfy (and anything else): the text as the body. Discord and Slack webhooks take JSON.
    assert notify.body("https://ntfy.sh/topic", "hi") == (b"hi", "text/plain; charset=utf-8")
    assert notify.body("https://discord.com/api/webhooks/1/abc", "hi") == (b'{"content": "hi"}', "application/json")
    assert notify.body("https://hooks.slack.com/services/T/B/x", "hi") == (b'{"text": "hi"}', "application/json")
