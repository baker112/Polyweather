"""Tests for Telegram send() retry logic."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _resp(status: int) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    return r


@pytest.fixture(autouse=True)
def _creds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")


def test_send_succeeds_immediately_on_200() -> None:
    """200 response → exactly one POST, no retry."""
    with patch("httpx.post", return_value=_resp(200)) as mock_post:
        import weather_edge.telegram as tg
        tg.send("hello")
    assert mock_post.call_count == 1


def test_send_retries_on_429_then_succeeds() -> None:
    """429 followed by 200 → two POSTs total."""
    with patch("httpx.post", side_effect=[_resp(429), _resp(200)]) as mock_post, \
         patch("time.sleep"):
        import weather_edge.telegram as tg
        tg.send("hello")
    assert mock_post.call_count == 2


def test_send_gives_up_after_four_attempts() -> None:
    """Four consecutive 429s → four POSTs then gives up without raising."""
    with patch("httpx.post", return_value=_resp(429)) as mock_post, \
         patch("time.sleep"):
        import weather_edge.telegram as tg
        tg.send("hello")
    assert mock_post.call_count == 4


def test_send_noop_when_not_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing env vars → no HTTP call made."""
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    with patch("httpx.post") as mock_post:
        import weather_edge.telegram as tg
        tg.send("hello")
    assert mock_post.call_count == 0
