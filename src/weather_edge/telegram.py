"""Telegram notification helpers.

Required env vars (both must be set; if either is missing all calls are no-ops):
  TELEGRAM_BOT_TOKEN   Bot token from @BotFather
  TELEGRAM_CHAT_ID     Your personal chat ID (send /start to the bot, then
                       fetch https://api.telegram.org/bot<TOKEN>/getUpdates)
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Callable

_logger = logging.getLogger(__name__)


def _creds() -> tuple[str, str] | None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        return token, chat_id
    return None


def send(text: str) -> None:
    """Send a message to the configured Telegram chat. No-op if not configured."""
    creds = _creds()
    if not creds:
        return
    token, chat_id = creds
    try:
        import httpx
        httpx.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as exc:
        _logger.warning("Telegram send failed: %s", exc)


def start_command_listener(get_status: Callable[[], str]) -> None:
    """Start a daemon thread that answers /status commands sent to the bot."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        return

    def _listen() -> None:
        offset = 0
        while True:
            try:
                import httpx
                resp = httpx.get(
                    f"https://api.telegram.org/bot{token}/getUpdates",
                    params={"offset": offset, "timeout": 30},
                    timeout=40,
                )
                for update in resp.json().get("result", []):
                    offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    cmd = msg.get("text", "").strip().lower().split("@")[0]
                    if cmd == "/status":
                        chat = msg["chat"]["id"]
                        httpx.post(
                            f"https://api.telegram.org/bot{token}/sendMessage",
                            json={"chat_id": chat, "text": get_status(), "parse_mode": "HTML"},
                            timeout=10,
                        )
            except Exception as exc:
                _logger.warning("Telegram listener error: %s", exc)
                time.sleep(5)

    threading.Thread(target=_listen, daemon=True, name="telegram-listener").start()
    _logger.info("Telegram /status listener started")
