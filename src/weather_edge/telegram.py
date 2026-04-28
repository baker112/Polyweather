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


def start_command_listener(handlers: dict[str, Callable[[str], str]]) -> None:
    """Start a daemon thread that dispatches bot commands to handler functions.

    handlers: mapping of command string (e.g. "/status") to a callable that
    takes an args string (everything after the command, may be empty) and
    returns the reply text.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        return

    def _listen() -> None:
        import httpx
        offset = 0
        while True:
            try:
                resp = httpx.get(
                    f"https://api.telegram.org/bot{token}/getUpdates",
                    params={"offset": offset, "timeout": 30},
                    timeout=40,
                )
                if resp.status_code == 409:
                    # Another instance is already polling — back off and let it win.
                    _logger.warning("Telegram 409 Conflict: another bot instance is polling. Retrying in 60s.")
                    time.sleep(60)
                    continue
                if resp.status_code != 200:
                    _logger.warning("Telegram getUpdates HTTP %d — retrying in 15s", resp.status_code)
                    time.sleep(15)
                    continue
                for update in resp.json().get("result", []):
                    offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    text = msg.get("text", "").strip()
                    if not text:
                        continue
                    parts = text.split(None, 1)
                    cmd = parts[0].lower().split("@")[0]
                    args = parts[1].strip() if len(parts) > 1 else ""
                    if cmd in handlers:
                        chat = msg["chat"]["id"]
                        try:
                            reply = handlers[cmd](args)
                        except Exception as exc:
                            reply = f"Error running {cmd}: {exc}"
                        httpx.post(
                            f"https://api.telegram.org/bot{token}/sendMessage",
                            json={"chat_id": chat, "text": reply, "parse_mode": "HTML"},
                            timeout=10,
                        )
            except Exception as exc:
                _logger.warning("Telegram listener error: %s", exc)
                time.sleep(5)

    cmds = ", ".join(handlers)
    threading.Thread(target=_listen, daemon=True, name="telegram-listener").start()
    _logger.info("Telegram command listener started — commands: %s", cmds)
