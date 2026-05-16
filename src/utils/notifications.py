"""Slack + Telegram alerting (async, fire-and-forget).

`send_alert(message, severity)` is the entry point.
- `critical` severity fans out to BOTH Slack and Telegram.
- `info` / `warning` go to Slack only.

A failed POST is logged at WARNING and swallowed — alerting is best-effort
and must never block or crash the producer.

Credentials are read from env vars at send time:
- SLACK_WEBHOOK_URL
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Literal

import httpx

logger = logging.getLogger(__name__)

Severity = Literal["info", "warning", "critical"]

SLACK_ENV = "SLACK_WEBHOOK_URL"
TELEGRAM_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ENV = "TELEGRAM_CHAT_ID"


def truncate_wallet(addr: str | None) -> str:
    if not addr or len(addr) < 12:
        return addr or "?"
    return f"{addr[:6]}...{addr[-4:]}"


def format_anomaly_alert(
    market_title: str,
    wallet: str | None,
    composite_score: float,
    breakdown: dict[str, float],
) -> str:
    components = ", ".join(f"{k}={v:.2f}" for k, v in breakdown.items())
    return (
        f"*Insider anomaly:* {market_title}\n"
        f"Wallet: `{truncate_wallet(wallet)}`\n"
        f"Composite: *{composite_score:.2f}* / 10\n"
        f"Components: {components}"
    )


async def _post_slack(message: str) -> None:
    url = os.environ.get(SLACK_ENV)
    if not url:
        logger.debug("%s not set; skipping Slack", SLACK_ENV)
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(url, json={"text": message})
            if r.status_code >= 400:
                logger.warning("Slack POST %d: %s", r.status_code, r.text[:200])
    except httpx.HTTPError as e:
        logger.warning("Slack POST failed: %s", e)


async def _post_telegram(message: str) -> None:
    token = os.environ.get(TELEGRAM_TOKEN_ENV)
    chat_id = os.environ.get(TELEGRAM_CHAT_ENV)
    if not token or not chat_id:
        logger.debug("Telegram credentials not set; skipping Telegram")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(
                url,
                json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"},
            )
            if r.status_code >= 400:
                logger.warning("Telegram POST %d: %s", r.status_code, r.text[:200])
    except httpx.HTTPError as e:
        logger.warning("Telegram POST failed: %s", e)


async def send_alert(message: str, severity: Severity = "info") -> None:
    tasks: list[asyncio.Task] = [asyncio.create_task(_post_slack(message))]
    if severity == "critical":
        tasks.append(asyncio.create_task(_post_telegram(message)))
    await asyncio.gather(*tasks, return_exceptions=True)
