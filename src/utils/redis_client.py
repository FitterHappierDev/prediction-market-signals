"""Redis client wrapper for streams and key/value caching (async).

Uses redis-py's asyncio interface. Streams use XADD with MAXLEN ~100000
(approximate trimming — more efficient than exact). Consumer groups give
at-least-once delivery; callers must be idempotent.

On Redis disconnect: log a warning and continue. Stream messages are
ephemeral signals — losing one is preferable to blocking the producer.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as redis

STREAM_MAXLEN = 100_000

logger = logging.getLogger(__name__)


class RedisClient:
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0) -> None:
        self._r: redis.Redis = redis.Redis(
            host=host, port=port, db=db, decode_responses=True
        )

    async def publish_to_stream(
        self, stream: str, payload: dict[str, Any]
    ) -> str | None:
        try:
            return await self._r.xadd(
                stream,
                {"data": json.dumps(payload, default=str)},
                maxlen=STREAM_MAXLEN,
                approximate=True,
            )
        except redis.ConnectionError as e:
            logger.warning("Redis disconnect; dropping message on %s: %s", stream, e)
            return None

    async def subscribe_stream(
        self,
        stream: str,
        group: str,
        consumer: str,
        block_ms: int = 5000,
        count: int = 100,
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        try:
            await self._r.xgroup_create(stream, group, id="$", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        while True:
            try:
                resp = await self._r.xreadgroup(
                    group, consumer, {stream: ">"}, count=count, block=block_ms
                )
            except redis.ConnectionError as e:
                logger.warning("Redis disconnect on read; will retry: %s", e)
                continue

            if not resp:
                continue
            for _stream_name, messages in resp:
                for msg_id, fields in messages:
                    yield msg_id, json.loads(fields["data"])

    async def ack(self, stream: str, group: str, msg_id: str) -> None:
        await self._r.xack(stream, group, msg_id)

    async def get_hash(self, key: str) -> dict[str, str]:
        try:
            return await self._r.hgetall(key) or {}
        except redis.ConnectionError as e:
            logger.warning("Redis disconnect on HGETALL %s: %s", key, e)
            return {}

    async def set_hash(
        self,
        key: str,
        mapping: dict[str, Any],
        ttl_seconds: int | None = None,
    ) -> None:
        encoded = {
            k: (v if isinstance(v, str) else json.dumps(v, default=str))
            for k, v in mapping.items()
        }
        try:
            await self._r.hset(key, mapping=encoded)
            if ttl_seconds:
                await self._r.expire(key, ttl_seconds)
        except redis.ConnectionError as e:
            logger.warning("Redis disconnect on HSET %s: %s", key, e)

    async def ping(self) -> bool:
        try:
            return await self._r.ping()
        except redis.ConnectionError:
            return False

    async def close(self) -> None:
        await self._r.aclose()
