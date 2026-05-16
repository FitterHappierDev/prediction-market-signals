"""QuestDB client: ILP for writes, HTTP for reads.

Writes use questdb-python's `Sender` over ILP/TCP to localhost:9009.
Reads issue HTTP requests to localhost:9000/exec and return DataFrames.

If ILP is unreachable, rows are buffered in memory (FIFO, max 1000) and
flushed when the connection is restored. If the buffer fills, the oldest
rows are dropped (the deque's `maxlen` semantics) and a warning logged.

Table DDL for the entire platform lives in `TABLE_DDL` and is materialised
by `ensure_tables()`. The CREATE statements use `IF NOT EXISTS` so
re-running is safe.
"""

from __future__ import annotations

import collections
import logging
import time
from collections.abc import Mapping
from typing import Any

import httpx
import pandas as pd
from questdb.ingress import IngressError, Sender, TimestampNanos

logger = logging.getLogger(__name__)

DEFAULT_MAX_BUFFER = 1000
DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY = 5.0

TABLE_DDL: dict[str, str] = {
    "pm_probabilities": """
        CREATE TABLE IF NOT EXISTS pm_probabilities (
            timestamp TIMESTAMP,
            market_id SYMBOL,
            source SYMBOL,
            probability DOUBLE,
            bid DOUBLE,
            ask DOUBLE,
            volume_usd DOUBLE,
            trade_count INT
        ) TIMESTAMP(timestamp) PARTITION BY DAY
    """,
    "pm_trades": """
        CREATE TABLE IF NOT EXISTS pm_trades (
            timestamp TIMESTAMP,
            market_id SYMBOL,
            source SYMBOL,
            wallet_address VARCHAR,
            side SYMBOL,
            price DOUBLE,
            size_usd DOUBLE,
            trade_id VARCHAR
        ) TIMESTAMP(timestamp) PARTITION BY DAY
    """,
    "pm_markets": """
        CREATE TABLE IF NOT EXISTS pm_markets (
            market_id SYMBOL,
            source SYMBOL,
            title VARCHAR,
            category SYMBOL,
            bet_type SYMBOL,
            created_at TIMESTAMP,
            resolution_time TIMESTAMP,
            resolved BOOLEAN,
            outcome SYMBOL,
            strike_price DOUBLE,
            underlying_ticker SYMBOL,
            volume_total_usd DOUBLE,
            last_updated TIMESTAMP
        ) TIMESTAMP(last_updated)
    """,
    "pm_wallets": """
        CREATE TABLE IF NOT EXISTS pm_wallets (
            wallet_address VARCHAR,
            first_seen TIMESTAMP,
            funding_source_1 VARCHAR,
            funding_source_2 VARCHAR,
            total_transactions INT,
            distinct_markets INT,
            total_volume_usd DOUBLE,
            last_traced TIMESTAMP
        ) TIMESTAMP(last_traced)
    """,
    "pm_anomaly_scores": """
        CREATE TABLE IF NOT EXISTS pm_anomaly_scores (
            timestamp TIMESTAMP,
            market_id SYMBOL,
            wallet_address VARCHAR,
            composite_score DOUBLE,
            wallet_freshness_score DOUBLE,
            statistical_improbability_score DOUBLE,
            pre_resolution_score DOUBLE,
            bet_size_anomaly_score DOUBLE,
            sybil_cluster_score DOUBLE,
            alert_level SYMBOL
        ) TIMESTAMP(timestamp) PARTITION BY DAY
    """,
    "pm_trades_executed": """
        CREATE TABLE IF NOT EXISTS pm_trades_executed (
            timestamp TIMESTAMP,
            signal_id VARCHAR,
            market_id SYMBOL,
            pm_category SYMBOL,
            asset_ticker SYMBOL,
            direction SYMBOL,
            entry_price DOUBLE,
            exit_price DOUBLE,
            position_size_usd DOUBLE,
            signal_quality_prob DOUBLE,
            linkage_score DOUBLE,
            joint_confidence DOUBLE,
            entry_fill_latency_ms INT,
            exit_fill_latency_ms INT,
            slippage_bps DOUBLE,
            hold_minutes INT,
            pnl_usd DOUBLE,
            pnl_bps DOUBLE,
            entry_timestamp TIMESTAMP,
            exit_timestamp TIMESTAMP
        ) TIMESTAMP(timestamp) PARTITION BY MONTH
    """,
    "pm_model_metrics": """
        CREATE TABLE IF NOT EXISTS pm_model_metrics (
            timestamp TIMESTAMP,
            model_version VARCHAR,
            stage SYMBOL,
            metric_name SYMBOL,
            metric_value DOUBLE,
            sample_size INT,
            window_days INT
        ) TIMESTAMP(timestamp)
    """,
}


_BufferedRow = tuple[str, dict[str, str], dict[str, Any], TimestampNanos]


class QuestDBClient:
    def __init__(
        self,
        ilp_host: str = "localhost",
        ilp_port: int = 9009,
        http_host: str = "localhost",
        http_port: int = 9000,
        max_buffer: int = DEFAULT_MAX_BUFFER,
    ) -> None:
        self._ilp_conf = f"tcp::addr={ilp_host}:{ilp_port};"
        self._http_url = f"http://{http_host}:{http_port}/exec"
        self._buffer: collections.deque[_BufferedRow] = collections.deque(maxlen=max_buffer)
        self._sender: Sender | None = None

    # ----- connection management ---------------------------------------------
    def _sender_or_buffer(self) -> Sender | None:
        if self._sender is not None:
            return self._sender
        try:
            s = Sender.from_conf(self._ilp_conf)
            s.establish()
            self._sender = s
            self._flush_buffer()
            return self._sender
        except IngressError as e:
            logger.warning("QuestDB ILP unreachable: %s; buffering", e)
            return None

    def _drop_sender(self) -> None:
        if self._sender is not None:
            try:
                self._sender.close()
            except Exception:
                pass
            self._sender = None

    def _flush_buffer(self) -> None:
        """Replay the in-memory buffer through a fresh sender.

        Transactional: rows are removed from the buffer only after the
        sender's flush() succeeds. If anything fails mid-replay, the
        rows are pushed back to the head of the buffer in their original
        order so we don't lose writes.
        """
        if not self._buffer or self._sender is None:
            return
        logger.info("Flushing %d buffered rows", len(self._buffer))
        queued = list(self._buffer)
        self._buffer.clear()
        try:
            for table, symbols, columns, at in queued:
                self._sender.row(table, symbols=symbols, columns=columns, at=at)
            self._sender.flush()
        except IngressError as e:
            logger.warning("Buffer re-flush failed: %s; re-queuing", e)
            self._drop_sender()
            # extendleft + reversed preserves original FIFO order
            self._buffer.extendleft(reversed(queued))

    # ----- writes -------------------------------------------------------------
    def write_row(
        self,
        table: str,
        symbols: Mapping[str, str] | None = None,
        columns: Mapping[str, Any] | None = None,
        at: TimestampNanos | None = None,
    ) -> None:
        row: _BufferedRow = (
            table,
            dict(symbols or {}),
            dict(columns or {}),
            at or TimestampNanos.now(),
        )
        sender = self._sender_or_buffer()
        if sender is None:
            if self._buffer.maxlen and len(self._buffer) >= self._buffer.maxlen:
                logger.warning("QuestDB buffer full (%d); oldest row will be dropped", self._buffer.maxlen)
            self._buffer.append(row)
            return
        try:
            # No explicit flush — questdb-python auto-flushes per row/byte/time
            # thresholds (defaults: 600 rows / 64K / 1000ms). Flushing after
            # every row was causing per-row TCP traffic that QuestDB resets
            # under sustained load.
            sender.row(table, symbols=row[1], columns=row[2], at=row[3])
        except IngressError as e:
            logger.warning("ILP write failed: %s; buffering and reconnecting", e)
            self._drop_sender()
            self._buffer.append(row)

    def write_batch(self, rows: list[dict[str, Any]]) -> None:
        for r in rows:
            self.write_row(r["table"], r.get("symbols"), r.get("columns"), r.get("at"))

    # ----- reads --------------------------------------------------------------
    def query(self, sql: str, retries: int = DEFAULT_RETRIES) -> pd.DataFrame:
        last_err: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                with httpx.Client(timeout=30.0) as client:
                    r = client.get(self._http_url, params={"query": sql})
                    r.raise_for_status()
                    body = r.json()
            except httpx.HTTPError as e:
                last_err = e
                logger.warning("QuestDB query attempt %d/%d failed: %s", attempt, retries, e)
                if attempt < retries:
                    time.sleep(DEFAULT_RETRY_DELAY)
                continue

            if "error" in body:
                raise RuntimeError(f"QuestDB query error: {body['error']}")
            cols = [c["name"] for c in body.get("columns", [])]
            return pd.DataFrame(body.get("dataset", []), columns=cols)

        raise RuntimeError(f"QuestDB query failed after {retries} retries") from last_err

    # ----- DDL ----------------------------------------------------------------
    def ensure_tables(self) -> None:
        for table, ddl in TABLE_DDL.items():
            logger.info("Ensuring table %s", table)
            self.query(ddl.strip())

    def close(self) -> None:
        # Best-effort final flush so the auto-flush buffer isn't lost on
        # shutdown.
        if self._sender is not None:
            try:
                self._sender.flush()
            except IngressError:
                pass
        self._drop_sender()
