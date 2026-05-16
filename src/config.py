"""Centralized configuration loader.

Loads `config/settings.yaml` into a Pydantic v2 model hierarchy that
mirrors the YAML structure. Runtime overrides from Redis hash
`system:config` take precedence over file values.

Override keys use dotted paths (e.g.
`detection.anomaly.alert_threshold_watch`). Override values are coerced to
the type of the corresponding YAML value.

`get_config()` is memoised. After updating overrides at runtime, call
`reload_config()` to pick them up.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_YAML = _PROJECT_ROOT / "config" / "settings.yaml"


# ----- Pydantic models -------------------------------------------------------

class PolymarketSettings(BaseModel):
    poll_interval_seconds: int = Field(gt=0)
    backfill_days: int = Field(ge=0)
    max_requests_per_second: float = Field(gt=0)


class KalshiSettings(BaseModel):
    poll_interval_seconds: int = Field(gt=0)
    api_key_env: str
    max_requests_per_second: float = Field(gt=0)


class WalletTracerSettings(BaseModel):
    max_concurrent_rpc: int = Field(ge=1)
    retrace_interval_hours: int = Field(gt=0)
    hop_depth: int = Field(ge=1)


class IngestionSettings(BaseModel):
    polymarket: PolymarketSettings
    kalshi: KalshiSettings
    wallet_tracer: WalletTracerSettings


class AnomalySettings(BaseModel):
    wallet_age_threshold_days: int = Field(ge=0)
    wallet_market_count_threshold: int = Field(ge=0)
    win_rate_pvalue_threshold: float = Field(gt=0, lt=1)
    bet_size_zscore_threshold: float = Field(gt=0)
    pre_resolution_window_minutes: int = Field(ge=0)
    alert_threshold_watch: float = Field(ge=0)
    alert_threshold_critical: float = Field(ge=0)


class VelocityThresholds(BaseModel):
    sprint: float = Field(gt=0)
    fast: float = Field(gt=0)
    moderate: float = Field(gt=0)


class SignalSettings(BaseModel):
    min_prob_shift: float = Field(ge=0, le=1)
    volume_confirmation_ratio: float = Field(gt=0)
    cross_platform_required: bool
    velocity_thresholds: VelocityThresholds


class TimeAdjusterSettings(BaseModel):
    preset_late_stage_threshold: float = Field(ge=0, le=1)
    preset_early_stage_threshold: float = Field(ge=0, le=1)
    expiring_risk_free_rate: float = Field(ge=0)
    triggered_confidence_discount: float = Field(ge=0, le=1)
    triggered_maturity_minutes: int = Field(ge=0)


class DetectionSettings(BaseModel):
    anomaly: AnomalySettings
    signal: SignalSettings
    time_adjuster: TimeAdjusterSettings


class ModelHyperparams(BaseModel):
    algorithm: str
    n_estimators: int = Field(gt=0)
    max_depth: int = Field(gt=0)
    learning_rate: float = Field(gt=0)
    min_child_samples: int | None = None
    subsample: float | None = None
    colsample_bytree: float | None = None
    min_training_samples: int = Field(ge=0)
    calibration_method: str | None = None
    objective: str | None = None


class ModelsSettings(BaseModel):
    stage1: ModelHyperparams
    stage2: ModelHyperparams


class CategoryCaps(BaseModel):
    fed_rate: float = Field(ge=0, le=1)
    earnings: float = Field(ge=0, le=1)
    geopolitical: float = Field(ge=0, le=1)
    recession: float = Field(ge=0, le=1)
    crypto: float = Field(ge=0, le=1)
    political: float = Field(ge=0, le=1)


class RiskGates(BaseModel):
    min_signal_quality: float = Field(ge=0, le=1)
    min_linkage_score: float = Field(ge=0, le=1)
    min_joint_confidence: float = Field(ge=0, le=1)
    max_spread_bps: float = Field(ge=0)


class RiskSizing(BaseModel):
    max_pm_exposure_pct: float = Field(ge=0, le=1)
    category_caps: CategoryCaps


class CircuitBreakers(BaseModel):
    drawdown_limit_pct: float = Field(ge=0, le=1)
    drawdown_cooldown_days: int = Field(ge=0)
    feed_stall_minutes: int = Field(ge=0)
    psi_halt_threshold: float = Field(ge=0)
    brier_reduce_threshold: float = Field(ge=0, le=1)
    brier_recovery_days: int = Field(ge=0)


class RiskSettings(BaseModel):
    gates: RiskGates
    sizing: RiskSizing
    circuit_breakers: CircuitBreakers


class Brokers(BaseModel):
    equities: str
    options: str
    crypto: str


class AlpacaSettings(BaseModel):
    api_key_env: str
    api_secret_env: str
    base_url: str


class ExecutionSettings(BaseModel):
    paper_mode: bool
    brokers: Brokers
    alpaca: AlpacaSettings
    order_timeout_minutes: int = Field(ge=0)


class NotificationRefs(BaseModel):
    slack_webhook_env: str
    telegram_bot_token_env: str
    telegram_chat_id_env: str


class MonitoringSettings(BaseModel):
    health_check_interval_hours: int = Field(ge=0)
    crowding_check_interval_days: int = Field(ge=0)
    notifications: NotificationRefs


class LinkageSettings(BaseModel):
    min_layers_passing: int = Field(ge=1)
    min_composite_score: float = Field(ge=0, le=1)
    min_events_for_event_study: int = Field(ge=1)
    granger_resample_minutes: int = Field(gt=0)
    granger_max_lag_minutes: int = Field(gt=0)
    cross_correlation_max_lag_minutes: int = Field(gt=0)
    min_viable_lag_minutes: int = Field(ge=0)


class FeatureFlags(BaseModel):
    paper_mode: bool
    stage2_enabled: bool
    execution_enabled: bool
    crypto_enabled: bool
    wallet_tracing_enabled: bool
    cross_platform_required: bool
    self_training_enabled: bool


class Settings(BaseModel):
    ingestion: IngestionSettings
    detection: DetectionSettings
    models: ModelsSettings
    risk: RiskSettings
    execution: ExecutionSettings
    monitoring: MonitoringSettings
    linkage: LinkageSettings
    feature_flags: FeatureFlags


# ----- Loaders ---------------------------------------------------------------

def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r") as f:
        return yaml.safe_load(f)


def _coerce_to_type_of(existing: Any, raw: str) -> Any:
    if isinstance(existing, bool):
        return raw.strip().lower() in ("1", "true", "yes", "on")
    if isinstance(existing, int):
        return int(raw)
    if isinstance(existing, float):
        return float(raw)
    return raw


def _apply_overrides(base: dict[str, Any], overrides: dict[str, str]) -> dict[str, Any]:
    """Merge dotted-key overrides into `base` in place."""
    for dotted_key, raw_value in overrides.items():
        parts = dotted_key.split(".")
        node: Any = base
        for p in parts[:-1]:
            if not isinstance(node, dict) or p not in node:
                node = None
                break
            node = node[p]
        if isinstance(node, dict) and parts[-1] in node:
            node[parts[-1]] = _coerce_to_type_of(node[parts[-1]], raw_value)
    return base


def _fetch_redis_overrides() -> dict[str, str]:
    """Best-effort fetch of `system:config` hash. Returns {} on any failure."""
    try:
        import redis  # local import keeps config.py importable without Redis
        client = redis.Redis(host="localhost", port=6379, decode_responses=True)
        return client.hgetall("system:config") or {}
    except Exception:
        return {}


@lru_cache(maxsize=1)
def get_config(path: str | os.PathLike[str] | None = None) -> Settings:
    yaml_path = Path(path) if path else _DEFAULT_YAML
    data = _load_yaml(yaml_path)
    overrides = _fetch_redis_overrides()
    if overrides:
        data = _apply_overrides(data, overrides)
    return Settings.model_validate(data)


def reload_config() -> Settings:
    get_config.cache_clear()
    return get_config()
