# -*- coding: utf-8 -*-
"""Threshold values for screening modules.

The numeric constants used when filtering fundamental and technical
signals can be tweaked via ``thresholds.json`` placed next to this file.
When loaded, the values are logged for transparency.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def _load_from_json(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as exc:  # pragma: no cover - just in case
        logger.warning("Failed to load %s: %s", path, exc)
        return {}


def load_thresholds(path: Path | None = None) -> dict[str, float]:
    """Load threshold values from ``path``.

    If ``path`` is ``None``, ``thresholds.json`` next to this module is used.
    The loaded values are logged and returned.
    """

    defaults: dict[str, float] = {
        "EPS_YOY_MIN": 0.30,
        "CF_QUALITY_MIN": 0.8,
        "ETA_DELTA_MIN": 0.0,
        "TREASURY_DELTA_MAX": 0.0,
        "RSI_THRESHOLD": 50,
        "ADX_THRESHOLD": 20,
        "OVERHEAT_FACTOR": 1.1,
        "OVERSOLD_FACTOR": 0.95,
        "SIGNAL_COUNT_MIN": 3,
        "SHORT_SIGNAL_COUNT_MIN": 4,
        "FIRST_LOOKBACK_DAYS": 30,
    }

    path = path or Path(__file__).with_suffix(".json")
    loaded = _load_from_json(path)
    thresholds = {**defaults, **loaded}
    logger.info("Thresholds loaded from %s: %s", path, thresholds)
    return thresholds


_vals = load_thresholds()

EPS_YOY_MIN = _vals["EPS_YOY_MIN"]
CF_QUALITY_MIN = _vals["CF_QUALITY_MIN"]
ETA_DELTA_MIN = _vals["ETA_DELTA_MIN"]
TREASURY_DELTA_MAX = _vals["TREASURY_DELTA_MAX"]
RSI_THRESHOLD = _vals["RSI_THRESHOLD"]
ADX_THRESHOLD = _vals["ADX_THRESHOLD"]
OVERHEAT_FACTOR = _vals["OVERHEAT_FACTOR"]
OVERSOLD_FACTOR = _vals["OVERSOLD_FACTOR"]
SIGNAL_COUNT_MIN = _vals["SIGNAL_COUNT_MIN"]
SHORT_SIGNAL_COUNT_MIN = _vals["SHORT_SIGNAL_COUNT_MIN"]
FIRST_LOOKBACK_DAYS = _vals["FIRST_LOOKBACK_DAYS"]


def log_thresholds(logger_: logging.Logger | None = None) -> None:
    """Log current threshold values using ``logger_`` or module logger."""

    logger_ = logger_ or logger
    logger_.info(
        "Thresholds: EPS_YOY_MIN=%s CF_QUALITY_MIN=%s ETA_DELTA_MIN=%s TREASURY_DELTA_MAX=%s "
        "RSI_THRESHOLD=%s ADX_THRESHOLD=%s OVERHEAT_FACTOR=%s OVERSOLD_FACTOR=%s SIGNAL_COUNT_MIN=%s SHORT_SIGNAL_COUNT_MIN=%s FIRST_LOOKBACK_DAYS=%s",
        EPS_YOY_MIN,
        CF_QUALITY_MIN,
        ETA_DELTA_MIN,
        TREASURY_DELTA_MAX,
        RSI_THRESHOLD,
        ADX_THRESHOLD,
        OVERHEAT_FACTOR,
        OVERSOLD_FACTOR,
        SIGNAL_COUNT_MIN,
        SHORT_SIGNAL_COUNT_MIN,
        FIRST_LOOKBACK_DAYS,
    )
