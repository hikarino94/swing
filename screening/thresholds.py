# -*- coding: utf-8 -*-
"""Threshold values for screening modules.

This module centralises numeric constants used when filtering
fundamental and technical signals so that they can be tweaked in one
place.
"""

# Fundamental screening thresholds
EPS_YOY_MIN = 0.30
CF_QUALITY_MIN = 0.8
ETA_DELTA_MIN = 0.0
TREASURY_DELTA_MAX = 0.0

# Technical screening thresholds
RSI_THRESHOLD = 50
ADX_THRESHOLD = 20
OVERHEAT_FACTOR = 1.1
SIGNAL_COUNT_MIN = 3
FIRST_LOOKBACK_DAYS = 30
