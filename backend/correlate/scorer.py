from __future__ import annotations

import math
from typing import Any, Dict

DEFAULT_KW_PAIR_SCORE_KIND = "npmi"
DEFAULT_KW_PAIR_SECONDARY_SCORE_KIND = "log_odds"
DEFAULT_KW_PAIR_SMOOTHING = 0.5
DEFAULT_KW_PAIR_MAX_DF_RATIO = 0.20
DEFAULT_KW_PAIR_MAX_DF_FLOOR = 8
DEFAULT_KW_PAIR_BONUS_MIN_SIGNAL = 0.15
DEFAULT_KW_PAIR_BONUS_MIN_EVENT_COUNT = 2

_EXCLUDED_KW_PAIR_PREFIXES = ("operational_noise_terms:",)
_EXCLUDED_KW_PAIR_PACK_TOKENS = ("noise", "suppress")


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)



def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)



def format_signal_score(value: float) -> str:
    return f"{float(value):.6f}"



def normalize_keyword(keyword: str) -> str:
    return str(keyword).strip().replace("|", "/").lower()



def keyword_pack(keyword: str) -> str:
    norm = normalize_keyword(keyword)
    pack, _sep, _rule = norm.partition(":")
    return pack



def is_excluded_kw_pair_keyword(keyword: str) -> bool:
    norm = normalize_keyword(keyword)
    if not norm:
        return True
    if any(norm.startswith(prefix) for prefix in _EXCLUDED_KW_PAIR_PREFIXES):
        return True
    pack = keyword_pack(norm)
    return bool(pack and any(token in pack for token in _EXCLUDED_KW_PAIR_PACK_TOKENS))



def compute_kw_pair_df_threshold(
    total_events: int,
    *,
    max_keyword_df_ratio: float = DEFAULT_KW_PAIR_MAX_DF_RATIO,
    max_keyword_df_floor: int = DEFAULT_KW_PAIR_MAX_DF_FLOOR,
) -> int:
    total_events = max(0, safe_int(total_events))
    if total_events <= 0:
        return 0

    ratio = max(0.0, safe_float(max_keyword_df_ratio, DEFAULT_KW_PAIR_MAX_DF_RATIO))
    floor = max(0, safe_int(max_keyword_df_floor, DEFAULT_KW_PAIR_MAX_DF_FLOOR))
    ratio_limit = total_events if ratio <= 0 else int(math.ceil(float(total_events) * ratio))
    return max(floor, ratio_limit)



def compute_kw_pair_signal(
    *,
    total_events: int,
    c1: int,
    c2: int,
    c12: int,
    smoothing: float = DEFAULT_KW_PAIR_SMOOTHING,
) -> Dict[str, float]:
    total_events = max(0, safe_int(total_events))
    c1 = max(0, safe_int(c1))
    c2 = max(0, safe_int(c2))
    c12 = max(0, safe_int(c12))
    alpha = max(0.0, safe_float(smoothing, DEFAULT_KW_PAIR_SMOOTHING))

    if total_events <= 0:
        return {
            "score_signal": 0.0,
            "score_secondary": 0.0,
            "pmi": 0.0,
            "npmi": 0.0,
            "log_odds": 0.0,
            "lift_raw": 0.0,
            "lift_smoothed": 0.0,
            "expected_count": 0.0,
        }

    a = float(c12) + alpha
    b = float(max(0, c1 - c12)) + alpha
    c = float(max(0, c2 - c12)) + alpha
    d = float(max(0, total_events - c1 - c2 + c12)) + alpha
    total_smoothed = a + b + c + d

    p12 = a / total_smoothed if total_smoothed > 0 else 0.0
    p1 = (a + b) / total_smoothed if total_smoothed > 0 else 0.0
    p2 = (a + c) / total_smoothed if total_smoothed > 0 else 0.0

    lift_smoothed = 0.0
    if p12 > 0 and p1 > 0 and p2 > 0:
        lift_smoothed = p12 / (p1 * p2)

    pmi = math.log(lift_smoothed) if lift_smoothed > 0 else 0.0
    npmi_denom = -math.log(p12) if 0.0 < p12 < 1.0 else 0.0
    npmi = pmi / npmi_denom if npmi_denom > 0 else 0.0
    log_odds = math.log((a * d) / (b * c)) if a > 0 and b > 0 and c > 0 and d > 0 else 0.0

    lift_raw = 0.0
    if total_events > 0 and c1 > 0 and c2 > 0:
        lift_raw = (float(c12) * float(total_events)) / (float(c1) * float(c2))

    expected_count = (float(c1) * float(c2)) / float(total_events) if total_events > 0 else 0.0

    return {
        "score_signal": float(npmi),
        "score_secondary": float(log_odds),
        "pmi": float(pmi),
        "npmi": float(npmi),
        "log_odds": float(log_odds),
        "lift_raw": float(lift_raw),
        "lift_smoothed": float(lift_smoothed),
        "expected_count": float(expected_count),
    }



def kw_pair_lane_payload(lanes_hit: Any) -> dict[str, Any]:
    if isinstance(lanes_hit, dict):
        if lanes_hit.get("lane") == "kw_pair":
            return lanes_hit
        nested = lanes_hit.get("kw_pair")
        if isinstance(nested, dict):
            return nested
        if any(key in lanes_hit for key in ("keyword_1", "k1", "event_count", "c12", "score_signal", "score_secondary")):
            return lanes_hit
    return {}



def kw_pair_score_signal(lanes_hit: Any) -> float | None:
    payload = kw_pair_lane_payload(lanes_hit)
    if not payload:
        return None
    value = payload.get("score_signal")
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None



def kw_pair_score_secondary(lanes_hit: Any) -> float | None:
    payload = kw_pair_lane_payload(lanes_hit)
    if not payload:
        return None
    value = payload.get("score_secondary")
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None



def kw_pair_event_count(lanes_hit: Any, *, fallback_score: Any = None) -> int:
    payload = kw_pair_lane_payload(lanes_hit)
    value = payload.get("event_count")
    if value is None:
        value = payload.get("c12")
    if value is None:
        try:
            return int(fallback_score)
        except Exception:
            return 0
    return safe_int(value)



def kw_pair_bonus_contribution(
    *,
    score_signal: float | None,
    event_count: int,
    min_signal: float = DEFAULT_KW_PAIR_BONUS_MIN_SIGNAL,
    min_event_count: int = DEFAULT_KW_PAIR_BONUS_MIN_EVENT_COUNT,
) -> float:
    if score_signal is None:
        return 0.0
    if safe_int(event_count) < safe_int(min_event_count):
        return 0.0
    signal = safe_float(score_signal)
    if signal < safe_float(min_signal):
        return 0.0
    return max(0.0, signal)


