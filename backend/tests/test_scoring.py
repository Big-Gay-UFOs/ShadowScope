from backend.analysis.scoring import score_from_keywords_clauses
from backend.correlate.scorer import compute_kw_pair_signal, kw_pair_lane_payload



def test_score_from_clauses():
    clauses = [
        {"pack": "p", "rule": "a", "weight": 5, "field": "snippet", "match": "x"},
        {"pack": "p", "rule": "b", "weight": 12, "field": "snippet", "match": "y"},
    ]
    score, details = score_from_keywords_clauses([], clauses, has_entity=False)
    assert score == 17
    assert details["clause_score"] == 17
    assert details["pack_hits"] == 1
    assert details["rule_hits"] == 2



def test_score_keyword_fallback_when_no_clauses():
    score, details = score_from_keywords_clauses(["a", "b", "c"], [], has_entity=False)
    assert score == 9  # 3 * 3 keywords
    assert details["keyword_score"] == 9



def test_entity_bonus():
    score, details = score_from_keywords_clauses([], [{"weight": 4}], has_entity=True)
    assert score == 14
    assert details["entity_bonus"] == 10



def test_compute_kw_pair_signal_handles_full_joint_probability_without_div_zero():
    metrics = compute_kw_pair_signal(total_events=3, c1=3, c2=3, c12=3, smoothing=0.0)
    assert metrics["pmi"] == 0.0
    assert metrics["npmi"] == 0.0
    assert metrics["score_signal"] == 0.0



def test_kw_pair_lane_payload_prefers_nested_kw_pair_object_when_present():
    lanes_hit = {
        "kw_pair": {
            "keyword_1": "nested:a",
            "keyword_2": "nested:b",
            "event_count": 4,
            "score_signal": 0.75,
        },
        "event_count": 999,
        "score_signal": 0.01,
    }
    payload = kw_pair_lane_payload(lanes_hit)
    assert payload["keyword_1"] == "nested:a"
    assert payload["keyword_2"] == "nested:b"
    assert payload["event_count"] == 4
    assert payload["score_signal"] == 0.75
