from backend.analysis.scoring import score_from_keywords_clauses


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