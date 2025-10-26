import argparse
import requests
from datetime import date
from typing import List, Dict, Any, Tuple

BASE = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

def _try(payload: Dict[str, Any]) -> Tuple[bool, List[Dict[str, Any]], str]:
    r = requests.post(BASE, json=payload, timeout=30)
    if r.status_code == 200:
        return True, r.json().get("results", []), ""
    return False, [], f"{r.status_code} {r.text[:500]}"

def fetch_awards(since: str = "2008-01-01", limit: int = 10) -> List[Dict[str, Any]]:
    """
    Fetch a small page of awards with safe defaults.
    Tries a sequence of payloads from most to least specific to avoid 400s.
    """
    end = date.today().strftime("%Y-%m-%d")

    base = {
        "filters": {"time_period": [{"date_type": "action_date", "start_date": since, "end_date": end}]},
        "page": 1,
        "limit": limit,
        "subawards": False,
        "sort": "Action Date",
        "order": "desc",
    }

    attempts = [
        {**base, "award_type_codes": ["A", "B", "C", "D"]},  # common contract types
        base,                                                # drop award_type filter
        {**base, "sort": "Award Amount", "order": "desc"},   # change sort if API doesn’t like the label
    ]

    last_err = ""
    for p in attempts:
        ok, results, err = _try(p)
        if ok:
            return results
        last_err = err

    raise RuntimeError(f"USAspending request failed after {len(attempts)} attempts; last error: {last_err}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--since", default="2008-01-01")
    p.add_argument("--limit", type=int, default=10)
    a = p.parse_args()

    results = fetch_awards(a.since, a.limit)
    print(f"Fetched {len(results)} awards")
    for r in results[:3]:
        print("-", r.get("Award ID") or r.get("piid") or r.get("generated_unique_award_id"))

if __name__ == "__main__":
    main()
