import argparse
import requests
from datetime import date
from typing import List, Dict, Any, Tuple

BASE = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

def _try(payload: Dict[str, Any]) -> Tuple[bool, List[Dict[str, Any]], str]:
    """POST the payload; return (ok, results, error_snippet)."""
    r = requests.post(BASE, json=payload, timeout=30)
    if r.status_code == 200:
        try:
            data = r.json()
        except Exception:
            return False, [], f"{r.status_code} bad JSON"
        return True, data.get("results", []), ""
    return False, [], f"{r.status_code} {r.text[:500]}"

def fetch_awards(since: str = "2008-01-01", limit: int = 10) -> List[Dict[str, Any]]:
    """
    Fetch a small page of awards with safe defaults and fallbacks.
    Some deployments of /spending_by_award/ require `fields`; others are picky about sort
    or award_type_codes. We always include `fields` and try a couple of variants.
    """
    end = date.today().strftime("%Y-%m-%d")

    fields = ["Award ID", "Recipient Name", "Award Amount", "Action Date"]

    base: Dict[str, Any] = {
        "filters": {"time_period": [{"date_type": "action_date", "start_date": since, "end_date": end}]},
        "page": 1,
        "limit": max(1, int(limit)),
        "subawards": False,
    }

    attempts: List[Dict[str, Any]] = [
        # Most common: explicit contract types + fields + sort by Action Date desc
        {**base, "filters": {**base["filters"], "award_type_codes": ["A", "B", "C", "D"]},
         "fields": fields, "sort": "Action Date", "order": "desc"},

        # Drop award_type filter, keep fields and Action Date sort
        {**base, "fields": fields, "sort": "Action Date", "order": "desc"},

        # Keep fields, change sort if Action Date is rejected
        {**base, "fields": fields, "sort": "Award Amount", "order": "desc"},

        # Re-introduce award_type_codes with the alternate sort in case they're required
        {**base, "filters": {**base["filters"], "award_type_codes": ["A", "B", "C", "D"]},
         "fields": fields, "sort": "Award Amount", "order": "desc"},
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
