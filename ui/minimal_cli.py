import argparse, requests, datetime
from typing import List, Dict, Any

BASE = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

def fetch_awards(since: str="2008-01-01", limit: int=10) -> List[Dict[str, Any]]:
    end = datetime.date.today().strftime("%Y-%m-%d")
    fields = [
        "Award ID",
        "Recipient Name",
        "Award Amount",
        "Action Date",
    ]
    payload = {
        "filters": {
            "time_period": [
                {"date_type": "action_date", "start_date": since, "end_date": end}
            ],
            "award_type_codes": ["A", "B", "C", "D"],
        },
        "page": 1,
        "limit": limit,
        "subawards": False,
        "sort": "Award Amount",
        "order": "desc",
        "fields": fields,
    }
    r = requests.post(BASE, json=payload, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])

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
