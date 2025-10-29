# tools/find_contracts.py
import argparse, csv, datetime as dt, json, re, sys
from typing import List, Dict
import requests

API = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

def split_or(q: str) -> List[str]:
    parts = [p.strip().strip('"').strip("'") for p in re.split(r"\s+OR\s+|,", q, flags=re.I) if p.strip()]
    return parts or [q]

def fetch_awards(query: str, start: str, end: str, max_results: int = 200) -> List[Dict]:
    page, gathered, page_size = 1, [], min(100, max_results)
    keywords = split_or(query)
    while len(gathered) < max_results:
        payload = {
            "filters": {
                "keywords": keywords,
                "award_type_codes": ["A","B","C","D"],  # contracts
                "time_period": [{"start_date": start, "end_date": end}],
            },
            "fields": [
                "Award ID","Recipient Name","Award Amount","Awarding Agency","Description",
                "Action Date","Period of Performance Start Date","Period of Performance Current End Date",
            ],
            "page": page, "limit": page_size, "sort": "Award Amount", "order": "desc",
        }
        r = requests.post(API, json=payload, timeout=60)
        if r.status_code != 200:
            raise SystemExit(f"USAspending {r.status_code}: {r.text}")
        data = r.json()
        results = data.get("results", [])
        if not results: break
        gathered.extend(results)
        if len(results) < page_size:_
