# tools/find_contracts.py
import argparse, csv, datetime as dt, json, re, sys
from typing import List, Dict
import requests

API = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

def fetch_awards(query: str, start: str, end: str, limit: int = 100) -> List[Dict]:
    payload = {
        "filters": {
            "keywords": [query],
            "time_period": [{"start_date": start, "end_date": end}],
        },
        "fields": [
            "Award ID", "Recipient Name", "Award Amount",
            "Awarding Agency", "Description",
            "Period of Performance Start Date", "Period of Performance Current End Date"
        ],
        "page": 1,
        "limit": limit,
        "sort": "Award Amount",
        "order": "desc",
    }
    r = requests.post(API, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data.get("results", [])

def score(text: str, terms: List[str]) -> int:
    if not text: return 0
    t = text.lower()
    return sum(1 for term in terms if re.search(rf"\b{re.escape(term.lower())}\b", t))

def load_terms(s: str) -> List[str]:
    # comma or newline separated
    if "\n" in s or "," in s:
        parts = [p.strip() for p in re.split(r"[\n,]+", s) if p.strip()]
        return parts
    return [s.strip()] if s.strip() else []

def main():
    parser = argparse.ArgumentParser(description="Pull & score awards from USAspending")
    parser.add_argument("--q", "--query", dest="query", required=True, help="keyword(s) for USAspending 'keywords' filter")
    parser.add_argument("--since", default=(dt.date.today().replace(month=1, day=1)).isoformat(), help="YYYY-MM-DD (start)")
    parser.add_argument("--until", default=dt.date.today().isoformat(), help="YYYY-MM-DD (end)")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--terms", help="comma/newline-separated scoring terms (your investigatory matrix keywords)")
    parser.add_argument("--terms-file", help="file path containing scoring terms (one per line)")

    parser.add_argument("--csv", help="write results to CSV path")
    parser.add_argument("--json", help="write raw JSON to file")

    args = parser.parse_args()

    terms: List[str] = []
    if args.terms:
        terms.extend(load_terms(args.terms))
    if args.terms_file:
        terms.extend(load_terms(open(args.terms_file, "r", encoding="utf-8").read()))
    terms = [t for t in terms if t]

    results = fetch_awards(args.query, args.since, args.until, args.limit)

    rows = []
    for r in results:
        desc = r.get("Description") or ""
        s = score(desc, terms) if terms else 0
        rows.append({
            "score": s,
            "award_id": r.get("Award ID"),
            "recipient": r.get("Recipient Name"),
            "amount": r.get("Award Amount"),
            "agency": r.get("Awarding Agency"),
            "start": r.get("Period of Performance Start Date"),
            "end": r.get("Period of Performance Current End Date"),
            "description": desc,
        })

    # sort by score then amount
    rows.sort(key=lambda x: (x["score"], x["amount"] or 0), reverse=True)

    # print a quick top-10 to console
    for i, row in enumerate(rows[:10], 1):
        print(f"{i:>2}. [score {row['score']}] ${row['amount']:,} {row['recipient']} — {row['award_id']}")
        print(f"    {row['agency']} | {row['start']} → {row['end']}")
        print(f"    {row['description'][:240].replace(chr(10),' ')}")
        print()

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else
                               ["score","award_id","recipient","amount","agency","start","end","description"])
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote CSV: {args.csv}")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        print(f"Wrote JSON: {args.json}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
