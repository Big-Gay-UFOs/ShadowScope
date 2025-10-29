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
        if len(results) < page_size: break
        page += 1
    return gathered[:max_results]

def any_hit(text: str, terms: List[str]) -> bool:
    if not text: return False
    t = text.lower()
    return any(re.search(rf"\b{re.escape(term.lower())}\b", t) for term in terms)

def count_hits(text: str, terms: List[str]) -> int:
    if not text: return 0
    t = text.lower()
    return sum(1 for term in terms if re.search(rf"\b{re.escape(term.lower())}\b", t))

def parse_terms_blob(s: str) -> List[str]:
    return [p.strip() for p in re.split(r"[\n,]+", s) if p.strip()]

def main():
    p = argparse.ArgumentParser(description="Pull & score awards against an investigatory matrix")
    p.add_argument("--q", required=True, help='keywords (use "A OR B OR C" or commas)')
    p.add_argument("--since", default=(dt.date.today().replace(month=1, day=1)).isoformat())
    p.add_argument("--until", default=dt.date.today().isoformat())
    p.add_argument("--limit", type=int, default=200)

    # Weighted buckets (3/2/1)
    p.add_argument("--tech", help="comma/newline list of technical terms (weight 3)")
    p.add_argument("--platform", help="platform/airframe/program terms (weight 2)")
    p.add_argument("--org", help="org/vendor acronyms/names (weight 1)")

    # Hard filters (must/must-not)
    p.add_argument("--include", help="must appear (any of) in description")
    p.add_argument("--exclude", help="must NOT appear (any of) in description")
    p.add_argument("--min-amount", type=float, default=0.0)
    p.add_argument("--agency-like", help="substring match on 'Awarding Agency'")
    p.add_argument("--recipient-like", help="substring match on 'Recipient Name'")

    # Output
    p.add_argument("--csv")
    p.add_argument("--json")

    args = p.parse_args()

    tech = parse_terms_blob(args.tech or "")
    plat = parse_terms_blob(args.platform or "")
    orgs = parse_terms_blob(args.org or "")
    need = parse_terms_blob(args.include or "")
    block = parse_terms_blob(args.exclude or "")

    results = fetch_awards(args.q, args.since, args.until, args.limit)

    rows = []
    for r in results:
        desc = (r.get("Description") or "")
        agency = r.get("Awarding Agency") or ""
        recip = r.get("Recipient Name") or ""
        amt = r.get("Award Amount") or 0

        # hard filters
        if args.min_amount and (amt or 0) < args.min_amount: 
            continue
        if args.agency_like and args.agency_like.lower() not in agency.lower():
            continue
        if args.recipient_like and args.recipient_like.lower() not in recip.lower():
            continue
        if need and not any_hit(desc, need): 
            continue
        if block and any_hit(desc, block): 
            continue

        # weighted score
        s = 3*count_hits(desc, tech) + 2*count_hits(desc, plat) + 1*count_hits(desc, orgs)

        rows.append({
            "score": s,
            "amount": amt,
            "award_id": r.get("Award ID"),
            "recipient": recip,
            "agency": agency,
            "action_date": r.get("Action Date"),
            "start": r.get("Period of Performance Start Date"),
            "end": r.get("Period of Performance Current End Date"),
            "description": desc,
        })

    # rank
    rows.sort(key=lambda x: (x["score"], x["amount"] or 0), reverse=True)

    # console top-10
    for i, row in enumerate(rows[:10], 1):
        amt = f"${row['amount']:,}" if isinstance(row["amount"], (int, float)) else row["amount"]
        print(f"{i:>2}. [score {row['score']}] {amt} {row['recipient']} — {row['award_id']}")
        print(f"    {row['agency']} | action {row['action_date']} | {row['start']} → {row['end']}")
        print(f"    {row['description'][:240].replace(chr(10),' ')}\n")

    if args.csv and rows:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)
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
