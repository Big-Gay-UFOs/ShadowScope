import argparse
import concurrent.futures as cf
import csv
import datetime as dt
import json
import os
import re
import sys
from typing import Dict, List, Optional, Tuple

import requests


# ---------- shared scoring ----------
def blob(s: str) -> List[str]:
    return [p.strip() for p in re.split(r"[\n,]+", s or "") if p.strip()]


def split_or(q: str) -> List[str]:
    return [
        p.strip().strip('"').strip("'")
        for p in re.split(r"\s+OR\s+|,", q, flags=re.I)
        if p.strip()
    ] or [q]


def hits(text: str, terms: List[str]) -> int:
    if not text:
        return 0
    lowered = text.lower()
    return sum(1 for term in terms if re.search(rf"\b{re.escape(term.lower())}\b", lowered))


def score_row(text: str, tech: List[str], plat: List[str], orgs: List[str]) -> int:
    return 3 * hits(text, tech) + 2 * hits(text, plat) + 1 * hits(text, orgs)


def shared_award_id(row: Dict) -> Optional[str]:
    if row.get("source") in {"usaspending_awards", "usaspending_txns"}:
        identifier = str(row.get("id") or "")
        return identifier.split("@", 1)[0] or None
    return None


# ---------- USAspending: awards ----------
USA_AWARD_API = "https://api.usaspending.gov/api/v2/search/spending_by_award/"


def fetch_usaspending_awards(q: str, since: str, until: str, limit: int) -> List[Dict]:
    page, got, size = 1, [], min(100, limit)
    keywords = split_or(q)
    while len(got) < limit:
        payload = {
            "filters": {
                "keywords": keywords,
                "award_type_codes": ["A", "B", "C", "D"],
                "time_period": [{"start_date": since, "end_date": until}],
            },
            "fields": [
                "Award ID",
                "Recipient Name",
                "Award Amount",
                "Awarding Agency",
                "Description",
                "Action Date",
                "Period of Performance Start Date",
                "Period of Performance Current End Date",
            ],
            "page": page,
            "limit": size,
            "sort": "Award Amount",
            "order": "desc",
        }
        resp = requests.post(USA_AWARD_API, json=payload, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(
                f"USAspending awards {resp.status_code}: {resp.text}"
            )
        results = resp.json().get("results", []) or []
        if not results:
            break
        got += results
        if len(results) < size:
            break
        page += 1
    rows = []
    for result in got[:limit]:
        rows.append(
            {
                "source": "usaspending_awards",
                "id": result.get("Award ID"),
                "title": result.get("Recipient Name"),
                "agency": result.get("Awarding Agency"),
                "date": result.get("Action Date"),
                "amount": result.get("Award Amount"),
                "url": None,
                "text": (result.get("Description") or ""),
                "extra": {
                    "start": result.get("Period of Performance Start Date"),
                    "end": result.get("Period of Performance Current End Date"),
                },
            }
        )
    return rows


# ---------- USAspending: transactions ----------
USA_TXN_API = "https://api.usaspending.gov/api/v2/search/spending_by_transaction/"


def fetch_usaspending_txns(q: str, since: str, until: str, limit: int) -> List[Dict]:
    page, got, size = 1, [], min(100, limit)
    keywords = split_or(q)
    while len(got) < limit:
        payload = {
            "filters": {
                "keywords": keywords,
                "award_type_codes": ["A", "B", "C", "D"],
                "time_period": [{"start_date": since, "end_date": until}],
            },
            "fields": [
                "Transaction Amount",
                "Action Date",
                "Award ID",
                "Recipient Name",
                "Awarding Agency",
                "Description",
            ],
            "page": page,
            "limit": size,
            "sort": "Action Date",
            "order": "desc",
        }
        resp = requests.post(USA_TXN_API, json=payload, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(
                f"USAspending txns {resp.status_code}: {resp.text}"
            )
        results = resp.json().get("results", []) or []
        if not results:
            break
        got += results
        if len(results) < size:
            break
        page += 1
    rows = []
    for result in got[:limit]:
        rows.append(
            {
                "source": "usaspending_txns",
                "id": f"{result.get('Award ID')}@{result.get('Action Date')}",
                "title": result.get("Recipient Name"),
                "agency": result.get("Awarding Agency"),
                "date": result.get("Action Date"),
                "amount": result.get("Transaction Amount"),
                "url": None,
                "text": (result.get("Description") or ""),
                "extra": {},
            }
        )
    return rows


# ---------- SAM.gov notices ----------
SAM_API = "https://api.sam.gov/opportunities/v2/search"


def fetch_sam_notices(q: str, since: str, until: str, limit: int) -> List[Dict]:
    api_key = os.environ.get("SAM_API_KEY")
    if not api_key:
        raise RuntimeError("Set SAM_API_KEY env var for SAM.gov.")
    size = min(100, limit)
    offset = 0
    got = []
    keywords = ",".join(split_or(q))
    while len(got) < limit:
        params = {
            "limit": size,
            "offset": offset,
            "postedFrom": since,
            "postedTo": until,
            "keywords": keywords,
            "ptype": "k",  # broad
        }
        # SAM.gov has historically accepted the API key in the query string,
        # but their gateway intermittently rejects it with an "API_KEY_INVALID"
        # response even when the key is correct.  When that happens we retry
        # with the key provided in the "X-API-KEY" header which is accepted by
        # the newer infrastructure.  Keeping the query parameter first avoids
        # breaking older behaviour while still allowing the request to succeed
        # for the updated gateway.
        resp = requests.get(
            SAM_API,
            params={"api_key": api_key, **params},
            timeout=60,
        )
        if resp.status_code == 403:
            try:
                payload = resp.json()
            except ValueError:
                payload = {}
            if payload.get("code") == "API_KEY_INVALID":
                resp = requests.get(
                    SAM_API,
                    params=params,
                    headers={"X-API-KEY": api_key},
                    timeout=60,
                )
        if resp.status_code != 200:
            raise RuntimeError(f"SAM.gov {resp.status_code}: {resp.text}")
        results = resp.json().get("opportunitiesData") or []
        if not results:
            break
        got += results
        if len(results) < size:
            break
        offset += size
    rows = []
    for result in got[:limit]:
        title = result.get("title") or ""
        desc = result.get("description") or ""
        naics = ",".join(result.get("naics", []) or [])
        agency = result.get("department") or result.get("agency") or ""
        url = result.get("uiLink") or result.get("url")
        rows.append(
            {
                "source": "sam_notices",
                "id": result.get("solicitationNumber") or result.get("noticeId"),
                "title": title,
                "agency": agency,
                "date": result.get("postedDate"),
                "amount": None,
                "url": url,
                "text": " ".join([title, desc, naics]),
                "extra": {"type": result.get("type")},
            }
        )
    return rows


# ---------- orchestrator ----------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hunt multiple gov sources and score against your matrix"
    )
    parser.add_argument("--q", required=True, help='keywords: use "A OR B OR C" or commas')
    parser.add_argument("--since", default="2007-10-01")
    parser.add_argument("--until", default=dt.date.today().isoformat())
    parser.add_argument("--limit", type=int, default=300)
    parser.add_argument(
        "--sources", default="awards,txns,sam", help="comma list: awards,txns,sam"
    )

    # weights
    parser.add_argument("--tech")
    parser.add_argument("--platform")
    parser.add_argument("--org")
    # hard filters
    parser.add_argument("--include")
    parser.add_argument("--exclude")
    parser.add_argument("--agency-like")
    parser.add_argument("--min-amount", type=float, default=0.0)

    parser.add_argument("--csv")
    parser.add_argument("--json")
    args = parser.parse_args()

    tech = blob(args.tech)
    plat = blob(args.platform)
    orgs = blob(args.org)
    need = blob(args.include)
    block = blob(args.exclude)
    srcs = {s.strip().lower() for s in args.sources.split(",") if s.strip()}

    tasks = []
    task_sources: Dict[cf.Future, str] = {}
    with cf.ThreadPoolExecutor(max_workers=3) as executor:
        if "awards" in srcs:
            future = executor.submit(
                fetch_usaspending_awards, args.q, args.since, args.until, args.limit
            )
            tasks.append(future)
            task_sources[future] = "usaspending_awards"
        if "txns" in srcs:
            future = executor.submit(
                fetch_usaspending_txns, args.q, args.since, args.until, args.limit
            )
            tasks.append(future)
            task_sources[future] = "usaspending_txns"
        if "sam" in srcs:
            future = executor.submit(
                fetch_sam_notices, args.q, args.since, args.until, args.limit
            )
            tasks.append(future)
            task_sources[future] = "sam_notices"

        all_rows: List[Dict] = []
        errors: List[str] = []
        for task in cf.as_completed(tasks):
            source = task_sources.get(task, "unknown")
            try:
                all_rows += task.result()
            except Exception as exc:
                errors.append(f"{source}: {exc}")
        if errors:
            print(
                "Some sources failed and were skipped:\n  - "
                + "\n  - ".join(errors),
                file=sys.stderr,
            )

    # filters + scoring
    filtered = []
    for row in all_rows:
        text = row["text"] or ""
        if args.agency_like and (args.agency_like.lower() not in (row["agency"] or "").lower()):
            continue
        if need and not any(hits(text, [required]) for required in need):
            continue
        if block and any(hits(text, [denied]) for denied in block):
            continue
        if args.min_amount and row["amount"] is not None and row["amount"] < args.min_amount:
            continue
        score = score_row(text, tech, plat, orgs)
        merged = dict(row)
        merged["score"] = score
        filtered.append(merged)

    # dedupe by (source,id) first; then keep highest score per Award ID across sources
    seen: Dict[Tuple[str, str], Dict] = {}
    for row in filtered:
        key = (row["source"], row["id"])
        if key not in seen or row["score"] > seen[key]["score"]:
            seen[key] = row
    rows = list(seen.values())

    award_best: Dict[str, Dict] = {}
    passthrough: List[Dict] = []
    for row in rows:
        award_id = shared_award_id(row)
        if award_id is None:
            passthrough.append(row)
            continue
        if award_id not in award_best or row["score"] > award_best[award_id]["score"]:
            award_best[award_id] = row
    rows = passthrough + list(award_best.values())
    rows.sort(
        key=lambda x: (x["score"], x.get("amount") or 0, x.get("date") or ""),
        reverse=True,
    )

    # console top 12
    for idx, row in enumerate(rows[:12], 1):
        amount = (
            f"${row['amount']:,}"
            if isinstance(row["amount"], (int, float))
            else "-"
        )
        print(
            f"{idx:>2}. [{row['source']}] [score {row['score']}] {row['agency']} — {row['title']}  {amount}"
        )
        print(f"    {row['date']}  id={row['id']}  {row['url'] or ''}")
        print(f"    {row['text'][:220].replace(chr(10), ' ')}\n")

    if args.csv and rows:
        with open(args.csv, "w", newline="", encoding="utf-8") as handle:
            fields = [
                "source",
                "score",
                "id",
                "title",
                "agency",
                "date",
                "amount",
                "url",
                "text",
                "extra",
            ]
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                out = dict(row)
                out["extra"] = json.dumps(out.get("extra") or {}, ensure_ascii=False)
                writer.writerow(out)
        print(f"Wrote CSV: {args.csv}")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(rows, handle, ensure_ascii=False, indent=2)
        print(f"Wrote JSON: {args.json}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
