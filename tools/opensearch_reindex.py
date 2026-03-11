import argparse
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests import Session
from sqlalchemy import create_engine, text


def env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def make_session() -> Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "ShadowScope/0.1"})
    return s


def req(s: Session, method: str, url: str, *, attempts: int = 6, timeout: int = 30, **kwargs) -> requests.Response:
    last: Optional[Exception] = None
    for i in range(1, attempts + 1):
        try:
            return s.request(method, url, timeout=timeout, **kwargs)
        except requests.RequestException as e:
            last = e
            if i == attempts:
                raise
            time.sleep(min(2 ** (i - 1), 10))
    raise last or RuntimeError("request failed")


def wait_for_opensearch(s: Session, base_url: str, *, timeout_s: int = 90) -> None:
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        try:
            r = req(s, "GET", f"{base_url}/_cluster/health", attempts=1, timeout=10)
            if r.status_code == 200:
                data = r.json()
                status = data.get("status")
                if status in ("yellow", "green") and not data.get("timed_out"):
                    return
                last = f"health={status}"
            else:
                last = f"http={r.status_code}"
        except Exception as e:
            last = str(e)
        time.sleep(2)
    raise SystemExit(f"OpenSearch not ready after {timeout_s}s ({last})")


def ensure_index(s: Session, base_url: str, index: str, recreate: bool) -> None:
    wait_for_opensearch(s, base_url)

    if recreate:
        try:
            req(s, "DELETE", f"{base_url}/{index}", timeout=30)
        except requests.RequestException:
            pass

    r = req(s, "GET", f"{base_url}/{index}", timeout=20)
    if r.status_code == 200:
        return
    if r.status_code not in (404,):
        r.raise_for_status()

    mapping = {
        "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
        "mappings": {
            "properties": {
                "hash": {"type": "keyword"},
                "event_id": {"type": "integer"},
                "category": {"type": "keyword"},
                "source": {"type": "keyword"},
                "doc_id": {"type": "keyword"},
                "document_id": {"type": "keyword"},
                "notice_id": {"type": "keyword"},
                "solicitation_number": {"type": "keyword"},
                "source_url": {"type": "keyword"},
                "award_id": {"type": "keyword"},
                "generated_unique_award_id": {"type": "keyword"},
                "piid": {"type": "keyword"},
                "fain": {"type": "keyword"},
                "uri": {"type": "keyword"},
                "transaction_id": {"type": "keyword"},
                "modification_number": {"type": "keyword"},
                "source_record_id": {"type": "keyword"},
                "recipient_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "recipient_uei": {"type": "keyword"},
                "recipient_parent_uei": {"type": "keyword"},
                "recipient_duns": {"type": "keyword"},
                "recipient_cage_code": {"type": "keyword"},
                "awarding_agency_code": {"type": "keyword"},
                "awarding_agency_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "funding_agency_code": {"type": "keyword"},
                "funding_agency_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "contracting_office_code": {"type": "keyword"},
                "contracting_office_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "psc_code": {"type": "keyword"},
                "psc_description": {"type": "text"},
                "naics_code": {"type": "keyword"},
                "naics_description": {"type": "text"},
                "notice_award_type": {"type": "keyword"},
                "place_of_performance_city": {"type": "keyword"},
                "place_of_performance_state": {"type": "keyword"},
                "place_of_performance_country": {"type": "keyword"},
                "place_of_performance_zip": {"type": "keyword"},
                "occurred_at": {"type": "date"},
                "created_at": {"type": "date"},
                "place_text": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "snippet": {"type": "text"},
                "keywords": {"type": "keyword"},
            }
        },
    }

    cr = req(
        s,
        "PUT",
        f"{base_url}/{index}",
        json=mapping,
        timeout=60,
        headers={"Content-Type": "application/json"},
    )
    cr.raise_for_status()


def normalize_keywords(val: Any) -> List[str]:
    if val is None:
        return []
    if isinstance(val, list):
        out: List[str] = []
        for item in val:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                for k, v in item.items():
                    out.append(f"{k}:{v}")
            else:
                out.append(str(item))
        return out
    if isinstance(val, dict):
        return [f"{k}:{v}" for k, v in val.items()]
    return [str(val)]


def to_iso(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    if hasattr(dt, "isoformat"):
        return dt.isoformat()
    return str(dt)


def get_max_event_id_in_index(s: Session, base_url: str, index: str) -> int:
    r = req(s, "GET", f"{base_url}/{index}", attempts=2, timeout=15)
    if r.status_code != 200:
        return 0

    body = {"size": 0, "aggs": {"max_id": {"max": {"field": "event_id"}}}}
    resp = req(s, "POST", f"{base_url}/{index}/_search", json=body, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    val = data.get("aggregations", {}).get("max_id", {}).get("value")
    if val is None:
        return 0
    try:
        return int(val)
    except Exception:
        return 0


def fetch_batch(conn, *, last_id: int, limit: int) -> List[Dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            select id, hash, category, source, doc_id, source_url, occurred_at, created_at, place_text, snippet, keywords,
                   award_id, generated_unique_award_id, piid, fain, uri, transaction_id, modification_number, source_record_id,
                   recipient_name, recipient_uei, recipient_parent_uei, recipient_duns, recipient_cage_code,
                   awarding_agency_code, awarding_agency_name, funding_agency_code, funding_agency_name,
                   contracting_office_code, contracting_office_name,
                   psc_code, psc_description, naics_code, naics_description, notice_award_type,
                   place_of_performance_city, place_of_performance_state, place_of_performance_country, place_of_performance_zip,
                   solicitation_number, notice_id, document_id
            from events
            where id > :last_id
            order by id asc
            limit :limit
            """
        ),
        {"last_id": last_id, "limit": limit},
    ).mappings().all()
    return list(rows)


def bulk_index(
    s: Session,
    base_url: str,
    index: str,
    db_url: str,
    batch: int,
    *,
    start_id: int,
) -> Tuple[int, int]:
    engine = create_engine(db_url, future=True)
    indexed = 0
    last_id = start_id

    while True:
        with engine.connect() as conn:
            rows = fetch_batch(conn, last_id=last_id, limit=batch)

        if not rows:
            break

        last_id = int(rows[-1]["id"])

        lines: List[str] = []
        for r in rows:
            doc: Dict[str, Any] = {
                "hash": r["hash"],
                "event_id": r["id"],
                "category": r["category"],
                "source": r["source"],
                "doc_id": r["doc_id"],
                "document_id": r["document_id"],
                "notice_id": r["notice_id"],
                "solicitation_number": r["solicitation_number"],
                "source_url": r["source_url"],
                "award_id": r["award_id"],
                "generated_unique_award_id": r["generated_unique_award_id"],
                "piid": r["piid"],
                "fain": r["fain"],
                "uri": r["uri"],
                "transaction_id": r["transaction_id"],
                "modification_number": r["modification_number"],
                "source_record_id": r["source_record_id"],
                "recipient_name": r["recipient_name"],
                "recipient_uei": r["recipient_uei"],
                "recipient_parent_uei": r["recipient_parent_uei"],
                "recipient_duns": r["recipient_duns"],
                "recipient_cage_code": r["recipient_cage_code"],
                "awarding_agency_code": r["awarding_agency_code"],
                "awarding_agency_name": r["awarding_agency_name"],
                "funding_agency_code": r["funding_agency_code"],
                "funding_agency_name": r["funding_agency_name"],
                "contracting_office_code": r["contracting_office_code"],
                "contracting_office_name": r["contracting_office_name"],
                "psc_code": r["psc_code"],
                "psc_description": r["psc_description"],
                "naics_code": r["naics_code"],
                "naics_description": r["naics_description"],
                "notice_award_type": r["notice_award_type"],
                "place_of_performance_city": r["place_of_performance_city"],
                "place_of_performance_state": r["place_of_performance_state"],
                "place_of_performance_country": r["place_of_performance_country"],
                "place_of_performance_zip": r["place_of_performance_zip"],
                "occurred_at": to_iso(r["occurred_at"]),
                "created_at": to_iso(r["created_at"]),
                "place_text": r["place_text"],
                "snippet": r["snippet"],
                "keywords": normalize_keywords(r["keywords"]),
            }
            lines.append(json.dumps({"index": {"_index": index, "_id": r["hash"]}}))
            lines.append(json.dumps(doc, ensure_ascii=False))

        payload = ("\n".join(lines) + "\n").encode("utf-8")

        resp = req(
            s,
            "POST",
            f"{base_url}/_bulk",
            data=payload,
            headers={"Content-Type": "application/x-ndjson"},
            timeout=180,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errors"):
            raise SystemExit(2)

        indexed += len(rows)

    req(s, "POST", f"{base_url}/{index}/_refresh", timeout=60)
    return indexed, last_id


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--opensearch-url", default=env("OPENSEARCH_URL", "http://opensearch:9200"))
    ap.add_argument("--index", default=env("OPENSEARCH_INDEX", "shadowscope-events"))
    ap.add_argument("--database-url", default=env("DATABASE_URL"))
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--recreate", action="store_true", help="Drop and recreate the index (full reindex).")
    ap.add_argument("--full", action="store_true", help="Full reindex without dropping the index.")
    ap.add_argument("--json", action="store_true", help="Print a one-line JSON summary to stdout.")
    args = ap.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL is not set")

    db_hint = args.database_url.split("@")[-1]
    s = make_session()
    ensure_index(s, args.opensearch_url, args.index, recreate=args.recreate)

    if args.recreate or args.full:
        start_id = 0
        mode = "recreate" if args.recreate else "full"
    else:
        start_id = get_max_event_id_in_index(s, args.opensearch_url, args.index)
        mode = "incremental"

    n, last_id = bulk_index(
        s,
        args.opensearch_url,
        args.index,
        args.database_url,
        batch=args.batch,
        start_id=start_id,
    )

    summary = {
        "indexed": n,
        "last_event_id": last_id,
        "index": args.index,
        "opensearch_url": args.opensearch_url,
        "db": db_hint,
        "mode": mode,
        "start_id": start_id,
    }

    if args.json:
        print(json.dumps(summary))
    else:
        print(f"Done. Indexed {n} documents. Last event_id={last_id}")


if __name__ == "__main__":
    main()
