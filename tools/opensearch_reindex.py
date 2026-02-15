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
        # ignore failures if missing
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
                "source_url": {"type": "keyword"},
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
    # If index doesn't exist, treat as empty
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
            select id, hash, category, source, doc_id, source_url, occurred_at, created_at, place_text, snippet, keywords
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

    with engine.connect() as conn:
        total = conn.execute(text("select count(*) from events")).scalar_one()
        max_id = conn.execute(text("select coalesce(max(id),0) from events")).scalar_one()

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
                "source_url": r["source_url"],
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
            bad = [it for it in data.get("items", []) if list(it.values())[0].get("error")]
            print("Bulk indexing errors detected. First 3:", bad[:3])
            raise SystemExit(2)

        indexed += len(rows)

        if indexed and indexed % (batch * 5) == 0:
            print(f"Indexed {indexed} rows... (db max_id={max_id}, db total={total})")

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
    args = ap.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL is not set")

    # Avoid leaking creds in output
    db_hint = args.database_url.split("@")[-1]
    print("OpenSearch:", args.opensearch_url)
    print("Index:", args.index)
    print("DB:", db_hint)

    s = make_session()
    ensure_index(s, args.opensearch_url, args.index, recreate=args.recreate)

    if args.recreate or args.full:
        start_id = 0
    else:
        start_id = get_max_event_id_in_index(s, args.opensearch_url, args.index)

    if start_id:
        print(f"Incremental mode: indexing events with id > {start_id}")
    else:
        print("Full mode: indexing from id 1")

    n, last_id = bulk_index(
        s,
        args.opensearch_url,
        args.index,
        args.database_url,
        batch=args.batch,
        start_id=start_id,
    )
    summary = {"indexed": n, "last_event_id": last_id, "index": args.index, "opensearch_url": args.opensearch_url, "db": db_hint, "mode": ("recreate" if args.recreate else ("full" if args.full else "incremental")), "start_id": start_id}
if args.json:
    print(json.dumps(summary))
else:
    print(f"Done. Indexed {n} documents. Last event_id={last_id}")


if __name__ == "__main__":
    main()