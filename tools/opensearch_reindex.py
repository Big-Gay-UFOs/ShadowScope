import argparse
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from sqlalchemy import create_engine, text


def env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def ensure_index(base_url: str, index: str, recreate: bool) -> None:
    if recreate:
        requests.delete(f"{base_url}/{index}", timeout=30)

    r = requests.get(f"{base_url}/{index}", timeout=30)
    if r.status_code == 200:
        return

    mapping = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
            }
        },
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
                "place_text": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "snippet": {"type": "text"},
                "keywords": {"type": "keyword"},
            }
        },
    }

    cr = requests.put(
        f"{base_url}/{index}",
        json=mapping,
        timeout=60,
        headers={"Content-Type": "application/json"},
    )
    cr.raise_for_status()


def normalize_keywords(val: Any) -> List[str]:
    # Stored as JSON; we accept list/obj and return list[str]
    if val is None:
        return []
    if isinstance(val, list):
        out: List[str] = []
        for item in val:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                # flatten dict entries to "k:v"
                for k, v in item.items():
                    out.append(f"{k}:{v}")
            else:
                out.append(str(item))
        return out
    if isinstance(val, dict):
        out = []
        for k, v in val.items():
            out.append(f"{k}:{v}")
        return out
    return [str(val)]


def to_iso(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    if hasattr(dt, "isoformat"):
        return dt.isoformat()
    return str(dt)


def bulk_index(base_url: str, index: str, db_url: str, batch: int) -> int:
    engine = create_engine(db_url, future=True)

    with engine.connect() as conn:
        total = conn.execute(text("select count(*) from events")).scalar_one()

    indexed = 0
    for offset in range(0, total, batch):
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    select id, hash, category, source, doc_id, source_url,
                           occurred_at, created_at, place_text, snippet, keywords
                    from events
                    order by id asc
                    limit :limit offset :offset
                    """
                ),
                {"limit": batch, "offset": offset},
            ).mappings().all()

        if not rows:
            break

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

            # Use event hash as _id so indexing is idempotent
            lines.append(json.dumps({"index": {"_index": index, "_id": r["hash"]}}))
            lines.append(json.dumps(doc, ensure_ascii=False))

        payload = "\n".join(lines) + "\n"
        resp = requests.post(
            f"{base_url}/_bulk",
            data=payload.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errors"):
            # Print first few failures to help debug mapping issues
            bad = [it for it in data.get("items", []) if list(it.values())[0].get("error")]
            print("Bulk indexing errors detected. First 3:", bad[:3])
            raise SystemExit(2)

        indexed += len(rows)
        if indexed % (batch * 5) == 0:
            print(f"Indexed {indexed}/{total}...")

    # refresh for immediate queryability
    requests.post(f"{base_url}/{index}/_refresh", timeout=60)
    return indexed


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--opensearch-url", default=env("OPENSEARCH_URL", "http://opensearch:9200"))
    ap.add_argument("--index", default=env("OPENSEARCH_INDEX", "shadowscope-events"))
    ap.add_argument("--database-url", default=env("DATABASE_URL"))
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--recreate", action="store_true")
    args = ap.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL is not set")

    print("OpenSearch:", args.opensearch_url)
    print("Index:", args.index)
    print("DB:", args.database_url.split("@")[-1])

    ensure_index(args.opensearch_url, args.index, recreate=args.recreate)
    n = bulk_index(args.opensearch_url, args.index, args.database_url, batch=args.batch)
    print(f"Done. Indexed {n} documents.")


if __name__ == "__main__":
    main()
