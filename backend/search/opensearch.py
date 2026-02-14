from __future__ import annotations

import os
from typing import Any, Dict

import requests

DEFAULT_OPENSEARCH_URL = "http://opensearch:9200"
DEFAULT_OPENSEARCH_INDEX = "shadowscope-events"


def get_opensearch_config() -> tuple[str, str]:
    url = os.getenv("OPENSEARCH_URL", DEFAULT_OPENSEARCH_URL).rstrip("/")
    index = os.getenv("OPENSEARCH_INDEX", DEFAULT_OPENSEARCH_INDEX)
    return url, index


def opensearch_health(timeout: float = 2.0) -> Dict[str, Any]:
    url, index = get_opensearch_config()
    out: Dict[str, Any] = {"ok": False, "url": url, "index": index}

    try:
        r = requests.get(f"{url}/_cluster/health", timeout=timeout)
        out["http_status"] = r.status_code
        if r.status_code != 200:
            out["error"] = f"cluster_health_http_{r.status_code}"
            return out

        h = r.json()
        out["cluster_status"] = h.get("status")
        out["number_of_nodes"] = h.get("number_of_nodes")
        out["timed_out"] = h.get("timed_out")

        ri = requests.get(f"{url}/{index}", timeout=timeout)
        out["index_exists"] = (ri.status_code == 200)

        out["ok"] = (out["cluster_status"] in ("yellow", "green")) and not bool(out.get("timed_out"))
        return out

    except Exception as e:
        out["error"] = str(e)
        return out
def opensearch_search(
    q: str,
    limit: int = 50,
    source: str | None = None,
    category: str | None = None,
    timeout: float = 5.0,
) -> list[dict]:
    url, index = get_opensearch_config()
    url = url.rstrip("/")

    if not q or not q.strip():
        return []

    body: Dict[str, Any] = {
        "size": max(1, min(int(limit), 200)),
        "query": {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": q,
                            "fields": ["snippet^2", "place_text", "doc_id", "keywords"],
                            "type": "best_fields",
                        }
                    }
                ],
                "filter": [],
            }
        },
        "sort": ["_score"],
    }

    if source:
        body["query"]["bool"]["filter"].append({"term": {"source": source}})
    if category:
        body["query"]["bool"]["filter"].append({"term": {"category": category}})

    r = requests.post(f"{url}/{index}/_search", json=body, timeout=timeout)
    if r.status_code == 404:
        raise RuntimeError(f"OpenSearch index not found: {index}")
    r.raise_for_status()

    data = r.json()
    hits = data.get("hits", {}).get("hits", [])
    out = []
    for h in hits:
        src = h.get("_source", {})
        out.append(
            {
                "score": h.get("_score"),
                "hash": h.get("_id") or src.get("hash"),
                **src,
            }
        )
    return out