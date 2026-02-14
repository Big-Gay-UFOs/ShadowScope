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