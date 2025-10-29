"""Placeholder SAM.gov connector supporting attachment capture."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, Optional

import requests

from backend.parsers import pdf_text
from backend.runtime import RAW_SOURCES, PARSED_SOURCES, ensure_runtime_directories

LOGGER = logging.getLogger(__name__)


def download_opportunity_attachments(
    notice_id: str,
    attachments: Iterable[Dict[str, str]],
    session: Optional[requests.Session] = None,
) -> Dict[str, Path]:
    ensure_runtime_directories()
    saved: Dict[str, Path] = {}
    if not attachments:
        return saved
    sess = session or requests.Session()
    raw_dir = RAW_SOURCES["sam"] / notice_id
    parsed_dir = PARSED_SOURCES["sam"] / notice_id
    raw_dir.mkdir(parents=True, exist_ok=True)
    parsed_dir.mkdir(parents=True, exist_ok=True)
    for attachment in attachments:
        url = attachment.get("url") or attachment.get("href")
        if not url:
            continue
        filename = attachment.get("filename") or Path(url).name or "attachment.pdf"
        target = raw_dir / filename
        try:
            response = sess.get(url, timeout=60)
            if response.status_code != 200:
                LOGGER.info("SAM attachment %s returned %s", url, response.status_code)
                continue
            target.write_bytes(response.content)
            saved[filename] = target
            try:
                extracted = pdf_text.extract_text_from_pdf(str(target))
            except NotImplementedError:
                LOGGER.debug("PDF extraction not yet available for %s", target)
            else:
                parsed_path = parsed_dir / f"{Path(filename).stem}.txt"
                parsed_path.write_text(extracted.get("text", ""), encoding="utf-8")
        except requests.RequestException as exc:
            LOGGER.warning("Failed to download SAM attachment %s: %s", url, exc)
    return saved


__all__ = ["download_opportunity_attachments"]
