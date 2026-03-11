from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.connectors.samgov_context import extract_sam_context_fields
from backend.db.models import Correlation, CorrelationLink, Entity, Event, get_session_factory


def not_implemented() -> None:
    raise NotImplementedError("Correlation engine placeholder was replaced by rebuild_entity_correlations().")


def rebuild_entity_correlations(
    *,
    window_days: int = 30,
    source: Optional[str] = "USAspending",
    min_events: int = 2,
    radius_km: float = 0.0,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Idempotent rebuild (same-entity lane) using correlation_key:
      key format: same_entity|<source>|<window_days>|entity:<entity_id>

    - Updates existing correlations if key exists
    - Creates new ones if missing
    - Deletes stale ones for this lane/window/source
    """
    window_days = int(window_days)
    min_events = int(min_events)
    if window_days <= 0:
        raise ValueError("window_days must be > 0")
    if min_events < 2:
        raise ValueError("min_events must be >= 2")

    lane = "same_entity"
    src_key = source if source else "*"
    key_prefix = f"{lane}|{src_key}|{window_days}|entity:"

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=window_days)

    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()

    ts = func.coalesce(Event.occurred_at, Event.created_at)

    try:
        groups_q = (
            db.query(Event.entity_id, func.count(Event.id))
            .filter(Event.entity_id.isnot(None))
            .filter(ts >= since)
        )
        if source:
            groups_q = groups_q.filter(Event.source == source)
        groups = groups_q.group_by(Event.entity_id).all()

        eligible = [(int(eid), int(cnt)) for (eid, cnt) in groups if int(cnt) >= min_events]
        eligible_keys = set(f"{key_prefix}{eid}" for (eid, _cnt) in eligible)

        # Load existing correlations for this lane/window/source
        existing = (
            db.query(Correlation)
            .filter(Correlation.correlation_key.like(f"{key_prefix}%"))
            .all()
        )
        existing_by_key = {c.correlation_key: c for c in existing if c.correlation_key}

        correlations_created = 0
        correlations_updated = 0
        correlations_deleted = 0
        links_created = 0
        links_deleted = 0

        for (entity_id, cnt) in eligible:
            key = f"{key_prefix}{entity_id}"

            ent = db.get(Entity, int(entity_id))
            ent_name = ent.name if ent else f"entity_id={entity_id}"
            ent_uei = ent.uei if ent else None

            lanes_hit = {
                "lane": lane,
                "entity_id": int(entity_id),
                "uei": ent_uei,
                "event_count": cnt,
                "key_count": cnt,
                "since": since.isoformat(),
                "until": now.isoformat(),
            }

            if dry_run:
                continue

            c = existing_by_key.get(key)
            if c is None:
                c = Correlation(
                    correlation_key=key,
                    score=str(cnt),
                    window_days=window_days,
                    radius_km=float(radius_km),
                    lanes_hit=lanes_hit,
                    summary=f"{cnt} events share entity {ent_name}",
                    rationale=f"Grouped events with entity_id={entity_id} within last {window_days} days (min_events={min_events}).",
                    created_at=now,
                )
                db.add(c)
                db.flush()
                correlations_created += 1
            else:
                c.score = str(cnt)
                c.window_days = window_days
                c.radius_km = float(radius_km)
                c.lanes_hit = lanes_hit
                c.summary = f"{cnt} events share entity {ent_name}"
                c.rationale = f"Grouped events with entity_id={entity_id} within last {window_days} days (min_events={min_events})."
                c.created_at = now
                correlations_updated += 1

            # rebuild links for this correlation
            links_deleted += db.query(CorrelationLink).filter(CorrelationLink.correlation_id == int(c.id)).delete(synchronize_session=False)

            ids_q = db.query(Event.id).filter(Event.entity_id == int(entity_id)).filter(ts >= since)
            if source:
                ids_q = ids_q.filter(Event.source == source)
            event_ids = [r[0] for r in ids_q.all()]

            for ev_id in event_ids:
                db.add(CorrelationLink(correlation_id=int(c.id), event_id=int(ev_id)))
            links_created += len(event_ids)

        # Delete stale correlations for this lane/window/source
        if not dry_run:
            stale_keys = [k for k in existing_by_key.keys() if k not in eligible_keys]
            if stale_keys:
                correlations_deleted = db.query(Correlation).filter(Correlation.correlation_key.in_(stale_keys)).delete(synchronize_session=False)

        if not dry_run:
            db.commit()

        return {
            "status": "ok",
            "dry_run": dry_run,
            "source": source,
            "window_days": window_days,
            "min_events": min_events,
            "since": since.isoformat(),
            "entities_seen": len(groups),
            "eligible_entities": len(eligible),
            "correlations_created": correlations_created,
            "correlations_updated": correlations_updated,
            "correlations_deleted": correlations_deleted,
            "links_created": links_created,
            "links_deleted": links_deleted,
        }
    finally:
        db.close()
def rebuild_uei_correlations(
    *,
    window_days: int = 30,
    source: Optional[str] = "USAspending",
    min_events: int = 2,
    radius_km: float = 0.0,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Idempotent rebuild (same-UEI lane) using correlation_key:
      key format: same_uei|<source>|<window_days>|uei:<UEI>

    Uses UEI from Event.raw_json (USAspending: "Recipient UEI").
    """
    window_days = int(window_days)
    min_events = int(min_events)
    if window_days <= 0:
        raise ValueError("window_days must be > 0")
    if min_events < 2:
        raise ValueError("min_events must be >= 2")

    lane = "same_uei"
    src_key = source if source else "*"
    key_prefix = f"{lane}|{src_key}|{window_days}|uei:"

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=window_days)

    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()

    ts = func.coalesce(Event.occurred_at, Event.created_at)

    try:
        q = db.query(Event).filter(ts >= since)
        if source:
            q = q.filter(Event.source == source)
        rows = q.order_by(Event.id.asc()).all()

        # group events by UEI
        uei_to_event_ids: Dict[str, List[int]] = {}
        for ev in rows:
            raw = ev.raw_json if isinstance(ev.raw_json, dict) else {}
            u = ev.recipient_uei or raw.get("Recipient UEI") or raw.get("recipient_uei") or raw.get("uei")
            if not u:
                continue
            uei = str(u).strip().upper()
            if not uei:
                continue
            uei_to_event_ids.setdefault(uei, []).append(int(ev.id))

        # eligible UEIs
        eligible: Dict[str, List[int]] = {}
        for uei, ids in uei_to_event_ids.items():
            uniq = sorted(set(ids))
            if len(uniq) >= min_events:
                eligible[uei] = uniq

        eligible_keys = set(f"{key_prefix}{uei}" for uei in eligible.keys())

        existing = (
            db.query(Correlation)
            .filter(Correlation.correlation_key.like(f"{key_prefix}%"))
            .all()
        )
        existing_by_key = {c.correlation_key: c for c in existing if c.correlation_key}

        correlations_created = 0
        correlations_updated = 0
        correlations_deleted = 0
        links_created = 0
        links_deleted = 0

        for uei, event_ids in eligible.items():
            key = f"{key_prefix}{uei}"
            cnt = len(event_ids)

            lanes_hit = {
                "lane": lane,
                "uei": uei,
                "event_count": cnt,
                "key_count": cnt,
                "since": since.isoformat(),
                "until": now.isoformat(),
            }

            if dry_run:
                continue

            c = existing_by_key.get(key)
            if c is None:
                c = Correlation(
                    correlation_key=key,
                    score=str(cnt),
                    window_days=window_days,
                    radius_km=float(radius_km),
                    lanes_hit=lanes_hit,
                    summary=f"{cnt} events share UEI {uei}",
                    rationale=f"Grouped events with UEI={uei} within last {window_days} days (min_events={min_events}).",
                    created_at=now,
                )
                db.add(c)
                db.flush()
                correlations_created += 1
            else:
                c.score = str(cnt)
                c.window_days = window_days
                c.radius_km = float(radius_km)
                c.lanes_hit = lanes_hit
                c.summary = f"{cnt} events share UEI {uei}"
                c.rationale = f"Grouped events with UEI={uei} within last {window_days} days (min_events={min_events})."
                c.created_at = now
                correlations_updated += 1

            # rebuild links
            links_deleted += (
                db.query(CorrelationLink)
                .filter(CorrelationLink.correlation_id == int(c.id))
                .delete(synchronize_session=False)
            )
            for ev_id in event_ids:
                db.add(CorrelationLink(correlation_id=int(c.id), event_id=int(ev_id)))
            links_created += len(event_ids)

        # delete stale correlations for this lane/source/window
        if not dry_run:
            stale_keys = [k for k in existing_by_key.keys() if k not in eligible_keys]
            if stale_keys:
                correlations_deleted = (
                    db.query(Correlation)
                    .filter(Correlation.correlation_key.in_(stale_keys))
                    .delete(synchronize_session=False)
                )

        if not dry_run:
            db.commit()

        return {
            "status": "ok",
            "dry_run": dry_run,
            "source": source,
            "window_days": window_days,
            "min_events": min_events,
            "since": since.isoformat(),
            "ueis_seen": len(uei_to_event_ids),
            "eligible_ueis": len(eligible),
            "correlations_created": correlations_created,
            "correlations_updated": correlations_updated,
            "correlations_deleted": correlations_deleted,
            "links_created": links_created,
            "links_deleted": links_deleted,
        }
    finally:
        db.close()
def rebuild_keyword_correlations(
    *,
    window_days: int = 30,
    source: Optional[str] = "USAspending",
    min_events: int = 3,
    max_events: int = 200,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Idempotent rebuild (same-keyword lane) using correlation_key:
      key format: same_keyword|<source>|<window_days>|kw:<keyword>

    Uses Event.keywords (list of "pack:rule") populated by ontology apply.
    """
    window_days = int(window_days)
    min_events = int(min_events)
    max_events = int(max_events)

    if window_days <= 0:
        raise ValueError("window_days must be > 0")
    if min_events < 2:
        raise ValueError("min_events must be >= 2")
    if max_events < min_events:
        raise ValueError("max_events must be >= min_events")

    lane = "same_keyword"
    src_key = source if source else "*"
    key_prefix = f"{lane}|{src_key}|{window_days}|kw:"

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=window_days)

    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()

    ts = func.coalesce(Event.occurred_at, Event.created_at)

    try:
        q = db.query(Event).filter(ts >= since)
        if source:
            q = q.filter(Event.source == source)
        rows = q.order_by(Event.id.asc()).all()

        # Build keyword -> event_ids index
        kw_to_ids: dict[str, list[int]] = {}
        for ev in rows:
            kws = ev.keywords if isinstance(ev.keywords, list) else []
            for kw in kws:
                if not isinstance(kw, str):
                    continue
                kw_norm = kw.strip()
                if not kw_norm:
                    continue
                # normalize to stabilize keys
                kw_key = kw_norm.replace("|", "/").lower()
                kw_to_ids.setdefault(kw_key, []).append(int(ev.id))

        eligible: dict[str, list[int]] = {}
        for kw, ids in kw_to_ids.items():
            uniq = sorted(set(ids))
            if len(uniq) >= min_events and len(uniq) <= max_events:
                eligible[kw] = uniq

        eligible_keys = set(f"{key_prefix}{kw}" for kw in eligible.keys())

        existing = (
            db.query(Correlation)
            .filter(Correlation.correlation_key.like(f"{key_prefix}%"))
            .all()
        )
        existing_by_key = {c.correlation_key: c for c in existing if c.correlation_key}

        correlations_created = 0
        correlations_updated = 0
        correlations_deleted = 0
        links_created = 0
        links_deleted = 0

        for kw, event_ids in eligible.items():
            key = f"{key_prefix}{kw}"
            cnt = len(event_ids)

            lanes_hit = {
                "lane": lane,
                "keyword": kw,
                "event_count": cnt,
                "key_count": cnt,
                "since": since.isoformat(),
                "until": now.isoformat(),
            }

            if dry_run:
                continue

            c = existing_by_key.get(key)
            if c is None:
                c = Correlation(
                    correlation_key=key,
                    score=str(cnt),
                    window_days=window_days,
                    radius_km=0.0,
                    lanes_hit=lanes_hit,
                    summary=f"{cnt} events share keyword {kw}",
                    rationale=f"Grouped events sharing keyword '{kw}' within last {window_days} days (min_events={min_events}, max_events={max_events}).",
                    created_at=now,
                )
                db.add(c)
                db.flush()
                correlations_created += 1
            else:
                c.score = str(cnt)
                c.window_days = window_days
                c.radius_km = 0.0
                c.lanes_hit = lanes_hit
                c.summary = f"{cnt} events share keyword {kw}"
                c.rationale = f"Grouped events sharing keyword '{kw}' within last {window_days} days (min_events={min_events}, max_events={max_events})."
                c.created_at = now
                correlations_updated += 1

            # rebuild links
            links_deleted += (
                db.query(CorrelationLink)
                .filter(CorrelationLink.correlation_id == int(c.id))
                .delete(synchronize_session=False)
            )
            for ev_id in event_ids:
                db.add(CorrelationLink(correlation_id=int(c.id), event_id=int(ev_id)))
            links_created += len(event_ids)

        # delete stale correlations for this lane/source/window
        if not dry_run:
            stale_keys = [k for k in existing_by_key.keys() if k not in eligible_keys]
            if stale_keys:
                correlations_deleted = (
                    db.query(Correlation)
                    .filter(Correlation.correlation_key.in_(stale_keys))
                    .delete(synchronize_session=False)
                )

        if not dry_run:
            db.commit()

        return {
            "status": "ok",
            "dry_run": dry_run,
            "source": source,
            "window_days": window_days,
            "min_events": min_events,
            "max_events": max_events,
            "since": since.isoformat(),
            "keywords_seen": len(kw_to_ids),
            "eligible_keywords": len(eligible),
            "correlations_created": correlations_created,
            "correlations_updated": correlations_updated,
            "correlations_deleted": correlations_deleted,
            "links_created": links_created,
            "links_deleted": links_deleted,
        }
    finally:
        db.close()

def rebuild_keyword_pair_correlations(
    *,
    window_days: int = 30,
    source: Optional[str] = "USAspending",
    min_events: int = 3,
    max_events: int = 200,
    max_keywords_per_event: int = 10,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Idempotent rebuild (keyword-pair lane) using correlation_key.

    key format: kw_pair|<source>|<window_days>|pair:<hash16>
    - Stores kw1/kw2 in lanes_hit + summary for readability.
    - Links correlation -> all events that contain BOTH keywords.
    """
    import hashlib

    window_days = int(window_days)
    min_events = int(min_events)
    max_events = int(max_events)
    max_keywords_per_event = int(max_keywords_per_event)

    if window_days <= 0:
        raise ValueError("window_days must be > 0")
    if min_events < 2:
        raise ValueError("min_events must be >= 2")
    if max_events < min_events:
        raise ValueError("max_events must be >= min_events")
    if max_keywords_per_event < 2:
        raise ValueError("max_keywords_per_event must be >= 2")

    lane = "kw_pair"
    src_key = source if source else "*"
    key_prefix = f"{lane}|{src_key}|{window_days}|pair:"

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=window_days)

    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()
    ts = func.coalesce(Event.occurred_at, Event.created_at)

    try:
        q = db.query(Event).filter(ts >= since)
        if source:
            q = q.filter(Event.source == source)
        rows = q.order_by(Event.id.asc()).all()

        # Build pair -> event_ids index
        pair_to_ids: dict[str, list[int]] = {}

        def norm_kw(s: str) -> str:
            return s.strip().replace("|", "/").lower()

        for ev in rows:
            kws = ev.keywords if isinstance(ev.keywords, list) else []
            normed = []
            for kw in kws:
                if isinstance(kw, str):
                    k = norm_kw(kw)
                    if k:
                        normed.append(k)
            kw_list = sorted(set(normed))
            if len(kw_list) < 2:
                continue
            if len(kw_list) > max_keywords_per_event:
                continue

            ev_id = int(ev.id)
            for i in range(len(kw_list)):
                for j in range(i + 1, len(kw_list)):
                    pair = f"{kw_list[i]}+{kw_list[j]}"
                    pair_to_ids.setdefault(pair, []).append(ev_id)

        # Eligible pairs (within min/max event counts)
        eligible: dict[str, list[int]] = {}
        pair_hash: dict[str, str] = {}
        for pair, ids in pair_to_ids.items():
            uniq = sorted(set(ids))
            if len(uniq) >= min_events and len(uniq) <= max_events:
                eligible[pair] = uniq
                pair_hash[pair] = hashlib.sha1(pair.encode("utf-8")).hexdigest()[:16]

        eligible_keys = set(f"{key_prefix}{pair_hash[p]}" for p in eligible.keys())

        existing = (
            db.query(Correlation)
            .filter(Correlation.correlation_key.like(f"{key_prefix}%"))
            .all()
        )
        existing_by_key = {c.correlation_key: c for c in existing if c.correlation_key}

        correlations_created = 0
        correlations_updated = 0
        correlations_deleted = 0
        links_created = 0
        links_deleted = 0

        for pair, event_ids in eligible.items():
            h = pair_hash[pair]
            key = f"{key_prefix}{h}"
            cnt = len(event_ids)

            parts = pair.split("+", 1)
            kw1 = parts[0]
            kw2 = parts[1] if len(parts) > 1 else ""

            lanes_hit = {
                "lane": lane,
                "keyword_1": kw1,
                "keyword_2": kw2,
                "pair_hash": h,
                "event_count": cnt,
                "key_count": cnt,
                "since": since.isoformat(),
                "until": now.isoformat(),
            }

            if dry_run:
                continue

            c = existing_by_key.get(key)
            if c is None:
                c = Correlation(
                    correlation_key=key,
                    score=str(cnt),
                    window_days=window_days,
                    radius_km=0.0,
                    lanes_hit=lanes_hit,
                    summary=f"{cnt} events share keyword-pair {kw1} + {kw2}",
                    rationale=f"Grouped events sharing keyword-pair within last {window_days} days (min_events={min_events}, max_events={max_events}, max_keywords_per_event={max_keywords_per_event}).",
                    created_at=now,
                )
                db.add(c)
                db.flush()
                correlations_created += 1
            else:
                c.score = str(cnt)
                c.window_days = window_days
                c.radius_km = 0.0
                c.lanes_hit = lanes_hit
                c.summary = f"{cnt} events share keyword-pair {kw1} + {kw2}"
                c.rationale = f"Grouped events sharing keyword-pair within last {window_days} days (min_events={min_events}, max_events={max_events}, max_keywords_per_event={max_keywords_per_event})."
                c.created_at = now
                correlations_updated += 1

            # rebuild links
            links_deleted += (
                db.query(CorrelationLink)
                .filter(CorrelationLink.correlation_id == int(c.id))
                .delete(synchronize_session=False)
            )
            for ev_id in event_ids:
                db.add(CorrelationLink(correlation_id=int(c.id), event_id=int(ev_id)))
            links_created += len(event_ids)

        # delete stale correlations
        if not dry_run:
            stale_keys = [k for k in existing_by_key.keys() if k not in eligible_keys]
            if stale_keys:
                correlations_deleted = (
                    db.query(Correlation)
                    .filter(Correlation.correlation_key.in_(stale_keys))
                    .delete(synchronize_session=False)
                )

        if not dry_run:
            db.commit()

        return {
            "status": "ok",
            "dry_run": dry_run,
            "source": source,
            "window_days": window_days,
            "min_events": min_events,
            "max_events": max_events,
            "max_keywords_per_event": max_keywords_per_event,
            "since": since.isoformat(),
            "pairs_seen": len(pair_to_ids),
            "eligible_pairs": len(eligible),
            "correlations_created": correlations_created,
            "correlations_updated": correlations_updated,
            "correlations_deleted": correlations_deleted,
            "links_created": links_created,
            "links_deleted": links_deleted,
        }
    finally:
        db.close()


def rebuild_sam_naics_correlations(
    *,
    window_days: int = 30,
    source: Optional[str] = "SAM.gov",
    min_events: int = 2,
    max_events: int = 200,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Idempotent rebuild (SAM NAICS lane) using correlation_key:
      key format: same_sam_naics|<source>|<window_days>|naics:<code>

    Uses canonical SAM context fields persisted in Event.raw_json.
    """
    window_days = int(window_days)
    min_events = int(min_events)
    max_events = int(max_events)

    if window_days <= 0:
        raise ValueError("window_days must be > 0")
    if min_events < 2:
        raise ValueError("min_events must be >= 2")
    if max_events < min_events:
        raise ValueError("max_events must be >= min_events")

    lane = "same_sam_naics"
    src_key = source if source else "*"
    key_prefix = f"{lane}|{src_key}|{window_days}|naics:"

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=window_days)

    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()
    ts = func.coalesce(Event.occurred_at, Event.created_at)

    try:
        q = db.query(Event).filter(ts >= since)
        if source:
            q = q.filter(Event.source == source)
        rows = q.order_by(Event.id.asc()).all()

        naics_to_event_ids: Dict[str, List[int]] = {}
        naics_to_desc_counts: Dict[str, Dict[str, int]] = {}

        for ev in rows:
            raw = ev.raw_json if isinstance(ev.raw_json, dict) else {}
            ctx = extract_sam_context_fields(raw)
            naics = ctx.get("sam_naics_code")
            if not naics:
                continue
            naics_code = str(naics).strip().upper()
            if not naics_code:
                continue

            naics_to_event_ids.setdefault(naics_code, []).append(int(ev.id))

            naics_desc = ctx.get("sam_naics_description")
            if naics_desc:
                desc_text = str(naics_desc).strip()
                if desc_text:
                    desc_counts = naics_to_desc_counts.setdefault(naics_code, {})
                    desc_counts[desc_text] = desc_counts.get(desc_text, 0) + 1

        eligible: Dict[str, List[int]] = {}
        capped_naics: Dict[str, int] = {}
        key_counts: Dict[str, int] = {}
        for naics_code, ids in naics_to_event_ids.items():
            uniq = sorted(set(ids))
            key_counts[naics_code] = len(uniq)
            if len(uniq) > max_events:
                capped_naics[naics_code] = len(uniq)
                continue
            if len(uniq) >= min_events:
                eligible[naics_code] = uniq

        eligible_keys = set(f"{key_prefix}{naics_code}" for naics_code in eligible.keys())

        existing = (
            db.query(Correlation)
            .filter(Correlation.correlation_key.like(f"{key_prefix}%"))
            .all()
        )
        existing_by_key = {c.correlation_key: c for c in existing if c.correlation_key}

        correlations_created = 0
        correlations_updated = 0
        correlations_deleted = 0
        links_created = 0
        links_deleted = 0

        for naics_code, event_ids in eligible.items():
            key = f"{key_prefix}{naics_code}"
            cnt = len(event_ids)

            desc_counts = naics_to_desc_counts.get(naics_code) or {}
            top_desc = None
            if desc_counts:
                top_desc = sorted(desc_counts.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)[0][0]

            lanes_hit = {
                "lane": lane,
                "naics_code": naics_code,
                "naics_description": top_desc,
                "event_count": cnt,
                "key_count": cnt,
                "since": since.isoformat(),
                "until": now.isoformat(),
            }

            summary_bits = [f"{cnt} events share SAM NAICS {naics_code}"]
            if top_desc:
                summary_bits.append(f"({top_desc})")
            summary = " ".join(summary_bits)

            if dry_run:
                continue

            c = existing_by_key.get(key)
            if c is None:
                c = Correlation(
                    correlation_key=key,
                    score=str(cnt),
                    window_days=window_days,
                    radius_km=0.0,
                    lanes_hit=lanes_hit,
                    summary=summary,
                    rationale=(
                        f"Grouped SAM events sharing NAICS={naics_code} within last {window_days} days "
                        f"(min_events={min_events}, max_events={max_events})."
                    ),
                    created_at=now,
                )
                db.add(c)
                db.flush()
                correlations_created += 1
            else:
                c.score = str(cnt)
                c.window_days = window_days
                c.radius_km = 0.0
                c.lanes_hit = lanes_hit
                c.summary = summary
                c.rationale = (
                    f"Grouped SAM events sharing NAICS={naics_code} within last {window_days} days "
                    f"(min_events={min_events}, max_events={max_events})."
                )
                c.created_at = now
                correlations_updated += 1

            links_deleted += (
                db.query(CorrelationLink)
                .filter(CorrelationLink.correlation_id == int(c.id))
                .delete(synchronize_session=False)
            )
            for ev_id in event_ids:
                db.add(CorrelationLink(correlation_id=int(c.id), event_id=int(ev_id)))
            links_created += len(event_ids)

        if not dry_run:
            stale_keys = [k for k in existing_by_key.keys() if k not in eligible_keys]
            if stale_keys:
                correlations_deleted = (
                    db.query(Correlation)
                    .filter(Correlation.correlation_key.in_(stale_keys))
                    .delete(synchronize_session=False)
                )

        if not dry_run:
            db.commit()

        return {
            "status": "ok",
            "dry_run": dry_run,
            "source": source,
            "window_days": window_days,
            "min_events": min_events,
            "max_events": max_events,
            "since": since.isoformat(),
            "naics_seen": len(naics_to_event_ids),
            "eligible_naics": len(eligible),
            "keys_capped": len(capped_naics),
            "top_key_counts": [
                {
                    "key": code,
                    "count": int(count),
                    "status": "capped" if code in capped_naics else "eligible" if code in eligible else "below_min",
                }
                for code, count in sorted(key_counts.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)[:20]
            ],
            "correlations_created": correlations_created,
            "correlations_updated": correlations_updated,
            "correlations_deleted": correlations_deleted,
            "links_created": links_created,
            "links_deleted": links_deleted,
        }
    finally:
        db.close()




def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        v = value.strip()
        return (not v) or (v.lower() in {"null", "none", "n/a", "nan"})
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) == 0
    return False


def _raw_ci_get(raw: Dict[str, Any], key: str) -> Any:
    if key in raw:
        return raw.get(key)
    k = str(key).lower()
    for rk, rv in raw.items():
        if str(rk).lower() == k:
            return rv
    return None


def _event_value(ev: Event, attrs: Tuple[str, ...], raw_keys: Tuple[str, ...] = ()) -> Any:
    for attr in attrs:
        value = getattr(ev, attr, None)
        if not _is_blank(value):
            return value

    raw = ev.raw_json if isinstance(ev.raw_json, dict) else {}
    for key in raw_keys:
        value = _raw_ci_get(raw, key)
        if not _is_blank(value):
            return value

    return None


def _clean_key_token(value: Any, *, upper: bool = False, lower: bool = False) -> Optional[str]:
    if _is_blank(value):
        return None
    v = str(value).strip().replace("|", "/")
    if not v:
        return None
    if upper:
        return v.upper()
    if lower:
        return v.lower()
    return v


def _corr_token(value: str) -> str:
    token = str(value).strip().replace("|", "/")
    if len(token) <= 120:
        return token
    import hashlib

    digest = hashlib.sha1(token.encode("utf-8")).hexdigest()[:20]
    return f"sha1:{digest}"


def _rebuild_same_field_correlations(
    *,
    lane: str,
    key_segment: str,
    value_label: str,
    extractor: Callable[[Event], Tuple[Optional[str], Dict[str, Any]]],
    window_days: int,
    source: Optional[str],
    min_events: int,
    max_events: int,
    radius_km: float = 0.0,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    window_days = int(window_days)
    min_events = int(min_events)
    max_events = int(max_events)

    if window_days <= 0:
        raise ValueError("window_days must be > 0")
    if min_events < 2:
        raise ValueError("min_events must be >= 2")
    if max_events < min_events:
        raise ValueError("max_events must be >= min_events")

    src_key = source if source else "*"
    key_prefix = f"{lane}|{src_key}|{window_days}|{key_segment}:"

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=window_days)

    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()
    ts = func.coalesce(Event.occurred_at, Event.created_at)

    try:
        q = db.query(Event).filter(ts >= since)
        if source:
            q = q.filter(Event.source == source)
        rows = q.order_by(Event.id.asc()).all()

        key_to_ids: Dict[str, List[int]] = {}
        key_meta: Dict[str, Dict[str, Any]] = {}

        for ev in rows:
            lane_key, meta = extractor(ev)
            if _is_blank(lane_key):
                continue
            lane_key = _corr_token(str(lane_key))
            key_to_ids.setdefault(lane_key, []).append(int(ev.id))
            if lane_key not in key_meta:
                key_meta[lane_key] = dict(meta or {})
            else:
                cur_meta = key_meta[lane_key]
                for mk, mv in (meta or {}).items():
                    if _is_blank(cur_meta.get(mk)) and not _is_blank(mv):
                        cur_meta[mk] = mv

        key_counts: Dict[str, int] = {k: len(set(ids)) for k, ids in key_to_ids.items()}
        capped_counts = {k: c for k, c in key_counts.items() if c > max_events}

        eligible: Dict[str, List[int]] = {
            k: sorted(set(key_to_ids[k]))
            for k, c in key_counts.items()
            if c >= min_events and c <= max_events
        }
        eligible_keys = set(f"{key_prefix}{k}" for k in eligible.keys())

        existing = (
            db.query(Correlation)
            .filter(Correlation.correlation_key.like(f"{key_prefix}%"))
            .all()
        )
        existing_by_key = {c.correlation_key: c for c in existing if c.correlation_key}

        correlations_created = 0
        correlations_updated = 0
        correlations_deleted = 0
        links_created = 0
        links_deleted = 0

        for lane_key, event_ids in eligible.items():
            key = f"{key_prefix}{lane_key}"
            cnt = len(event_ids)
            meta = dict(key_meta.get(lane_key) or {})

            lanes_hit = {
                "lane": lane,
                value_label: lane_key,
                "event_count": cnt,
                "key_count": cnt,
                "since": since.isoformat(),
                "until": now.isoformat(),
            }
            for mk, mv in meta.items():
                if not _is_blank(mv):
                    lanes_hit[mk] = mv

            display_value = meta.get(value_label) or lane_key
            summary = f"{cnt} events share {value_label} {display_value}"
            rationale = (
                f"Grouped events by {value_label} within last {window_days} days "
                f"(min_events={min_events}, max_events={max_events})."
            )

            if dry_run:
                continue

            c = existing_by_key.get(key)
            if c is None:
                c = Correlation(
                    correlation_key=key,
                    score=str(cnt),
                    window_days=window_days,
                    radius_km=float(radius_km),
                    lanes_hit=lanes_hit,
                    summary=summary,
                    rationale=rationale,
                    created_at=now,
                )
                db.add(c)
                db.flush()
                correlations_created += 1
            else:
                c.score = str(cnt)
                c.window_days = window_days
                c.radius_km = float(radius_km)
                c.lanes_hit = lanes_hit
                c.summary = summary
                c.rationale = rationale
                c.created_at = now
                correlations_updated += 1

            links_deleted += (
                db.query(CorrelationLink)
                .filter(CorrelationLink.correlation_id == int(c.id))
                .delete(synchronize_session=False)
            )
            for ev_id in event_ids:
                db.add(CorrelationLink(correlation_id=int(c.id), event_id=int(ev_id)))
            links_created += len(event_ids)

        if not dry_run:
            stale_keys = [k for k in existing_by_key.keys() if k not in eligible_keys]
            if stale_keys:
                correlations_deleted = (
                    db.query(Correlation)
                    .filter(Correlation.correlation_key.in_(stale_keys))
                    .delete(synchronize_session=False)
                )

        if not dry_run:
            db.commit()

        top_key_counts = [
            {
                "key": key,
                "count": int(count),
                "status": "capped" if key in capped_counts else "eligible" if key in eligible else "below_min",
            }
            for key, count in sorted(key_counts.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)[:20]
        ]

        return {
            "status": "ok",
            "dry_run": dry_run,
            "source": source,
            "window_days": window_days,
            "min_events": min_events,
            "max_events": max_events,
            "since": since.isoformat(),
            "keys_seen": len(key_counts),
            "keys_capped": len(capped_counts),
            "eligible_keys": len(eligible),
            "top_key_counts": top_key_counts,
            "correlations_created": correlations_created,
            "correlations_updated": correlations_updated,
            "correlations_deleted": correlations_deleted,
            "links_created": links_created,
            "links_deleted": links_deleted,
        }
    finally:
        db.close()


def _extract_award_id_lane(ev: Event) -> Tuple[Optional[str], Dict[str, Any]]:
    award_id = _event_value(ev, ("award_id",), ("award_id", "Award ID"))
    guaid = _event_value(ev, ("generated_unique_award_id",), ("generated_unique_award_id",))

    if not _is_blank(award_id):
        value = _clean_key_token(award_id, upper=True)
        return value, {"award_id": value, "award_id_kind": "award_id"}
    if not _is_blank(guaid):
        value = _clean_key_token(guaid, upper=True)
        return value, {"award_id": value, "award_id_kind": "generated_unique_award_id"}

    return None, {}


def _extract_contract_id_lane(ev: Event) -> Tuple[Optional[str], Dict[str, Any]]:
    piid = _event_value(ev, ("piid",), ("piid", "PIID"))
    if not _is_blank(piid):
        value = _clean_key_token(piid, upper=True)
        return f"piid:{value}", {"contract_id": value, "contract_id_kind": "piid", "piid": value}

    fain = _event_value(ev, ("fain",), ("fain", "FAIN"))
    if not _is_blank(fain):
        value = _clean_key_token(fain, upper=True)
        return f"fain:{value}", {"contract_id": value, "contract_id_kind": "fain", "fain": value}

    uri = _event_value(ev, ("uri",), ("uri", "URI"))
    if not _is_blank(uri):
        value = _clean_key_token(uri, upper=True)
        return f"uri:{value}", {"contract_id": value, "contract_id_kind": "uri", "uri": value}

    return None, {}


def _extract_doc_id_lane(ev: Event) -> Tuple[Optional[str], Dict[str, Any]]:
    document_id = _event_value(ev, ("document_id",), ("document_id", "Document ID"))
    if not _is_blank(document_id):
        value = _clean_key_token(document_id, upper=True)
        return f"document:{value}", {"document_id": value, "doc_id_kind": "document_id"}

    notice_id = _event_value(ev, ("notice_id",), ("notice_id", "noticeId"))
    if not _is_blank(notice_id):
        value = _clean_key_token(notice_id, upper=True)
        return f"notice:{value}", {"document_id": value, "doc_id_kind": "notice_id", "notice_id": value}

    doc_id = _event_value(ev, ("doc_id",), ("doc_id",))
    if not _is_blank(doc_id):
        value = _clean_key_token(doc_id, upper=True)
        return f"doc:{value}", {"document_id": value, "doc_id_kind": "doc_id"}

    solicitation_number = _event_value(ev, ("solicitation_number",), ("solicitation_number", "solicitationNumber"))
    if not _is_blank(solicitation_number):
        value = _clean_key_token(solicitation_number, upper=True)
        return f"sol:{value}", {"document_id": value, "doc_id_kind": "solicitation_number", "solicitation_number": value}

    return None, {}


def _extract_agency_lane(ev: Event) -> Tuple[Optional[str], Dict[str, Any]]:
    awarding_code = _event_value(ev, ("awarding_agency_code",), ("awarding_agency_code", "fullParentPathCode"))
    if not _is_blank(awarding_code):
        value = _clean_key_token(awarding_code, upper=True)
        return f"award:{value}", {
            "agency_key": value,
            "agency_type": "awarding_code",
            "awarding_agency_code": value,
            "awarding_agency_name": _event_value(ev, ("awarding_agency_name",), ("awarding_agency_name", "fullParentPathName")),
        }

    funding_code = _event_value(ev, ("funding_agency_code",), ("funding_agency_code",))
    if not _is_blank(funding_code):
        value = _clean_key_token(funding_code, upper=True)
        return f"fund:{value}", {
            "agency_key": value,
            "agency_type": "funding_code",
            "funding_agency_code": value,
            "funding_agency_name": _event_value(ev, ("funding_agency_name",), ("funding_agency_name",)),
        }

    office_code = _event_value(ev, ("contracting_office_code",), ("contracting_office_code", "officeCode", "subTierCode"))
    if not _is_blank(office_code):
        value = _clean_key_token(office_code, upper=True)
        return f"office:{value}", {
            "agency_key": value,
            "agency_type": "contracting_office_code",
            "contracting_office_code": value,
            "contracting_office_name": _event_value(ev, ("contracting_office_name",), ("contracting_office_name", "officeName", "subTier")),
        }

    awarding_name = _event_value(ev, ("awarding_agency_name",), ("awarding_agency_name", "fullParentPathName"))
    if not _is_blank(awarding_name):
        value = _clean_key_token(awarding_name, lower=True)
        return f"award_name:{value}", {"agency_key": awarding_name, "agency_type": "awarding_name"}

    funding_name = _event_value(ev, ("funding_agency_name",), ("funding_agency_name",))
    if not _is_blank(funding_name):
        value = _clean_key_token(funding_name, lower=True)
        return f"fund_name:{value}", {"agency_key": funding_name, "agency_type": "funding_name"}

    office_name = _event_value(ev, ("contracting_office_name",), ("contracting_office_name", "officeName", "subTier"))
    if not _is_blank(office_name):
        value = _clean_key_token(office_name, lower=True)
        return f"office_name:{value}", {"agency_key": office_name, "agency_type": "contracting_office_name"}

    return None, {}


def _extract_psc_lane(ev: Event) -> Tuple[Optional[str], Dict[str, Any]]:
    psc = _event_value(ev, ("psc_code",), ("psc_code", "sam_classification_code", "classificationCode", "PSC Code"))
    if _is_blank(psc):
        return None, {}
    value = _clean_key_token(psc, upper=True)
    return value, {
        "psc_code": value,
        "psc_description": _event_value(ev, ("psc_description",), ("psc_description", "PSC Description")),
    }


def _extract_naics_lane(ev: Event) -> Tuple[Optional[str], Dict[str, Any]]:
    naics = _event_value(ev, ("naics_code",), ("naics_code", "sam_naics_code", "naicsCode", "NAICS"))
    if _is_blank(naics):
        return None, {}
    value = _clean_key_token(naics, upper=True)
    return value, {
        "naics_code": value,
        "naics_description": _event_value(
            ev,
            ("naics_description",),
            ("naics_description", "sam_naics_description", "naicsDescription", "NAICS Description"),
        ),
    }


def _extract_place_region_lane(ev: Event) -> Tuple[Optional[str], Dict[str, Any]]:
    country = _clean_key_token(
        _event_value(
            ev,
            ("place_of_performance_country",),
            ("place_of_performance_country", "sam_place_country_code", "countryCode", "country"),
        ),
        upper=True,
    )
    state = _clean_key_token(
        _event_value(
            ev,
            ("place_of_performance_state",),
            ("place_of_performance_state", "sam_place_state_code", "stateCode", "state"),
        ),
        upper=True,
    )
    zip_code = _clean_key_token(
        _event_value(ev, ("place_of_performance_zip",), ("place_of_performance_zip", "zip", "postalCode")),
        upper=True,
    )

    if state:
        c = country or "USA"
        key = f"{c}:{state}"
        return key, {
            "place_region": key,
            "place_country": c,
            "place_state": state,
        }

    if zip_code:
        c = country or "USA"
        zip_prefix = zip_code[:3]
        key = f"{c}:ZIP{zip_prefix}"
        return key, {
            "place_region": key,
            "place_country": c,
            "place_zip_prefix": zip_prefix,
        }

    return None, {}


def rebuild_award_id_correlations(
    *,
    window_days: int = 30,
    source: Optional[str] = "USAspending",
    min_events: int = 2,
    max_events: int = 200,
    radius_km: float = 0.0,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    return _rebuild_same_field_correlations(
        lane="same_award_id",
        key_segment="award",
        value_label="award_id",
        extractor=_extract_award_id_lane,
        window_days=window_days,
        source=source,
        min_events=min_events,
        max_events=max_events,
        radius_km=radius_km,
        dry_run=dry_run,
        database_url=database_url,
    )


def rebuild_contract_id_correlations(
    *,
    window_days: int = 30,
    source: Optional[str] = "USAspending",
    min_events: int = 2,
    max_events: int = 200,
    radius_km: float = 0.0,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    return _rebuild_same_field_correlations(
        lane="same_contract_id",
        key_segment="contract",
        value_label="contract_id",
        extractor=_extract_contract_id_lane,
        window_days=window_days,
        source=source,
        min_events=min_events,
        max_events=max_events,
        radius_km=radius_km,
        dry_run=dry_run,
        database_url=database_url,
    )


def rebuild_doc_id_correlations(
    *,
    window_days: int = 30,
    source: Optional[str] = "USAspending",
    min_events: int = 2,
    max_events: int = 200,
    radius_km: float = 0.0,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    return _rebuild_same_field_correlations(
        lane="same_doc_id",
        key_segment="doc",
        value_label="document_id",
        extractor=_extract_doc_id_lane,
        window_days=window_days,
        source=source,
        min_events=min_events,
        max_events=max_events,
        radius_km=radius_km,
        dry_run=dry_run,
        database_url=database_url,
    )


def rebuild_agency_correlations(
    *,
    window_days: int = 30,
    source: Optional[str] = None,
    min_events: int = 2,
    max_events: int = 200,
    radius_km: float = 0.0,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    return _rebuild_same_field_correlations(
        lane="same_agency",
        key_segment="agency",
        value_label="agency_key",
        extractor=_extract_agency_lane,
        window_days=window_days,
        source=source,
        min_events=min_events,
        max_events=max_events,
        radius_km=radius_km,
        dry_run=dry_run,
        database_url=database_url,
    )


def rebuild_psc_correlations(
    *,
    window_days: int = 30,
    source: Optional[str] = None,
    min_events: int = 2,
    max_events: int = 200,
    radius_km: float = 0.0,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    return _rebuild_same_field_correlations(
        lane="same_psc",
        key_segment="psc",
        value_label="psc_code",
        extractor=_extract_psc_lane,
        window_days=window_days,
        source=source,
        min_events=min_events,
        max_events=max_events,
        radius_km=radius_km,
        dry_run=dry_run,
        database_url=database_url,
    )


def rebuild_naics_correlations(
    *,
    window_days: int = 30,
    source: Optional[str] = None,
    min_events: int = 2,
    max_events: int = 200,
    radius_km: float = 0.0,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    return _rebuild_same_field_correlations(
        lane="same_naics",
        key_segment="naics",
        value_label="naics_code",
        extractor=_extract_naics_lane,
        window_days=window_days,
        source=source,
        min_events=min_events,
        max_events=max_events,
        radius_km=radius_km,
        dry_run=dry_run,
        database_url=database_url,
    )


def rebuild_place_region_correlations(
    *,
    window_days: int = 30,
    source: Optional[str] = None,
    min_events: int = 2,
    max_events: int = 200,
    radius_km: float = 0.0,
    dry_run: bool = False,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    return _rebuild_same_field_correlations(
        lane="same_place_region",
        key_segment="region",
        value_label="place_region",
        extractor=_extract_place_region_lane,
        window_days=window_days,
        source=source,
        min_events=min_events,
        max_events=max_events,
        radius_km=radius_km,
        dry_run=dry_run,
        database_url=database_url,
    )







