from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.analysis.scoring import score_from_keywords_clauses, score_from_keywords_clauses_v2
from backend.correlate.scorer import (
    DEFAULT_KW_PAIR_BONUS_MIN_EVENT_COUNT,
    DEFAULT_KW_PAIR_BONUS_MIN_SIGNAL,
    kw_pair_bonus_contribution,
    kw_pair_event_count,
    kw_pair_score_signal,
)
from backend.db.models import (
    AnalysisRun,
    Correlation,
    CorrelationLink,
    Event,
    LeadSnapshot,
    LeadSnapshotItem,
    get_session_factory,
)


_DOD_PACK_PREFIX = "sam_dod_"
_FOIA_MATRIX_BONUS_CAP = 3



def _norm_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, dict):
        return []
    if isinstance(value, list):
        return value
    return []



def _dod_keyword_metrics(keywords: list[Any]) -> tuple[int, int]:
    lane_ids: set[str] = set()
    keyword_hits = 0
    for item in keywords:
        if not isinstance(item, str):
            continue
        pack, _, _rule = item.partition(":")
        if not pack.startswith(_DOD_PACK_PREFIX):
            continue
        lane_ids.add(pack)
        keyword_hits += 1
    return len(lane_ids), keyword_hits



def _foia_matrix_bonus(*, dod_lane_count: int, pair_count: int) -> int:
    if dod_lane_count <= 0:
        return 0
    lane_component = min(int(dod_lane_count), 2)
    pair_component = 1 if int(pair_count) > 0 else 0
    return min(_FOIA_MATRIX_BONUS_CAP, lane_component + pair_component)



def _foia_potential_tier(*, dod_lane_count: int, dod_keyword_hit_count: int, pair_count: int) -> str:
    if dod_lane_count >= 3 and dod_keyword_hit_count >= 4 and pair_count >= 1:
        return "high"
    if dod_lane_count >= 2 and dod_keyword_hit_count >= 2:
        return "medium"
    if dod_lane_count >= 1 and dod_keyword_hit_count >= 2 and pair_count >= 1:
        return "medium"
    return "low"



def _legacy_pair_bonus_contribution(event_count: int) -> float:
    if int(event_count) <= 0:
        return 0.0
    return 1.0 / math.sqrt(float(event_count))



def compute_leads(
    db: Session,
    *,
    scan_limit: int = 5000,
    limit: int = 200,
    min_score: int = 1,
    source: Optional[str] = None,
    exclude_source: Optional[str] = None,
    scoring_version: str = "v2",
    pair_bonus_multiplier: int = 6,
    pair_bonus_cap: int = 12,
    noise_pair_bonus_cap: int = 2,
    noise_penalty: int = 8,
    pair_signal_threshold: float = DEFAULT_KW_PAIR_BONUS_MIN_SIGNAL,
    pair_event_count_threshold: int = DEFAULT_KW_PAIR_BONUS_MIN_EVENT_COUNT,
) -> Tuple[List[Tuple[int, Event, Dict[str, Any]]], int]:
    rows = db.execute(select(Event).order_by(Event.id.desc()).limit(int(scan_limit))).scalars().all()
    scanned = len(rows)

    pair_counts: Dict[int, int] = {}
    pair_counts_total: Dict[int, int] = {}
    pair_strength: Dict[int, float] = {}

    is_v2 = str(scoring_version).lower().startswith("v2")

    if is_v2:
        ids = [int(e.id) for e in rows]
        if ids:
            like_pat = f"kw_pair|{source}|%|pair:%" if source else "kw_pair|%|%|pair:%"
            q = (
                db.query(CorrelationLink.event_id, Correlation.score, Correlation.lanes_hit)
                .join(Correlation, Correlation.id == CorrelationLink.correlation_id)
                .filter(Correlation.correlation_key.like(like_pat))
                .filter(CorrelationLink.event_id.in_(ids))
            )
            for event_id, cscore, lanes_hit in q.all():
                eid = int(event_id)
                event_count = kw_pair_event_count(lanes_hit, fallback_score=cscore)
                if event_count <= 0:
                    continue

                pair_counts_total[eid] = pair_counts_total.get(eid, 0) + 1

                score_signal = kw_pair_score_signal(lanes_hit)
                contribution = kw_pair_bonus_contribution(
                    score_signal=score_signal,
                    event_count=event_count,
                    min_signal=float(pair_signal_threshold),
                    min_event_count=int(pair_event_count_threshold),
                )
                if contribution <= 0 and score_signal is None:
                    contribution = _legacy_pair_bonus_contribution(event_count)

                if contribution <= 0:
                    continue

                pair_counts[eid] = pair_counts.get(eid, 0) + 1
                pair_strength[eid] = pair_strength.get(eid, 0.0) + float(contribution)

    scored: List[Tuple[int, Event, Dict[str, Any]]] = []
    for e in rows:
        if source and e.source != source:
            continue
        if exclude_source and e.source == exclude_source:
            continue

        kw_list = _norm_list(e.keywords)
        has_noise = any(isinstance(k, str) and k.startswith(_NOISE_PACK_PREFIXES) for k in kw_list)
        dod_lane_count, dod_keyword_hit_count = _dod_keyword_metrics(kw_list)
        pair_n = pair_counts.get(int(e.id), 0)
        pair_n_total = pair_counts_total.get(int(e.id), 0)
        strength = pair_strength.get(int(e.id), 0.0)

        if is_v2:
            pair_bonus = int(round(float(pair_bonus_multiplier) * float(strength)))
            if pair_bonus > int(pair_bonus_cap):
                pair_bonus = int(pair_bonus_cap)

            if has_noise:
                pair_bonus = min(int(pair_bonus), int(noise_pair_bonus_cap))

            score, details = score_from_keywords_clauses_v2(
                e.keywords,
                e.clauses,
                has_entity=bool(e.entity_id),
                pair_bonus=int(pair_bonus),
            )
            details["pair_count"] = int(pair_n)
            details["pair_count_total"] = int(pair_n_total)
            details["pair_strength"] = round(float(strength), 4)
            details["pair_signal_total"] = round(float(strength), 4)
            details["pair_signal_threshold"] = float(pair_signal_threshold)
            details["pair_event_count_threshold"] = int(pair_event_count_threshold)
            details["has_noise"] = bool(has_noise)
            if has_noise:
                score = int(score) - int(noise_penalty)
                details["noise_penalty"] = int(noise_penalty)
                details["pair_bonus_cap_due_to_noise"] = int(noise_pair_bonus_cap)
        else:
            score, details = score_from_keywords_clauses(
                e.keywords,
                e.clauses,
                has_entity=bool(e.entity_id),
            )

        foia_matrix_bonus = 0
        if is_v2:
            foia_matrix_bonus = _foia_matrix_bonus(dod_lane_count=dod_lane_count, pair_count=pair_n)
            if has_noise:
                foia_matrix_bonus = min(int(foia_matrix_bonus), 1)
            score = int(score) + int(foia_matrix_bonus)

        details.setdefault("pair_count", int(pair_n))
        details.setdefault("pair_count_total", int(pair_n_total))
        details.setdefault("pair_strength", round(float(strength), 4))
        details.setdefault("pair_signal_total", round(float(strength), 4))
        details.setdefault("pair_signal_threshold", float(pair_signal_threshold))
        details.setdefault("pair_event_count_threshold", int(pair_event_count_threshold))
        details.setdefault("has_noise", bool(has_noise))
        details["dod_lane_count"] = int(dod_lane_count)
        details["dod_keyword_hit_count"] = int(dod_keyword_hit_count)
        details["foia_matrix_bonus"] = int(foia_matrix_bonus)
        details["foia_potential_tier"] = _foia_potential_tier(
            dod_lane_count=int(dod_lane_count),
            dod_keyword_hit_count=int(dod_keyword_hit_count),
            pair_count=int(pair_n),
        )

        if int(score) >= int(min_score):
            scored.append((int(score), e, details))

    scored.sort(key=lambda t: (t[0], t[1].id), reverse=True)
    return scored[: int(limit)], scanned



def create_lead_snapshot(
    *,
    analysis_run_id: Optional[int] = None,
    source: Optional[str] = None,
    exclude_source: Optional[str] = None,
    min_score: int = 1,
    limit: int = 200,
    scan_limit: int = 5000,
    scoring_version: str = "v2",
    notes: Optional[str] = None,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    SessionFactory = get_session_factory(database_url)
    db: Session = SessionFactory()
    try:
        if analysis_run_id is not None:
            ok = (
                db.execute(select(AnalysisRun.id).where(AnalysisRun.id == analysis_run_id))
                .scalar_one_or_none()
            )
            if ok is None:
                raise ValueError(
                    f"analysis_run_id {analysis_run_id} not found in analysis_runs. "
                    f"Run 'ss ontology apply ...' and use the printed analysis_run_id, or omit --analysis-run-id."
                )

        ranked, scanned = compute_leads(
            db,
            scan_limit=scan_limit,
            limit=limit,
            min_score=min_score,
            source=source,
            exclude_source=exclude_source,
            scoring_version=scoring_version,
        )

        snap = LeadSnapshot(
            analysis_run_id=analysis_run_id,
            source=source,
            min_score=int(min_score),
            limit=int(limit),
            scoring_version=str(scoring_version),
            notes=notes,
        )
        db.add(snap)
        db.commit()
        db.refresh(snap)

        inserted = 0
        for idx, (score, e, details) in enumerate(ranked, start=1):
            item = LeadSnapshotItem(
                snapshot_id=snap.id,
                event_id=e.id,
                event_hash=e.hash,
                rank=idx,
                score=int(score),
                score_details=details,
            )
            db.add(item)
            inserted += 1

        db.commit()
        return {
            "status": "ok",
            "snapshot_id": snap.id,
            "analysis_run_id": analysis_run_id,
            "source": source,
            "exclude_source": exclude_source,
            "min_score": int(min_score),
            "limit": int(limit),
            "scan_limit": int(scan_limit),
            "scoring_version": str(scoring_version),
            "scanned": int(scanned),
            "items": int(inserted),
        }
    finally:
        db.close()
