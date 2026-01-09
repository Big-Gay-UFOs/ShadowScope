import os
import argparse
from sqlalchemy import select

from backend.connectors.usaspending import fetch_awards, normalize_awards
from backend.db.models import session_scope, Event, Entity
from backend.parsers.keyword_packs import KEYWORD_PACKS


def keyword_hits(text: str):
    if not text:
        return []
    t = text.lower()
    hits = []
    for pack, words in KEYWORD_PACKS.items():
        for w in words:
            if w.lower() in t:
                hits.append(f"{pack}:{w}")
    return sorted(set(hits))


def build_entity_term_map(entities):
    term_map = {}
    for e in entities:
        name = (e.name or "").lower()
        parent = (e.parent or "").lower()

        terms = set()
        if name:
            terms.add(name)
        if parent:
            terms.add(parent)

        # seeded lab heuristics
        if "los alamos" in name:
            terms.update(["los alamos", "lanl", "triad"])
        if "lawrence livermore" in name or "livermore" in name:
            terms.update(["livermore", "llnl"])
        if "sandia" in name:
            terms.update(["sandia", "ntess"])

        term_map[e.id] = sorted(terms)
    return term_map


def match_entity_id(text: str, entity_term_map):
    if not text:
        return None
    t = text.lower()
    for entity_id, terms in entity_term_map.items():
        for term in terms:
            if term and term in t:
                return entity_id
    return None


def score(entity_id, keywords):
    return (10 if entity_id else 0) + (3 * len(keywords or []))


def main():
    ap = argparse.ArgumentParser(description="USAspending ingest with local scoring + DB insert filtering")

    # Upstream narrowing (passed into USAspending API)
    ap.add_argument("--recipient", action="append", default=[],
                    help="USAspending recipient_search_text (repeatable)")
    ap.add_argument("--keyword", action="append", default=[],
                    help="USAspending filters.keywords (repeatable)")
    ap.add_argument("--max-pages", type=int, default=None,
                    help="Cap pages for safety (optional)")

    # Local time window + fetch size
    ap.add_argument("--since", default="2025-01-01")
    ap.add_argument("--limit", type=int, default=200)

    # Local “interesting” gate
    ap.add_argument("--min-score", type=int, default=3,
                    help="3=at least 1 keyword hit, 10=entity match, 13=entity + 1 keyword hit")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    db = os.getenv("DATABASE_URL", "sqlite:///./dev.db")

    # IMPORTANT: these now actually get passed to the connector
    recipient_search_text = args.recipient or None
    keywords = args.keyword or None

    raw = list(fetch_awards(
        since=args.since,
        limit=args.limit,
        max_pages=args.max_pages,
        recipient_search_text=recipient_search_text,
        keywords=keywords,
    ))
    events = normalize_awards(raw)

    inserted = 0
    skipped = 0
    already = 0

    with session_scope(db) as s:
        entities = s.execute(select(Entity)).scalars().all()
        entity_term_map = build_entity_term_map(entities)

        # idempotency: skip hashes already present
        hashes = [e["hash"] for e in events]
        existing = set(
            r[0] for r in s.execute(select(Event.hash).where(Event.hash.in_(hashes))).all()
        )

        to_insert = []
        for e in events:
            if e["hash"] in existing:
                already += 1
                continue

            recipient = ""
            rj = e.get("raw_json") or {}
            if isinstance(rj, dict):
                recipient = rj.get("Recipient Name") or rj.get("recipient_name") or ""

            blob = " ".join(x for x in [e.get("snippet") or "", e.get("place_text") or "", recipient] if x)

            kw = keyword_hits(blob)
            eid = match_entity_id(blob, entity_term_map)

            sc = score(eid, kw)
            if sc < args.min_score:
                skipped += 1
                continue

            e["keywords"] = kw
            e["entity_id"] = eid
            to_insert.append(e)

        if args.dry_run:
            print("DRY RUN")
            print("since:", args.since, "limit:", args.limit, "max_pages:", args.max_pages)
            print("recipient:", args.recipient)
            print("keyword:", args.keyword)
            print("min_score:", args.min_score)
            print("fetched:", len(events), "already:", already, "would_insert:", len(to_insert), "skipped:", skipped)
            return

        if to_insert:
            s.bulk_insert_mappings(Event, to_insert)
            inserted = len(to_insert)

    print("fetched:", len(events), "already:", already, "inserted:", inserted, "skipped:", skipped)


if __name__ == "__main__":
    main()
