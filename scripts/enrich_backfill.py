import os
from sqlalchemy import select
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
    """
    Simple, Phase-0 mapping for the seeded entities.
    Later you’ll replace this with UEI/CAGE + fuzzy matching + geo.
    """
    term_map = {}
    for e in entities:
        name = (e.name or "").lower()
        parent = (e.parent or "").lower()

        terms = set()

        # Always include fragments of the name/parent
        for token in [name, parent]:
            if token:
                terms.add(token)

        # Special heuristics for seeded labs
        if "los alamos" in name:
            terms.update(["los alamos", "lanl", "triad"])
        if "lawrence livermore" in name or "livermore" in name:
            terms.update(["livermore", "llnl"])
        if "sandia" in name:
            terms.update(["sandia", "ntess"])

        term_map[e.id] = sorted(terms)
    return term_map


def match_entity_id(text: str, entity_term_map: dict[int, list[str]]):
    if not text:
        return None
    t = text.lower()
    for entity_id, terms in entity_term_map.items():
        for term in terms:
            if term and term in t:
                return entity_id
    return None


def main():
    db = os.getenv("DATABASE_URL", "sqlite:///./dev.db")

    updated_keywords = 0
    updated_entity = 0

    with session_scope(db) as s:
        entities = s.execute(select(Entity)).scalars().all()
        entity_term_map = build_entity_term_map(entities)

        events = s.execute(select(Event)).scalars().all()

        for ev in events:
            # Build a text blob from what we have
            recipient = ""
            if isinstance(ev.raw_json, dict):
                recipient = (
                    ev.raw_json.get("Recipient Name")
                    or ev.raw_json.get("recipient_name")
                    or ""
                )

            blob = " ".join(
                x for x in [
                    ev.snippet or "",
                    ev.place_text or "",
                    recipient or "",
                ] if x
            )

            # keywords backfill
            hits = keyword_hits(blob)
            if hits and (not ev.keywords or ev.keywords == {} or ev.keywords == []):
                ev.keywords = hits
                updated_keywords += 1

            # entity_id backfill
            if ev.entity_id is None:
                eid = match_entity_id(blob, entity_term_map)
                if eid is not None:
                    ev.entity_id = eid
                    updated_entity += 1

    print("Backfill complete.")
    print("Events updated (keywords):", updated_keywords)
    print("Events updated (entity_id):", updated_entity)


if __name__ == "__main__":
    main()
