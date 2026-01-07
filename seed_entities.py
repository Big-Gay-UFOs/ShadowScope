"""Seed script for loading initial FFRDC/UARC entities."""
from __future__ import annotations

import argparse
import json
import os
from typing import Iterable

from sqlalchemy import select

from backend.db.models import Entity, ensure_schema, session_scope

DEFAULT_ENTITIES: Iterable[dict] = [
    {
        "name": "Los Alamos National Laboratory",
        "uei": None,
        "cage": "1B245",
        "parent": "Triad National Security, LLC",
        "type": "FFRDC",
        "sponsor": "DOE/NNSA",
        "sites_json": {"states": ["NM"], "rois": ["NM"]},
    },
    {
        "name": "Lawrence Livermore National Laboratory",
        "uei": None,
        "cage": "079R3",
        "parent": "Lawrence Livermore National Security, LLC",
        "type": "FFRDC",
        "sponsor": "DOE/NNSA",
        "sites_json": {"states": ["CA"], "rois": ["CA Antelope Valley"]},
    },
    {
        "name": "Sandia National Laboratories",
        "uei": None,
        "cage": "0C2L7",
        "parent": "National Technology & Engineering Solutions of Sandia, LLC",
        "type": "FFRDC",
        "sponsor": "DOE/NNSA",
        "sites_json": {"states": ["NM", "CA"], "rois": ["NM", "CA Antelope Valley"]},
    },
]


def seed(database_url: str | None = None) -> None:
    ensure_schema(database_url)
    with session_scope(database_url) as session:
        for record in DEFAULT_ENTITIES:
            existing = session.execute(select(Entity).where(Entity.name == record["name"]))
            if existing.scalars().first():
                continue
            session.add(Entity(**record))


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed baseline entities into the database")
    parser.add_argument("--database-url", dest="database_url", default=os.getenv("DATABASE_URL"))
    parser.add_argument(
        "--dump-json",
        action="store_true",
        help="Print the default entities as JSON (no database writes)",
    )
    args = parser.parse_args()

    if args.dump_json:
        print(json.dumps(list(DEFAULT_ENTITIES), indent=2))
        return

    seed(args.database_url)
    print("Seed complete")


if __name__ == "__main__":
    main()
