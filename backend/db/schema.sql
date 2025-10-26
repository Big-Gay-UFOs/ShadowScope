CREATE TABLE IF NOT EXISTS entities (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    uei TEXT,
    cage TEXT,
    parent TEXT,
    type TEXT,
    sponsor TEXT,
    sites_json JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    entity_id INTEGER REFERENCES entities(id),
    category TEXT NOT NULL,
    occurred_at TIMESTAMPTZ,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    source TEXT NOT NULL,
    source_url TEXT,
    doc_id TEXT,
    keywords TEXT[],
    clauses TEXT[],
    place_text TEXT,
    snippet TEXT,
    raw_json JSONB,
    hash TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS correlations (
    id SERIAL PRIMARY KEY,
    score TEXT NOT NULL,
    window_days INTEGER NOT NULL,
    radius_km DOUBLE PRECISION NOT NULL,
    lanes_hit TEXT[] NOT NULL,
    summary TEXT,
    rationale TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS correlation_links (
    id SERIAL PRIMARY KEY,
    correlation_id INTEGER REFERENCES correlations(id) ON DELETE CASCADE,
    event_id INTEGER REFERENCES events(id) ON DELETE CASCADE
);
