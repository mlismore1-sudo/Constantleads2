CREATE TABLE IF NOT EXISTS screened_companies (
    company_number TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    incorporation_date DATE,
    company_status TEXT,
    sic_codes TEXT,
    company_url TEXT NOT NULL,
    screened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    shortlisted BOOLEAN NOT NULL DEFAULT FALSE,
    published_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_screened_companies_incorporation_date
ON screened_companies (incorporation_date);

CREATE INDEX IF NOT EXISTS idx_screened_companies_screened_at
ON screened_companies (screened_at DESC);

CREATE INDEX IF NOT EXISTS idx_screened_companies_shortlisted
ON screened_companies (shortlisted);

CREATE TABLE IF NOT EXISTS stream_state (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    timepoint BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
