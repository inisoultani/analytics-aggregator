CREATE TABLE events (
  id UUID PRIMARY KEY,
  raw_data JSONB NOT NULL,
  enriched_data JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_events_created_at ON events(created_at);
CREATE TABLE dead_letter_events (
  id UUID PRIMARY KEY,
  raw_data JSONB NOT NULL,
  error_reason TEXT NOT NULL,
  -- Why did it fail? (e.g., "max retries exceeded: 503 Service Unavailable")
  failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_dlq_failed_at ON dead_letter_events(failed_at);