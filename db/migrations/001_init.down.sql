DROP INDEX IF EXISTS idx_events_created_at;
DROP TABLE IF EXISTS events;
DROP INDEX IF EXISTS idx_dlq_failed_at;
DROP TABLE IF EXISTS dead_letter_events;