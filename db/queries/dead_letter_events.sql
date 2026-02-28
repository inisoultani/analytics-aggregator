-- name: BulkInsertDeadLetterEvents :copyfrom
INSERT INTO dead_letter_events (id, raw_data, error_reason)
VALUES ($1, $2, $3);