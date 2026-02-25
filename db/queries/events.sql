-- name: BulkInsertEvents :copyfrom
INSERT INTO events (id, raw_data, enriched_data)
VALUES ($1, $2, $3);