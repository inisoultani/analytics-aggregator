-- name: InsertDeadLetter :one
INSERT INTO dead_letter_events (id, raw_data, error_reason)
VALUES ($1, $2, $3)
returning id;