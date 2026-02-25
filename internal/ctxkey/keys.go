package ctxkey

// key is an unexported custom type.
// Because it is unexported, absolutely no other package can create a key of this
// exact type, guaranteeing zero collisions in the context.Context map.
type key string

// globally shared keys here
const (
	RequestID      key = "request_id"
	UserID         key = "user_id"
	IdempotencyKey key = "idempotency_key"
)
