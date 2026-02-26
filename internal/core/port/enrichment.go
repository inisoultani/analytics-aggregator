package port

import "context"

type EnrichmentService interface {
	EnrichIp(ctx context.Context, ip string) ([]byte, error)
}
