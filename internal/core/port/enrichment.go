package port

import "context"

type DataEnricher interface {
	EnrichIp(ctx context.Context, ip string) ([]byte, error)
}
