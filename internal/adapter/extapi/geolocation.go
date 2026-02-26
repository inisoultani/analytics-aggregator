package extapi

import (
	"analytics-aggregator/internal/core/port"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type GeoAPIClient struct {
	client  *http.Client
	baseURL string
}

func NewGeoAPIClient(baseURL string) *GeoAPIClient {
	return &GeoAPIClient{
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		baseURL: baseURL,
	}
}

/*
sample response
{
"status": "success",
"country": "United States",
"countryCode": "US",
"region": "VA",
"regionName": "Virginia",
"city": "Ashburn",
"zip": "20149",
"lat": 39.03,
"lon": -77.5,
"timezone": "America/New_York",
"isp": "Google LLC",
"org": "Google Public DNS",
"as": "AS15169 Google LLC",
"query": "8.8.8.8"
}
*/

type GeoAPIResponse struct {
	Country string  `json:"country"`
	City    string  `json:"city"`
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
}

func (g *GeoAPIClient) EnrichIp(ctx context.Context, ip string) ([]byte, error) {
	url := fmt.Sprintf("%s/json/%s", g.baseURL, ip)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request, %w", err)
	}

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("external api request failed, %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, port.ErrRateLimited
	}

	var geoResp GeoAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&geoResp); err != nil {
		return nil, fmt.Errorf("failed to decode response, %w", err)
	}

	enrichBytes, err := json.Marshal(geoResp)
	if err != nil {
		return nil, fmt.Errorf("failed marshal enriched data, %w", err)
	}

	return enrichBytes, nil

}
