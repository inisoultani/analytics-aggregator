package service

import (
	"analytics-aggregator/internal/config"
	"analytics-aggregator/internal/core/domain"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type MockEnricher struct{}

func (m *MockEnricher) EnrichIp(ctx context.Context, ip string) ([]byte, error) {
	panic("intended issue during api call - unit test")
}

func TestWorker_ExecuteSafely_RecoverFromPanic(t *testing.T) {
	dlc := make(chan *domain.Event, 1)
	worker := &EnricherWorker{
		id:             1,
		dataEnricher:   &MockEnricher{},
		deadLetterChan: dlc,
	}

	e := &domain.Event{
		ID: uuid.New(),
	}

	worker.enriceWithPanicHandling(context.Background(), e)

	select {
	case panicEvent := <-dlc:
		if panicEvent.ErrorReason == "" {
			t.Errorf("Expected ErrorReason to be filled, got empty string instead")
		}
		t.Logf("Successfully captured error reason : %s", panicEvent.ErrorReason)
	case <-time.After(1 * time.Second):
		t.Fatal("Time out during waiting for panic event in DLC")
	}
}

func TestPipelineService_StoreWithRetry_BackpressureMaxRetries(t *testing.T) {
	cfg := &config.Config{
		EnricherWorkerSize: 1,
		PipelineJobSize:    1,
		InsertBatchSize:    1,
		// this is to ensure that the retry will not spent more 12 milliseconds instead 12 second
		BackoffMulitplier: time.Millisecond,
	}

	e := &domain.Event{
		ID: uuid.New(),
	}

	service := NewPipelineService(context.Background(), nil, &MockEnricher{}, cfg)

	// saturate the pipeline channel
	// this is necessary to ensure that the attemptCtx with 100ms always timeout
	service.pipelineJobChan <- e

	var wg sync.WaitGroup

	wg.Add(1)
	go service.storeWithRetry(context.Background(), e, &wg)

	expectedErrorReason := "pipeline_store_with_retry_maxed"
	select {
	case failedEvent := <-service.deadLetterChan:
		if failedEvent.ErrorReason != expectedErrorReason {
			t.Errorf("Expected reason : '%s', got : %s", expectedErrorReason, failedEvent.ErrorReason)
		}
		t.Logf("Successfully captured error reasons '%s'", expectedErrorReason)
	case <-time.After(15 * time.Second):
		t.Fatal("Time out during waiting for dead letter event")
	}

	wg.Wait()

}
