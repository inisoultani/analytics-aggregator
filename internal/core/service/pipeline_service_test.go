package service

import (
	"analytics-aggregator/internal/config"
	"analytics-aggregator/internal/core/domain"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
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

func TestPipelineService_JobDistributor_RoutesEvent(t *testing.T) {

	s := &PipelineService{
		pipelineJobChan: make(chan *domain.Event, 1),
		workerPoolChan:  make(chan *EnricherWorker, 1),
	}
	event := &domain.Event{
		ID: uuid.New(),
	}
	s.pipelineJobChan <- event

	dummyWorkerId := 123
	dummyWorker := &EnricherWorker{
		id:      dummyWorkerId,
		jobChan: make(chan *domain.Event, 1),
	}
	s.workerPoolChan <- dummyWorker

	ctxCancel, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go s.jobDistributor(ctxCancel, &wg)

	select {
	case e := <-dummyWorker.jobChan:
		if e.ID != event.ID {
			t.Errorf("Expected event ID : %s, found something else", e.ID.String())
		}
		t.Logf("Successfully receive event ID : %s", e.ID)
	case <-time.After(1 * time.Second):
		t.Errorf("Timeout during waiting for event")
	}

	cancel()
	wg.Wait()

}

func TestPipelineService_AssignWorker_AssignEvent(t *testing.T) {
	s := &PipelineService{
		pipelineJobChan: make(chan *domain.Event, 1),
		workerPoolChan:  make(chan *EnricherWorker, 1),
		deadLetterChan:  make(chan *domain.Event, 1),
	}
	event := &domain.Event{
		ID: uuid.New(),
	}

	dummyWorkerId := 123
	dummyWorker := &EnricherWorker{
		id:      dummyWorkerId,
		jobChan: make(chan *domain.Event, 1),
	}
	s.workerPoolChan <- dummyWorker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.assignWorker(ctx, event)

	select {
	case e := <-dummyWorker.jobChan:
		if e.ID != event.ID {
			t.Errorf("Expected to receive event id : %s, but receive something else", event.ID.String())
		}
		t.Logf("Successfully assignWorker with event id : %s", e.ID.String())
	case <-time.After(1 * time.Second):
		t.Errorf("Timeout ")
	}

}

type MockInsertDatabase struct {
	mock.Mock
}

func (mid *MockInsertDatabase) InsertActionDummy(ctx context.Context, events []domain.Event, batchName string) (int64, error) {
	args := mid.Called(ctx, events, batchName)
	return args.Get(0).(int64), args.Error(1)
}

func TestPipelineService_batchInsert_batchSize(t *testing.T) {

	s := &PipelineService{
		insertBatchSize:           2,
		batchChan:                 make(chan *domain.Event, 2),
		batchInsertTickerDuration: time.Duration(3 * time.Second),
	}

	mockInsertAction := new(MockInsertDatabase)
	doneMockChan := make(chan bool)

	mockInsertAction.On("InsertActionDummy", mock.Anything, mock.MatchedBy(func(events []domain.Event) bool {
		return len(events) == 2
	}), "test_batch").
		Return(int64(2), nil).
		Run(func(args mock.Arguments) {
			close(doneMockChan)
		}).
		Once()

	s.batchChan <- &domain.Event{ID: uuid.New()}
	s.batchChan <- &domain.Event{ID: uuid.New()}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go s.batchInsert(ctx, &wg, "test_batch", s.batchChan, mockInsertAction.InsertActionDummy)

	select {
	case <-doneMockChan:
		t.Log("Successfully capture the database insert process")
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout:  mockInsertAction.InsertActionDummy never called by batchInsert")
	}

	cancel()
	wg.Wait()

	mockInsertAction.AssertExpectations(t)
}

func TestPipelineService_batchInsert_ticker(t *testing.T) {

	s := &PipelineService{
		insertBatchSize:           5,
		batchChan:                 make(chan *domain.Event, 2),
		batchInsertTickerDuration: time.Duration(10 * time.Millisecond),
	}

	mockInsertAction := new(MockInsertDatabase)
	doneMockChan := make(chan bool)

	mockInsertAction.On("InsertActionDummy", mock.Anything, mock.MatchedBy(func(events []domain.Event) bool {
		return len(events) == 2
	}), "test_batch").
		Return(int64(2), nil).
		Run(func(args mock.Arguments) {
			close(doneMockChan)
		}).
		Once()

	s.batchChan <- &domain.Event{ID: uuid.New()}
	s.batchChan <- &domain.Event{ID: uuid.New()}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go s.batchInsert(ctx, &wg, "test_batch", s.batchChan, mockInsertAction.InsertActionDummy)

	select {
	case <-doneMockChan:
		t.Log("Successfully capture the database insert process")
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout:  mockInsertAction.InsertActionDummy never called by batchInsert")
	}

	cancel()
	wg.Wait()

	mockInsertAction.AssertExpectations(t)
}
