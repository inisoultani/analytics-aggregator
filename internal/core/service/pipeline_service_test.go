package service

import (
	"analytics-aggregator/internal/config"
	"analytics-aggregator/internal/core/domain"
	"analytics-aggregator/internal/core/port"
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

func TestPipelineService_Start_Stop(t *testing.T) {

	cfg, err := config.Load()
	if err != nil {
		t.Errorf("Error during testing loading config : %v", err)
	}

	s := NewPipelineService(context.Background(), &MockTxManager{}, &MockEnricher{}, cfg)
	s.Open(context.Background())
	s.Close()

}

func TestPipelineService_ProcessAndStore(t *testing.T) {
	cfg := &config.Config{
		EnricherWorkerSize: 1,
		PipelineJobSize:    1,
		InsertBatchSize:    1,
		BackoffMulitplier:  time.Second,
	}

	mainCtx, cancel := context.WithCancel(context.Background())
	service := NewPipelineService(context.Background(), nil, &MockEnricher{}, cfg)
	service.mainCtx = mainCtx

	num, err := service.ProcessAndStore(context.Background(), &domain.Event{ID: uuid.New()})

	cancel()
	service.wgStoreRetry.Wait()

	assert.Equal(t, num, int64(1))
	assert.Equal(t, err, nil)

}

func TestPipelineService_StoreWithRetry_NormalFlow(t *testing.T) {
	cfg := &config.Config{
		EnricherWorkerSize: 1,
		PipelineJobSize:    1,
		InsertBatchSize:    1,
		// this is to ensure that the retry will not spent more 12 milliseconds instead 12 second
		BackoffMulitplier: time.Millisecond,
	}

	dummyEvent := &domain.Event{ID: uuid.New()}
	mainCtx, cancelMainCtx := context.WithCancel(context.Background())
	service := NewPipelineService(mainCtx, nil, &MockEnricher{}, cfg)

	var wg sync.WaitGroup
	wg.Add(1)
	go service.storeWithRetry(context.Background(), dummyEvent, &wg)

	select {
	case e := <-service.pipelineJobChan:
		if e.ID != dummyEvent.ID {
			t.Errorf("Expected to receive event with id : '%s', got : %s", dummyEvent.ID.String(), e.ID.String())
		}
		t.Logf("Successfully receive id '%s'", e.ID.String())
	case <-time.After(1 * time.Second):
		t.Fatal("Time out during waiting for dead letter event")
	}
	cancelMainCtx()
	wg.Wait()
}

func TestPipelineService_StoreWithRetry_attempCtxCancelled(t *testing.T) {
	cfg := &config.Config{
		EnricherWorkerSize: 1,
		// intentionally set channel size to 0,
		// so that the channel will backpressure since no consumer listen to this channel
		PipelineJobSize: 0,
		InsertBatchSize: 1,
		// this is to ensure that the retry will not spent more 12 milliseconds instead 12 second
		BackoffMulitplier: time.Second,
	}

	dummyEvent := &domain.Event{ID: uuid.New()}
	mainCtx, cancelMainCtx := context.WithCancel(context.Background())
	service := NewPipelineService(mainCtx, nil, &MockEnricher{}, cfg)

	// trigger context cancelled so the event will sent to DLE instead

	var wg sync.WaitGroup
	wg.Add(1)
	go service.storeWithRetry(mainCtx, dummyEvent, &wg)
	cancelMainCtx()
	select {
	case e := <-service.deadLetterChan:
		t.Logf("main context status : %s", context.Cause(service.mainCtx))
		t.Logf("event error reason : %s", e.ErrorReason)
		if e.ErrorReason != "pipeline_store_attemptCtx_closed" {
			t.Errorf("Expected to receive event in DLC with id : '%s', got : %s", dummyEvent.ID.String(), e.ID.String())
		}
		t.Logf("Successfully receive id in DLC '%s'", e.ID.String())
	case <-time.After(1 * time.Second):
		t.Fatal("Time out during waiting for dead letter event")
	}

	wg.Wait()

}

func TestPipelineService_StoreWithRetry_ctxCancelled(t *testing.T) {
	cfg := &config.Config{
		EnricherWorkerSize: 1,
		// intentionally set channel size to 0,
		// so that the channel will backpressure since no consumer listen to this channel
		PipelineJobSize: 0,
		InsertBatchSize: 1,
		// this is to ensure that the retry will take longer and ctx.Done triggered first
		BackoffMulitplier: time.Second,
	}

	dummyEvent := &domain.Event{ID: uuid.New()}
	mainCtx, cancelMainCtx := context.WithCancel(context.Background())
	service := NewPipelineService(mainCtx, nil, &MockEnricher{}, cfg)

	// set service storeTimeDuration to 50ms so that we can trigger ctxCanceled after that
	service.storeTimeDuration = time.Duration(50 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(1)
	go service.storeWithRetry(mainCtx, dummyEvent, &wg)

	// trigger cancel only after first attempt context deadline exceeded
	time.AfterFunc(70*time.Millisecond, func() {
		cancelMainCtx()
	})

	select {
	case e := <-service.deadLetterChan:
		t.Logf("main context status : %s", context.Cause(service.mainCtx))
		t.Logf("event error reason : %s", e.ErrorReason)
		if e.ErrorReason != "pipeline_store_with_retry_closed" {
			t.Errorf("Expected to receive event in DLC with id : '%s', got : %s", dummyEvent.ID.String(), e.ID.String())
		}
		t.Logf("Successfully receive id in DLC '%s'", e.ID.String())
	case <-time.After(1 * time.Second):
		t.Fatal("Time out during waiting for dead letter event")
	}

	wg.Wait()

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

func TestPipelineService_AssignWorker_NormalFlow(t *testing.T) {
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

func TestPipelineService_AssignWorker_CtxCancelled_WaitingWorker(t *testing.T) {
	s := &PipelineService{
		pipelineJobChan: make(chan *domain.Event, 1),
		// intentionally set workerPoolChan as unbuffered channel to trigger blocking
		workerPoolChan: make(chan *EnricherWorker),
		deadLetterChan: make(chan *domain.Event, 1),
	}
	event := &domain.Event{ID: uuid.New()}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.assignWorker(ctx, event)

	select {
	case e := <-s.deadLetterChan:
		if e.ErrorReason != "get_worker_failed_context_closed" {
			t.Errorf("Expected to receive event id : %s, but receive something else", event.ID.String())
			return
		}
		t.Logf("Successfully receive event from DLC with id : %s and error reason : %s", e.ID.String(), e.ErrorReason)
	case <-time.After(1 * time.Second):
		t.Errorf("Timeout ")
	}
}

func TestPipelineService_AssignWorker_CtxCancelled_AssignJob(t *testing.T) {
	s := &PipelineService{
		pipelineJobChan: make(chan *domain.Event, 1),
		workerPoolChan:  make(chan *EnricherWorker, 1),
		deadLetterChan:  make(chan *domain.Event, 1),
	}
	event := &domain.Event{ID: uuid.New()}

	dummyWorkerId := 123
	dummyWorker := &EnricherWorker{
		id:      dummyWorkerId,
		jobChan: make(chan *domain.Event),
	}
	s.workerPoolChan <- dummyWorker

	ctx, cancel := context.WithCancel(context.Background())
	// intentionally trigger cancel after 70ms to ensure assignWorker reach worker assign job
	time.AfterFunc(70*time.Millisecond, func() {
		cancel()
	})
	s.assignWorker(ctx, event)

	select {
	case e := <-s.deadLetterChan:
		if e.ErrorReason != "assign_worker_job_failed_context_closed" {
			t.Errorf("Expected to receive event error reason : assign_worker_job_failed_context_closed, but receive something else")
			return
		}
		t.Logf("Successfully receive event from DLC with id : %s and error reason : %s", e.ID.String(), e.ErrorReason)
	case <-time.After(1 * time.Second):
		t.Errorf("Timeout ")
	}

}

type MockInsertAction struct {
	mock.Mock
}

func (mid *MockInsertAction) InsertFn(ctx context.Context, events []domain.Event, batchName string) (int64, error) {
	args := mid.Called(ctx, events, batchName)
	return args.Get(0).(int64), args.Error(1)
}

func TestPipelineService_batchInsert_batchSize(t *testing.T) {

	s := &PipelineService{
		insertBatchSize:           2,
		batchChan:                 make(chan *domain.Event, 2),
		batchInsertTickerDuration: time.Duration(3 * time.Second),
	}

	mockInsertAction := new(MockInsertAction)
	doneMockChan := make(chan bool)

	mockInsertAction.On("InsertFn", mock.Anything, mock.MatchedBy(func(events []domain.Event) bool {
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
	go s.batchInsert(ctx, &wg, "test_batch", s.batchChan, mockInsertAction.InsertFn)

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

	mockInsertAction := new(MockInsertAction)
	doneMockChan := make(chan bool)

	mockInsertAction.On("InsertFn", mock.Anything, mock.MatchedBy(func(events []domain.Event) bool {
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
	go s.batchInsert(ctx, &wg, "test_batch", s.batchChan, mockInsertAction.InsertFn)

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

type MockPipelineRepository struct {
	mer *MockEventRepository
}

func (m *MockPipelineRepository) Event() port.EventRepository {
	return m.mer
}

func (m *MockPipelineRepository) DeadLetterEvent() port.DeadLetterEventRepository {
	return m.mer
}

type MockEventRepository struct {
	mock.Mock
}

func (m *MockEventRepository) CreateEvents(ctx context.Context, e []domain.Event) (int64, error) {
	args := m.Called(ctx, e)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockEventRepository) CreateDeadLetters(ctx context.Context, e []domain.Event) (int64, error) {
	args := m.Called(ctx, e)
	return args.Get(0).(int64), args.Error(1)
}

type MockTxManager struct {
	mpr *MockPipelineRepository
}

func (m *MockTxManager) WithTx(ctx context.Context, fn func(port.PipelineRepository) error) error {
	return fn(m.mpr)
}

type TestPipelineSuite struct {
	suite.Suite
	ps           *PipelineService
	mer          *MockEventRepository
	doneMockChan chan bool
}

func (s *TestPipelineSuite) SetupSuite() {
	s.mer = new(MockEventRepository)

	s.ps = &PipelineService{
		txManager: &MockTxManager{
			mpr: &MockPipelineRepository{
				mer: s.mer,
			},
		},
		insertBatchSize:           2,
		batchInsertTickerDuration: time.Duration(10 * time.Second),
		batchChan:                 make(chan *domain.Event, 2),
		deadLetterChan:            make(chan *domain.Event, 2),
	}
}

func (s *TestPipelineSuite) SetupTest() {
	s.doneMockChan = make(chan bool)
}

func (s *TestPipelineSuite) TestPipelineService_insertEvents() {

	s.mer.On("CreateEvents", mock.Anything, mock.MatchedBy(func(events []domain.Event) bool {
		return len(events) == 2
	})).
		Return(int64(2), nil).
		Run(func(args mock.Arguments) {
			close(s.doneMockChan)
		}).
		Once()

	s.ps.batchChan <- &domain.Event{ID: uuid.New()}
	s.ps.batchChan <- &domain.Event{ID: uuid.New()}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go s.ps.batchInsert(ctx, &wg, "test_batch", s.ps.batchChan, s.ps.insertEvents)

	select {
	case <-s.doneMockChan:
		slog.Info("Successfully execute InsertEvents")
	case <-time.After(1 * time.Second):
		slog.Error("Timeout:  mockInsertAction.InsertActionDummy never called by batchInsert")
	}

	close(s.ps.batchChan)
	cancel()
	wg.Wait()

	s.mer.AssertExpectations(s.T())
}

func (s *TestPipelineSuite) TestPipelineService_insertDeadLetterEvents() {

	s.mer.On("CreateDeadLetters", mock.Anything, mock.MatchedBy(func(events []domain.Event) bool {
		return len(events) == 2
	})).
		Return(int64(2), nil).
		Run(func(args mock.Arguments) {
			close(s.doneMockChan)
		}).
		Once()

	s.ps.deadLetterChan <- &domain.Event{ID: uuid.New()}
	s.ps.deadLetterChan <- &domain.Event{ID: uuid.New()}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go s.ps.batchInsert(ctx, &wg, "test_batch", s.ps.deadLetterChan, s.ps.insertDeadLetterEvents)

	select {
	case <-s.doneMockChan:
		slog.Info("Successfully execute InsertEvents")
	case <-time.After(1 * time.Second):
		slog.Error("Timeout:  mockInsertAction.InsertActionDummy never called by batchInsert")
	}

	close(s.ps.deadLetterChan)
	cancel()
	wg.Wait()

	s.mer.AssertExpectations(s.T())
}

func TestExampleSuite(t *testing.T) {
	suite.Run(t, new(TestPipelineSuite))
}

type MockEnricher struct {
	mock.Mock
}

func (m *MockEnricher) EnrichIp(ctx context.Context, ip string) ([]byte, error) {
	// panic("intended issue during api call - unit test")
	args := m.Called(ctx, ip)
	return args.Get(0).([]byte), args.Error(1)
}

func TestWorker_DataEnricherProcess_NormalFlow(t *testing.T) {
	me := new(MockEnricher)
	me.On("EnrichIp", mock.Anything, "1.2.3.4").
		Return([]byte(`{"test":"ok"}`), nil).
		Once()

	dlc := make(chan *domain.Event, 1)
	batchChan := make(chan *domain.Event, 1)
	worker := &EnricherWorker{
		id:                           1,
		dataEnricher:                 me,
		jobChan:                      make(chan *domain.Event, 1),
		deadLetterChan:               dlc,
		batchChan:                    batchChan,
		workerPool:                   make(chan *EnricherWorker, 1),
		enricherWorkerTickerDuration: time.Duration(1 * time.Second),
	}

	eventId := uuid.New()
	worker.jobChan <- &domain.Event{ID: eventId, ClientIP: "1.2.3.4"}

	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go worker.DataEnricherProcess(ctx, &wg)

	select {
	case event := <-batchChan:
		assert.Equal(t, eventId, event.ID)
	case <-time.After(1 * time.Second):
		t.Fatal("Time out during waiting for panic event in batch channel")
	}

	cancel()
	wg.Wait()
	close(worker.jobChan)
	close(worker.workerPool)

	me.AssertExpectations(t)

}

func TestWorker_DataEnricherProcess_Sleep_And_CtxCancelled(t *testing.T) {
	me := new(MockEnricher)
	// me.On("EnrichIp", mock.Anything, "1.2.3.4").
	// 	Return([]byte(`{"test":"ok"}`), nil).
	// 	Once()

	dlc := make(chan *domain.Event, 1)
	batchChan := make(chan *domain.Event, 1)
	worker := &EnricherWorker{
		id:                           1,
		dataEnricher:                 me,
		jobChan:                      make(chan *domain.Event),
		deadLetterChan:               dlc,
		batchChan:                    batchChan,
		workerPool:                   make(chan *EnricherWorker, 1),
		enricherWorkerTickerDuration: time.Duration(20 * time.Millisecond),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go worker.DataEnricherProcess(ctx, &wg)

	time.AfterFunc(50*time.Millisecond, func() {
		cancel()
	})

	wg.Wait()
	close(worker.jobChan)
	close(worker.workerPool)

	assert.Error(t, context.Cause(ctx), context.Canceled)
	me.AssertExpectations(t)

}

func TestWorker_ExecuteSafely_RecoverFromPanic(t *testing.T) {

	me := new(MockEnricher)
	me.On("EnrichIp", mock.Anything, "1.2.3.4").
		Panic("intended issue during api call - unit test").
		Once()

	dlc := make(chan *domain.Event, 1)
	worker := &EnricherWorker{
		id:             1,
		dataEnricher:   me,
		deadLetterChan: dlc,
	}

	e := &domain.Event{ID: uuid.New(), ClientIP: "1.2.3.4"}
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
