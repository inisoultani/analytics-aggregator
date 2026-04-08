package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"analytics-aggregator/internal/config"
	"analytics-aggregator/internal/core/domain"
	"analytics-aggregator/internal/core/port"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// =====================================================================
// 1. LIFECYCLE TESTS (Start, Stop, ProcessAndStore)
// =====================================================================

func TestPipelineService_Lifecycle(t *testing.T) {
	t.Run("Start and Stop cleanly", func(t *testing.T) {
		cfg, err := config.Load()
		require.NoError(t, err, "Config should load without error")

		s := NewPipelineService(context.Background(), &MockTxManager{}, &MockEnricher{}, cfg)
		s.Open(context.Background())
		s.Close()
		// Test passes if it doesn't hang/panic
	})

	t.Run("ProcessAndStore happy path", func(t *testing.T) {
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

		assert.Equal(t, int64(1), num)
		assert.NoError(t, err)
	})
}

// =====================================================================
// 2. STORE WITH RETRY (Routing & Backpressure Logic)
// =====================================================================

func TestPipelineService_StoreWithRetry(t *testing.T) {
	tests := []struct {
		name              string
		jobChanSize       int
		cancelDelay       time.Duration
		backoffMulitplier time.Duration
		saturateChan      bool
		expectedChan      string // "pipeline" or "dlc"
		expectedErrReason string
	}{
		{
			name:              "Normal Flow - Event routed to pipelineJobChan",
			jobChanSize:       1,
			cancelDelay:       0,
			backoffMulitplier: time.Millisecond,
			saturateChan:      false,
			expectedChan:      "pipeline",
		},
		{
			name:              "Context Cancelled Immediately - Sent to DLC",
			jobChanSize:       0,
			cancelDelay:       0,
			backoffMulitplier: time.Millisecond,
			saturateChan:      false,
			expectedChan:      "dlc",
			expectedErrReason: "pipeline_store_attemptCtx_closed",
		},
		{
			name:              "Context Cancelled During Backoff - Sent to DLC",
			jobChanSize:       0,
			cancelDelay:       70 * time.Millisecond,
			saturateChan:      false,
			backoffMulitplier: time.Second,
			expectedChan:      "dlc",
			expectedErrReason: "pipeline_store_with_retry_closed",
		},
		{
			name:              "Max Retries Reached due to Backpressure - Sent to DLC",
			jobChanSize:       1,
			cancelDelay:       0,
			backoffMulitplier: time.Millisecond,
			saturateChan:      true,
			expectedChan:      "dlc",
			expectedErrReason: "pipeline_store_with_retry_maxed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{
				EnricherWorkerSize: 1,
				PipelineJobSize:    tt.jobChanSize,
				InsertBatchSize:    1,
				BackoffMulitplier:  tt.backoffMulitplier,
			}

			mainCtx, cancelMainCtx := context.WithCancel(context.Background())
			defer cancelMainCtx()

			service := NewPipelineService(mainCtx, nil, &MockEnricher{}, cfg)
			service.storeTimeDuration = 50 * time.Millisecond

			dummyEvent := &domain.Event{ID: uuid.New()}

			if tt.saturateChan {
				service.pipelineJobChan <- &domain.Event{ID: uuid.New()}
			}

			var wg sync.WaitGroup
			wg.Add(1)

			if tt.cancelDelay == 0 && tt.expectedErrReason == "pipeline_store_attemptCtx_closed" {
				cancelMainCtx()
			} else if tt.cancelDelay > 0 {
				time.AfterFunc(tt.cancelDelay, cancelMainCtx)
			}

			go service.storeWithRetry(mainCtx, dummyEvent, &wg)

			switch tt.expectedChan {
			case "pipeline":
				select {
				case e := <-service.pipelineJobChan:
					assert.Equal(t, dummyEvent.ID, e.ID)
				case <-time.After(1 * time.Second):
					t.Fatal("Timeout waiting for event in pipelineJobChan")
				}
				cancelMainCtx()
			case "dlc":
				select {
				case e := <-service.deadLetterChan:
					assert.Equal(t, tt.expectedErrReason, e.ErrorReason)
				case <-time.After(2 * time.Second):
					t.Fatal("Timeout waiting for dead letter event")
				}
			}

			wg.Wait()
		})
	}
}

// =====================================================================
// 3. WORKER ASSIGNMENT & DISTRIBUTION
// =====================================================================

func TestPipelineService_JobDistributor(t *testing.T) {
	s := &PipelineService{
		pipelineJobChan: make(chan *domain.Event, 1),
		workerPoolChan:  make(chan *EnricherWorker, 1),
	}
	event := &domain.Event{ID: uuid.New()}
	s.pipelineJobChan <- event

	dummyWorker := &EnricherWorker{
		id:      123,
		jobChan: make(chan *domain.Event, 1),
	}
	s.workerPoolChan <- dummyWorker

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go s.jobDistributor(ctx, &wg)

	select {
	case e := <-dummyWorker.jobChan:
		assert.Equal(t, event.ID, e.ID)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for JobDistributor to route event")
	}

	cancel()
	wg.Wait()
}

func TestPipelineService_AssignWorker(t *testing.T) {
	tests := []struct {
		name              string
		workerPoolUnbuf   bool
		cancelDelay       time.Duration
		expectedErrReason string
	}{
		{
			name:            "Normal Flow - Job Assigned",
			workerPoolUnbuf: false,
			cancelDelay:     0,
		},
		{
			name:              "Context Cancelled - Failed to get waiting worker",
			workerPoolUnbuf:   true, // Blocks getting a worker
			cancelDelay:       0,    // Cancel immediately
			expectedErrReason: "get_worker_failed_context_closed",
		},
		{
			name:              "Context Cancelled - Failed during job assignment",
			workerPoolUnbuf:   false,
			cancelDelay:       70 * time.Millisecond,
			expectedErrReason: "assign_worker_job_failed_context_closed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &PipelineService{
				pipelineJobChan: make(chan *domain.Event, 1),
				deadLetterChan:  make(chan *domain.Event, 1),
			}

			if tt.workerPoolUnbuf {
				s.workerPoolChan = make(chan *EnricherWorker)
			} else {
				s.workerPoolChan = make(chan *EnricherWorker, 1)
			}

			dummyWorker := &EnricherWorker{
				id:      123,
				jobChan: make(chan *domain.Event), // Unbuffered to allow blocking if needed
			}

			if !tt.workerPoolUnbuf {
				s.workerPoolChan <- dummyWorker
			}

			event := &domain.Event{ID: uuid.New()}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if tt.cancelDelay == 0 && tt.expectedErrReason != "" {
				cancel()
			} else if tt.cancelDelay > 0 {
				time.AfterFunc(tt.cancelDelay, cancel)
			}

			go s.assignWorker(ctx, event)

			if tt.expectedErrReason == "" {
				select {
				case e := <-dummyWorker.jobChan:
					assert.Equal(t, event.ID, e.ID)
				case <-time.After(1 * time.Second):
					t.Fatal("Timeout waiting for job assignment")
				}
			} else {
				select {
				case e := <-s.deadLetterChan:
					assert.Equal(t, tt.expectedErrReason, e.ErrorReason)
				case <-time.After(1 * time.Second):
					t.Fatal("Timeout waiting for event in DLC")
				}
			}
		})
	}
}

// =====================================================================
// 4. BATCH INSERT PIPELINE (Ticker & Size triggers)
// =====================================================================

func TestPipelineService_BatchInsert_Processor(t *testing.T) {
	tests := []struct {
		name           string
		batchSizeLimit int
		tickerDuration time.Duration
	}{
		{
			name:           "Triggered by Batch Size Reached",
			batchSizeLimit: 2,
			tickerDuration: 3 * time.Second, // Ticker won't hit
		},
		{
			name:           "Triggered by Ticker (Batch Size not reached)",
			batchSizeLimit: 5,
			tickerDuration: 10 * time.Millisecond, // Ticker hits first
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &PipelineService{
				insertBatchSize:           tt.batchSizeLimit,
				batchChan:                 make(chan *domain.Event, 2),
				batchInsertTickerDuration: tt.tickerDuration,
			}

			mockInsertAction := new(MockInsertAction)
			doneMockChan := make(chan bool)

			mockInsertAction.On("InsertFn", mock.Anything, mock.MatchedBy(func(events []domain.Event) bool {
				return len(events) == 2
			}), "test_batch").
				Return(int64(2), nil).
				Run(func(args mock.Arguments) { close(doneMockChan) }).
				Once()

			s.batchChan <- &domain.Event{ID: uuid.New()}
			s.batchChan <- &domain.Event{ID: uuid.New()}

			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			wg.Add(1)

			go s.batchInsert(ctx, &wg, "test_batch", s.batchChan, mockInsertAction.InsertFn)

			select {
			case <-doneMockChan:
				// Success
			case <-time.After(1 * time.Second):
				t.Fatal("Timeout: mockInsertAction.InsertFn never called")
			}

			cancel()
			wg.Wait()
			mockInsertAction.AssertExpectations(t)
		})
	}
}

// =====================================================================
// 5. DATABASE REPOSITORY INTEGRATION (Replaces TestPipelineSuite)
// =====================================================================

func TestPipelineService_Database_Insertion(t *testing.T) {
	tests := []struct {
		name           string
		targetMethod   string // "CreateEvents" or "CreateDeadLetters"
		insertFunction func(s *PipelineService) func(context.Context, []domain.Event, string) (int64, error)
		getChannel     func(s *PipelineService) chan *domain.Event
	}{
		{
			name:         "Insert standard events into repository",
			targetMethod: "CreateEvents",
			insertFunction: func(s *PipelineService) func(context.Context, []domain.Event, string) (int64, error) {
				return s.insertEvents
			},
			getChannel: func(s *PipelineService) chan *domain.Event { return s.batchChan },
		},
		{
			name:         "Insert dead letter events into repository",
			targetMethod: "CreateDeadLetters",
			insertFunction: func(s *PipelineService) func(context.Context, []domain.Event, string) (int64, error) {
				return s.insertDeadLetterEvents
			},
			getChannel: func(s *PipelineService) chan *domain.Event { return s.deadLetterChan },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRepo := new(MockEventRepository)
			ps := &PipelineService{
				txManager:                 &MockTxManager{mpr: &MockPipelineRepository{mer: mockRepo}},
				insertBatchSize:           2,
				batchInsertTickerDuration: 10 * time.Second,
				batchChan:                 make(chan *domain.Event, 2),
				deadLetterChan:            make(chan *domain.Event, 2),
			}

			doneMockChan := make(chan bool)
			mockRepo.On(tt.targetMethod, mock.Anything, mock.MatchedBy(func(events []domain.Event) bool {
				return len(events) == 2
			})).Return(int64(2), nil).
				Run(func(args mock.Arguments) { close(doneMockChan) }).
				Once()

			ch := tt.getChannel(ps)
			ch <- &domain.Event{ID: uuid.New()}
			ch <- &domain.Event{ID: uuid.New()}

			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			wg.Add(1)

			go ps.batchInsert(ctx, &wg, "test_batch", ch, tt.insertFunction(ps))

			select {
			case <-doneMockChan:
				// Success
			case <-time.After(1 * time.Second):
				t.Fatalf("Timeout: %s was never called", tt.targetMethod)
			}

			close(ch)
			cancel()
			wg.Wait()
			mockRepo.AssertExpectations(t)
		})
	}
}

// =====================================================================
// 6. ENRICHER WORKER LOGIC
// =====================================================================

func TestEnricherWorker_DataEnricherProcess(t *testing.T) {
	errIntended := errors.New("intended error - unit test")

	tests := []struct {
		name              string
		inputIP           string
		setupMock         func(m *MockEnricher)
		tickerDuration    time.Duration
		cancelAfter       time.Duration // 0 means don't auto-cancel
		triggerPanic      bool
		expectedErrReason string
		expectInBatch     bool
	}{
		{
			name:    "Normal Flow - Successfully enriches and batches",
			inputIP: "1.2.3.4",
			setupMock: func(m *MockEnricher) {
				m.On("EnrichIp", mock.Anything, "1.2.3.4").Return([]byte(`{"test":"ok"}`), nil).Once()
			},
			tickerDuration: 1 * time.Second,
			expectInBatch:  true,
		},
		{
			name:           "Context Cancelled While Sleeping",
			inputIP:        "", // No event sent
			setupMock:      func(m *MockEnricher) {},
			tickerDuration: 20 * time.Millisecond,
			cancelAfter:    50 * time.Millisecond,
			expectInBatch:  false,
		},
		{
			name:    "Retry Hits Max Attempts - Sent to Dead Letter",
			inputIP: "10.0.0.1",
			setupMock: func(m *MockEnricher) {
				m.On("EnrichIp", mock.Anything, "10.0.0.1").Return([]byte{}, errIntended).Times(4)
			},
			tickerDuration:    20 * time.Millisecond,
			expectedErrReason: "EnrichWithRetry_reach_max_attempt",
		},
		{
			name:    "Panic Recovery - Sent to Dead Letter",
			inputIP: "192.168.1.1",
			setupMock: func(m *MockEnricher) {
				m.On("EnrichIp", mock.Anything, "192.168.1.1").Panic("intended issue during api call").Once()
			},
			tickerDuration: 1 * time.Second,
			triggerPanic:   true,
			// Since your original test expected empty string to be caught, this matches it:
			expectedErrReason: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			me := new(MockEnricher)
			tt.setupMock(me)

			dlc := make(chan *domain.Event, 1)
			batchChan := make(chan *domain.Event, 1)
			jobChan := make(chan *domain.Event, 1)

			worker := &EnricherWorker{
				id:                           1,
				dataEnricher:                 me,
				jobChan:                      jobChan,
				deadLetterChan:               dlc,
				batchChan:                    batchChan,
				workerPool:                   make(chan *EnricherWorker, 1),
				enricherWorkerTickerDuration: tt.tickerDuration,
				backoffMultiplier:            time.Millisecond,
			}

			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup

			if tt.triggerPanic {
				e := &domain.Event{ID: uuid.New(), ClientIP: tt.inputIP}
				worker.enriceWithPanicHandling(ctx, e)
				cancel()
			} else {
				wg.Add(1)
				go worker.DataEnricherProcess(ctx, &wg)

				if tt.inputIP != "" {
					jobChan <- &domain.Event{ID: uuid.New(), ClientIP: tt.inputIP}
				}

				if tt.cancelAfter > 0 {
					time.AfterFunc(tt.cancelAfter, cancel)
				} else {
					time.Sleep(50 * time.Millisecond)
					cancel()
				}
				wg.Wait()
			}

			close(jobChan)
			close(worker.workerPool)

			if tt.expectInBatch {
				select {
				case <-batchChan:
					// Success
				default:
					t.Fatal("Expected event in batch channel, got none")
				}
			}

			if tt.expectedErrReason != "" || tt.triggerPanic {
				select {
				case e := <-dlc:
					if tt.triggerPanic && e.ErrorReason == "" {
						t.Errorf("Expected ErrorReason to be filled due to panic, got empty")
					} else if tt.expectedErrReason != "" {
						assert.Contains(t, e.ErrorReason, tt.expectedErrReason)
					}
				default:
					t.Fatal("Expected event in dead letter channel, got none")
				}
			}

			me.AssertExpectations(t)
		})
	}
}

// =====================================================================
// SHARED MOCKS
// =====================================================================

type MockEnricher struct {
	mock.Mock
}

func (m *MockEnricher) EnrichIp(ctx context.Context, ip string) ([]byte, error) {
	args := m.Called(ctx, ip)
	return args.Get(0).([]byte), args.Error(1)
}

type MockInsertAction struct {
	mock.Mock
}

func (mid *MockInsertAction) InsertFn(ctx context.Context, events []domain.Event, batchName string) (int64, error) {
	args := mid.Called(ctx, events, batchName)
	return args.Get(0).(int64), args.Error(1)
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

type MockPipelineRepository struct {
	mer *MockEventRepository
}

func (m *MockPipelineRepository) Event() port.EventRepository {
	return m.mer
}

func (m *MockPipelineRepository) DeadLetterEvent() port.DeadLetterEventRepository {
	return m.mer
}

type MockTxManager struct {
	mpr *MockPipelineRepository
}

func (m *MockTxManager) WithTx(ctx context.Context, fn func(port.PipelineRepository) error) error {
	return fn(m.mpr)
}
