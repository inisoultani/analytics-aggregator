package service

import (
	"analytics-aggregator/internal/config"
	"analytics-aggregator/internal/core/domain"
	"analytics-aggregator/internal/core/port"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type insertAction func(context.Context, []domain.Event, string) (int64, error)

type PipelineService struct {
	mainCtx                   context.Context
	txManager                 port.TxManager
	dataEnricher              port.DataEnricher
	enrichWorkerSize          int
	enrichWorkerList          []*EnricherWorker
	workerPoolChan            chan *EnricherWorker
	pipelineJobChan           chan *domain.Event
	batchChan                 chan *domain.Event
	deadLetterChan            chan *domain.Event
	wgWorker                  sync.WaitGroup
	wgBatchEvents             sync.WaitGroup
	wgBatchDLE                sync.WaitGroup
	wgStoreRetry              sync.WaitGroup
	wgJobDistributor          sync.WaitGroup
	insertBatchSize           int
	rejectedCount             atomic.Int64
	successCount              atomic.Int64
	deadLetterCount           atomic.Int64
	backoffMulitplier         time.Duration
	batchInsertTickerDuration time.Duration
	enricherWorkerDuration    time.Duration
	storeTimeDuration         time.Duration
}

func NewPipelineService(ctx context.Context, txManager port.TxManager, de port.DataEnricher, cfg *config.Config) *PipelineService {
	// initiating pipeline service
	return &PipelineService{
		mainCtx:                   ctx,
		txManager:                 txManager,
		dataEnricher:              de,
		workerPoolChan:            make(chan *EnricherWorker, cfg.EnricherWorkerSize),
		pipelineJobChan:           make(chan *domain.Event, cfg.PipelineJobSize),
		insertBatchSize:           cfg.InsertBatchSize,
		enrichWorkerSize:          cfg.EnricherWorkerSize,
		enrichWorkerList:          []*EnricherWorker{},
		batchChan:                 make(chan *domain.Event, cfg.InsertBatchSize),
		deadLetterChan:            make(chan *domain.Event, cfg.InsertBatchSize),
		backoffMulitplier:         cfg.BackoffMulitplier,
		batchInsertTickerDuration: time.Duration(3 * time.Second),
		enricherWorkerDuration:    time.Duration(5 * time.Second),
		storeTimeDuration:         time.Duration(100 * time.Millisecond),
	}
}

func (p *PipelineService) Open(ctx context.Context) {
	// initiating workers during service initiation
	for i := range p.enrichWorkerSize {
		workerCtx, workerCancelFunc := context.WithCancelCause(ctx)
		w := &EnricherWorker{
			id:                           i + 1,
			jobChan:                      make(chan *domain.Event),
			dataEnricher:                 p.dataEnricher,
			cancelFunc:                   workerCancelFunc,
			batchChan:                    p.batchChan,
			workerPool:                   p.workerPoolChan,
			deadLetterChan:               p.deadLetterChan,
			enricherWorkerTickerDuration: p.enricherWorkerDuration,
			backoffMultiplier:            time.Second,
		}
		p.enrichWorkerList = append(p.enrichWorkerList, w)
		p.wgWorker.Add(1)
		go w.DataEnricherProcess(workerCtx, &p.wgWorker)
	}

	// initiating job distributor
	p.wgJobDistributor.Add(1)
	go p.jobDistributor(ctx, &p.wgJobDistributor)

	// initiating batch process for events
	p.wgBatchEvents.Add(1)
	go p.batchInsert(ctx, &p.wgBatchEvents, "events", p.batchChan, p.insertEvents)

	// initiating batch process for dead-letter-events
	p.wgBatchDLE.Add(1)
	go p.batchInsert(ctx, &p.wgBatchDLE, "dead-letter-events", p.deadLetterChan, p.insertDeadLetterEvents)

	slog.Info("Pipeline openned, ready to ingest events")
}

func (p *PipelineService) ProcessAndStore(ctx context.Context, e *domain.Event) (int64, error) {
	p.wgStoreRetry.Add(1)

	// we use main context from the main flow
	// to ensure go routines in pipeline service survive the http response
	// and still aware of application level shutdown signal
	go p.storeWithRetry(p.mainCtx, e, &p.wgStoreRetry)

	return 1, nil
}

func (p *PipelineService) storeWithRetry(ctx context.Context, e *domain.Event, wg *sync.WaitGroup) {
	defer wg.Done()

	for e.RetryCount < 3 {

		attempCtx, cancelAttempt := context.WithTimeout(ctx, p.storeTimeDuration)
		select {
		case p.pipelineJobChan <- e:
			cancelAttempt()
			slog.Debug("storeWithRetry success",
				slog.String("event_id", e.ID.String()),
				slog.Int("retry_count", e.RetryCount))
			return

		case <-attempCtx.Done():
			cancelAttempt()
			// if context.DeadlineExceeded -> channel is geneuinely full (backpressure effect)
			// if context.Cancelled -> channel receive signal shutdown from the main flow
			if errors.Is(context.Cause(attempCtx), context.Canceled) {
				slog.Debug("storeWithRetry attempCtx interrupted",
					slog.String("event_id", e.ID.String()),
					slog.Int("retry_count", e.RetryCount),
					slog.Any("err", context.Cause(attempCtx)),
				)
				lastBreathToDLE("pipeline_store_attemptCtx_closed", e, p.deadLetterChan)
				return
			}
		}

		// retry flow
		e.RetryCount++
		retryDuration := time.Duration(e.RetryCount*2) * p.backoffMulitplier
		select {
		case <-time.After(retryDuration):
			slog.Debug("Failed to store data into the pipeline",
				slog.String("event_id", e.ID.String()),
				slog.Int("retry_count", e.RetryCount),
				slog.Duration("retry_backoff_time", retryDuration),
				slog.Any("err", domain.ErrFailedToPushToPipeline))
			continue
		case <-ctx.Done():
			slog.Debug("storeWithRetry_mechanism_interrupted",
				slog.String("event_id", e.ID.String()),
				slog.Int("retry_count", e.RetryCount),
				slog.Any("err", context.Cause(ctx)))
			lastBreathToDLE("pipeline_store_with_retry_closed", e, p.deadLetterChan)
			return
		}
	}
	p.rejectedCount.Add(1)
	e.ErrorReason = "pipeline_store_with_retry_maxed"
	sendToDLE(ctx, e, e.ErrorReason, p.deadLetterChan)
}

func (p *PipelineService) jobDistributor(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	slog.Info("Initiating job distributor...")

	for {
		select {
		case e, ok := <-p.pipelineJobChan:
			if !ok {
				slog.Debug("jobDistributor mechanism is closed",
					slog.Any("err", context.Cause(ctx)),
				)
				return
			}
			p.assignWorker(ctx, e)
		case <-ctx.Done():
			slog.Debug("jobDistributor mechanism interrupted",
				slog.Any("err", context.Cause(ctx)))
			return
		}
	}
}

func (p *PipelineService) assignWorker(ctx context.Context, e *domain.Event) {

	slog.Debug("assignWorker processing event",
		slog.String("event_id", e.ID.String()),
	)

	var worker *EnricherWorker
	select {
	case ew := <-p.workerPoolChan:
		// worker succesfully acquired
		worker = ew
	case <-ctx.Done():
		slog.Debug("get worker in assignWorker failed",
			slog.String("event_id", e.ID.String()),
			slog.Any("err", context.Cause(ctx)),
		)
		lastBreathToDLE("get_worker_failed_context_closed", e, p.deadLetterChan)
		return
	}

	select {
	case worker.jobChan <- e:
		// job succesfully handed to worker channel
	case <-ctx.Done():
		slog.Debug("assignWorker job failed",
			slog.String("event_id", e.ID.String()),
			slog.Any("err", context.Cause(ctx)),
		)
		lastBreathToDLE("assign_worker_job_failed_context_closed", e, p.deadLetterChan)
		return
	}
}

func (p *PipelineService) batchInsert(ctx context.Context, wg *sync.WaitGroup, batchName string, c <-chan *domain.Event, insertAction insertAction) {
	defer wg.Done()

	duration := p.batchInsertTickerDuration
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	slog.Info("Initiating batchInsert with ticker...",
		slog.String("batch_id", batchName),
		slog.Duration("tick_duration", duration),
	)

	events := make([]domain.Event, 0, p.insertBatchSize)
	for {
		select {
		case <-ticker.C:
			events = p.checkAndInsert(ctx, events, "timer_ticker", 0, batchName, insertAction)
		case e, ok := <-c:
			if !ok {
				slog.Info("Batch channel in pipeline is closed...", slog.String("batch_id", batchName))
				slog.Info("Checking remaining left data in collections...", slog.String("batch_id", batchName))
				_ = p.checkAndInsert(ctx, events, "batch_channel_closed", 0, batchName, insertAction)
				return
			}
			events = append(events, *e)
			events = p.checkAndInsert(ctx, events, "event_arrival", p.insertBatchSize-1, batchName, insertAction)
			// ONLY reset the ticker if we actually flushed the batch
			// we know a flush happened if checkAndInsert reduced the length to 0
			if len(events) == 0 {
				ticker.Reset(duration)
			}
		case <-ctx.Done():
			slog.Debug("batchInsert mechanism interrupted",
				slog.String("batch_id", batchName),
				slog.Any("err", context.Cause(ctx)))
			// flush remaining data before exit this func to ensure no data loss
			_ = p.checkAndInsert(context.Background(), events, "batch_channel_interrupted", 0, batchName, insertAction)
			return
		}
	}
}

func (p *PipelineService) checkAndInsert(ctx context.Context, events []domain.Event, phase string, min int, batchName string, insertAction insertAction) []domain.Event {
	if len(events) > min {
		slog.Debug("trigger batch insert",
			slog.String("batch_id", batchName),
			slog.String("via", phase),
		)
		affectedRecs, err := insertAction(ctx, events, batchName)
		if err != nil {
			slog.Error("Batch insert failed", slog.String("batch_id", batchName))
		}
		slog.Debug("Batch insert succed",
			slog.String("batch_id", batchName),
			slog.Int64("affected_records", affectedRecs),
		)
		clear(events)
		events = events[:0]
	} else {
		slog.Debug("no data exist / not meet the treshold to trigger insert",
			slog.String("batch_id", batchName),
			slog.String("via", phase),
		)
	}
	return events
}

func (p *PipelineService) insertEvents(ctx context.Context, events []domain.Event, batchName string) (int64, error) {
	recs, err := p.executeInserts(ctx, batchName, func(pr port.PipelineRepository) (int64, error) {
		return pr.Event().CreateEvents(ctx, events)
	})
	if err != nil {
		return 0, err
	}
	p.successCount.Add(recs)
	return recs, nil
}

func (p *PipelineService) insertDeadLetterEvents(ctx context.Context, events []domain.Event, batchName string) (int64, error) {
	recs, err := p.executeInserts(ctx, batchName, func(pr port.PipelineRepository) (int64, error) {
		return pr.DeadLetterEvent().CreateDeadLetters(ctx, events)
	})
	if err != nil {
		return 0, err
	}
	p.deadLetterCount.Add(recs)
	return recs, nil
}

func (p *PipelineService) executeInserts(ctx context.Context, batchName string, insertFn func(port.PipelineRepository) (int64, error)) (int64, error) {
	var recs int64
	err := p.txManager.WithTx(ctx, func(pr port.PipelineRepository) error {
		affectedRecs, err := insertFn(pr)
		if err != nil {
			slog.Error("executeInserts error",
				slog.String("batch_id", batchName),
				slog.Any("err", err),
			)
			return err
		}

		slog.Info("Successfully executeInserts",
			slog.String("batch_id", batchName),
			slog.Int64("affected_records", affectedRecs),
		)
		recs = affectedRecs
		return nil
	})

	if err != nil {
		return 0, err
	}
	return recs, nil
}

func (p *PipelineService) Close() {

	// wait all of ongoing storeWithRetry
	p.wgStoreRetry.Wait()
	slog.Debug("p.wgStoreRetry.Wait DONE")

	// close pipeline job
	close(p.pipelineJobChan)
	slog.Debug("close pipelineJobChan DONE")

	// wait ongoing job distributor process
	p.wgJobDistributor.Wait()
	slog.Debug("p.wgJobDistributor.Wait DONE")

	for _, ew := range p.enrichWorkerList {
		close(ew.jobChan)
		if ew.cancelFunc != nil {
			ew.cancelFunc(fmt.Errorf("shutdown_pipeline"))
		}
	}
	// wait all worker to be terminated
	p.wgWorker.Wait()
	slog.Debug("p.wgWorker.Wait DONE")

	close(p.workerPoolChan)
	slog.Debug("close workerPoolChan DONE")

	for ew := range p.workerPoolChan {
		slog.Debug("Draining remaing worker in workerPool channel",
			slog.Int("id", ew.id),
		)
	}
	slog.Debug("draining and closing workerPoolChan DONE")

	// close batch channel & will trigger ticker to stop
	close(p.batchChan)
	slog.Debug("close batchChan DONE")

	// wait batch process to terminated
	p.wgBatchEvents.Wait()
	slog.Debug("p.wgBatch.Wait DONE")

	// close deadLetter channel & will trigger ticker to stop
	close(p.deadLetterChan)
	slog.Debug("close deadLetterChan DONE")

	// wait batch process to terminated
	p.wgBatchDLE.Wait()
	slog.Debug("p.wgBatchDLE.Wait DONE")

	slog.Debug("Service total data",
		slog.Int64("rejected", p.rejectedCount.Load()),
		slog.Int64("succed", p.successCount.Load()),
		slog.Int64("dead_lettered", p.deadLetterCount.Load()),
	)
}

type EnricherWorker struct {
	id                           int
	jobChan                      chan *domain.Event
	dataEnricher                 port.DataEnricher
	cancelFunc                   context.CancelCauseFunc
	batchChan                    chan *domain.Event
	workerPool                   chan *EnricherWorker
	deadLetterChan               chan *domain.Event
	enricherWorkerTickerDuration time.Duration
	backoffMultiplier            time.Duration
}

func (e *EnricherWorker) DataEnricherProcess(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	slog.Info("EnricherWorker initiating",
		slog.Int("id", e.id),
	)

	inPool := false
	tickerDuration := e.enricherWorkerTickerDuration
	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()
	for {
		if !inPool {
			select {
			case <-ctx.Done():
				slog.Info("EnricherWorker interrupted during entering workerPool",
					slog.Int("id", e.id),
					slog.Any("caused_by", context.Cause(ctx)),
				)
				return
			case e.workerPool <- e:
				inPool = true
			}
		}

		select {
		case event, ok := <-e.jobChan:
			if !ok {
				slog.Info("EnricherWorker has complete its duty, shuting down the worker now",
					slog.Int("id", e.id),
				)
				return
			}
			inPool = false
			e.enriceWithPanicHandling(ctx, event)
			ticker.Reset(tickerDuration)

			// drain the ticker, remove any stale tick while the worker busy previously
			select {
			case <-ticker.C:
			default:
			}
		case <-ticker.C:
			slog.Debug("EnricherWorker done sleeping, still waiting for job...",
				slog.Int("id", e.id),
			)
		case <-ctx.Done():
			slog.Info("EnricherWorker interrupted",
				slog.Int("id", e.id),
				slog.Any("caused_by", context.Cause(ctx)),
			)
			return
		}
	}
}

func (e *EnricherWorker) enriceWithPanicHandling(ctx context.Context, event *domain.Event) {
	// handle panic here
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()

			slog.Error("worker caught panic, shielding worker pool..,",
				slog.String("event_id", event.ID.String()),
				slog.Any("stack", string(stack)),
				slog.Any("panic_reason", r),
			)

			reason := fmt.Sprintf("Panic : %v, stack : %v", r, string(stack))
			lastBreathToDLE(reason, event, e.deadLetterChan)
		}
	}()

	e.enrichWithRetry(ctx, event)
}

func (e *EnricherWorker) enrichWithRetry(ctx context.Context, event *domain.Event) {

	err := e.enrich(ctx, event)
	if err != nil {
		slog.Debug("Enrich process error, entering retry flow",
			slog.String("event_id", event.ID.String()),
			slog.Any("err", err),
		)
		event.RetryCount = 0
		for event.RetryCount < 3 {
			event.RetryCount++
			retryDuration := time.Duration(event.RetryCount*2) * e.backoffMultiplier
			select {
			case <-ctx.Done():
				msg := "EnrichWithRetry_mechanism_interrupted"
				slog.Debug(msg,
					slog.String("event_id", event.ID.String()),
					slog.Int("retry_count", event.RetryCount),
					slog.Any("err", context.Cause(ctx)))
				// push to dead letter to avoid message loss, probably triggered during shutdown
				lastBreathToDLE(fmt.Sprintf(msg+", event_id: %s, retry_count: %d", event.ID.String(), event.RetryCount), event, e.deadLetterChan)
				return
			case <-time.After(retryDuration):
				err := e.enrich(ctx, event)
				if err == nil {
					return
				}
				slog.Debug("Enrich retry error",
					slog.String("event_id", event.ID.String()),
					slog.Int("retry_count", event.RetryCount),
					slog.Any("err", err),
				)
			}
		}
		// if after multiple retry stil failed, push to deadletter pipeline
		slog.Debug("Enrich retry reach max attempts, will send the event to DLE",
			slog.String("event_id", event.ID.String()),
			slog.Int("retry_count", event.RetryCount),
			slog.Any("err", err),
		)
		event.ErrorReason = "EnrichWithRetry_reach_max_attempt"
		sendToDLE(ctx, event, "enricher_worker", e.deadLetterChan)
	}

}

func (e *EnricherWorker) enrich(ctx context.Context, event *domain.Event) error {
	start := time.Now()

	// intentionally use different context for each api call
	// since on each attempt, it will already marked as Canceled (DeadlineExceeded).
	// to ensure no data loss during enrichment process by
	timeout := time.Duration(1000 * time.Millisecond)
	cause := fmt.Errorf("api call timeout, waiting time was : %dms", timeout.Milliseconds())
	apiCtx, cancelFunc := context.WithTimeoutCause(context.Background(), timeout, cause)
	defer cancelFunc()

	enrichData, err := e.dataEnricher.EnrichIp(apiCtx, event.ClientIP)
	if err != nil {
		slog.Error("Failed to fetch enrich data",
			slog.Int("id", e.id),
			slog.String("event_id", event.ID.String()),
			slog.String("client_ip", event.ClientIP),
			slog.Any("err", err),
		)
		return err
	}
	event.EnrichedData = enrichData

	select {
	case <-ctx.Done():
		slog.Warn("Pipeline shutting down, failed to drop message to batch channel",
			slog.Any("event", event),
			slog.Any("err", context.Cause(ctx)),
		)
		return context.Cause(ctx)
	case e.batchChan <- event:
		slog.Debug("EnricherWorker finish enriching data",
			slog.Int("id", e.id),
			slog.String("event_id", event.ID.String()),
			slog.Duration("duration", time.Since(start)),
		)
	}

	return nil
}

func lastBreathToDLE(reason string, e *domain.Event, deadLetterChan chan<- *domain.Event) {
	e.ErrorReason = reason
	// since the origin ctx alread cancelled here, we create "last-breath" ctx
	// to ensure event had the time to reach DLE
	lastBreathCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	sendToDLE(lastBreathCtx, e, e.ErrorReason, deadLetterChan)
}

func sendToDLE(ctx context.Context, event *domain.Event, processName string, deadLetterChan chan<- *domain.Event) {
	select {
	case <-ctx.Done():
		slog.Warn("Pipeline shutting down, failed to drop message to DLE",
			slog.String("process_name", processName),
			slog.Any("event", event),
		)
	case deadLetterChan <- event:
		slog.Debug("Send event to DLE",
			slog.String("process_name", processName),
			slog.String("event_id", event.ID.String()),
		)
	}
}
