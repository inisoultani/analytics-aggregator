package service

import (
	"analytics-aggregator/internal/config"
	"analytics-aggregator/internal/core/domain"
	"analytics-aggregator/internal/core/port"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type PipelineService struct {
	txManager        port.TxManager
	dataEnricher     port.DataEnricher
	enrichWorkers    []*EnricherWorker
	workerPoolChan   chan *EnricherWorker
	pipelineJobChan  chan *domain.Event
	batchChan        chan *domain.Event
	wgWorker         sync.WaitGroup
	wgBatch          sync.WaitGroup
	wgStoreRetry     sync.WaitGroup
	wgJobDistributor sync.WaitGroup
	insertBatchSize  int
}

func NewPipelineService(ctx context.Context, txManager port.TxManager, de port.DataEnricher, cfg *config.Config) *PipelineService {
	// initiating pipeline service
	s := &PipelineService{
		txManager:       txManager,
		dataEnricher:    de,
		enrichWorkers:   make([]*EnricherWorker, cfg.EnricherWorkerNum),
		workerPoolChan:  make(chan *EnricherWorker, cfg.EnricherWorkerNum),
		batchChan:       make(chan *domain.Event),
		pipelineJobChan: make(chan *domain.Event, 10),
		insertBatchSize: cfg.InsertBatchSize,
	}
	// initiating workers during service initiation
	for i := range cfg.EnricherWorkerNum {
		workerCtx, workerCancelFunc := context.WithCancelCause(ctx)
		w := &EnricherWorker{
			jobChan:           make(chan *domain.Event),
			enrichmentService: s.dataEnricher,
			cancelFunc:        workerCancelFunc,
		}
		s.enrichWorkers[i] = w
		s.wgWorker.Add(1)
		go w.DataEnricherProcess(workerCtx, i, s.batchChan, s.workerPoolChan, &s.wgWorker)
	}

	// initiating job distributor
	s.wgJobDistributor.Add(1)
	go s.jobDistributor(ctx, &s.wgJobDistributor)

	// initiating batch process
	s.wgBatch.Add(1)
	go s.batchInsert(ctx, &s.wgBatch)
	return s
}

func (p *PipelineService) ProcessAndStore(ctx context.Context, e *domain.Event) (int64, error) {
	p.wgStoreRetry.Add(1)
	ctx = context.WithoutCancel(ctx)
	go p.storeWithRetry(ctx, e, &p.wgStoreRetry)

	return 1, nil
}

func (p *PipelineService) storeWithRetry(ctx context.Context, e *domain.Event, wg *sync.WaitGroup) {
	defer wg.Done()

	for e.RetryCount < 3 {
		e.RetryCount++
		retryDuration := time.Duration(e.RetryCount*2) * time.Second
		select {
		case p.pipelineJobChan <- e:
			return
		case <-time.After(retryDuration):
			slog.Debug("Failed to store data into the pipeline",
				slog.String("event_id", e.ID.String()),
				slog.Int("retry_count", e.RetryCount),
				slog.Duration("retry_backoff_time", retryDuration),
				slog.Any("err", domain.ErrFailedToPushToPipeline))
		case <-ctx.Done():
			slog.Debug("storeWithRetry mechanism interrupted",
				slog.String("event_id", e.ID.String()),
				slog.Int("retry_count", e.RetryCount),
				slog.Any("err", context.Cause(ctx)))
		}
	}
}

func (p *PipelineService) jobDistributor(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	slog.Info("Initiating job distributor...")

	// time.Sleep(10 * time.Second)
	for {
		select {
		case e, ok := <-p.pipelineJobChan:
			if !ok {
				slog.Debug("jobDistributor mechanism is closed",
					slog.Any("err", context.Cause(ctx)),
				)
				return
			}
			p.assignWorker(e)
		case <-ctx.Done():
			slog.Debug("jobDistributor mechanism interrupted",
				slog.Any("err", context.Cause(ctx)))
		}
	}
}

func (p *PipelineService) assignWorker(e *domain.Event) {
	// defer wg.Done()

	time.Sleep(1 * time.Second)
	slog.Debug("assignWorker processing event",
		slog.Any("event", e),
	)
	ew := <-p.workerPoolChan
	ew.jobChan <- e
}

func (p *PipelineService) batchInsert(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(3000 * time.Millisecond)
	slog.Info("Initiating batchInsert with ticker...",
		slog.Any("ticker", ticker),
	)

	events := make([]domain.Event, 0)
	for {
		select {
		case <-ticker.C:
			if len(events) > 0 {
				slog.Debug("trigger batch insert via timer ticker")
				p.insertEvents(ctx, events)
				clear(events)
				events = events[:0]
			} else {
				slog.Debug("no data exist during timer tick")
			}
		case e, ok := <-p.batchChan:
			if !ok {
				slog.Info("Batch channel in pipeline is closed...")
				ticker.Stop()
				return
			}
			events = append(events, *e)
			if len(events) >= p.insertBatchSize {
				slog.Debug("trigger batch insert via max size")
				p.insertEvents(ctx, events)
				clear(events)
				events = events[:0]
			}
		case <-ctx.Done():
			slog.Debug("batchInsert mechanism interrupted",
				slog.Any("err", context.Cause(ctx)))
		}
	}

}

func (p *PipelineService) insertEvents(ctx context.Context, events []domain.Event) {

	err := p.txManager.WithTx(ctx, func(aar port.AnalyticsAggregatorRepository) error {

		affectedRecs, err := aar.Event().CreateEvents(ctx, events)
		if err != nil {
			slog.Error("create events error", slog.Any("err", err))
			return err
		}

		slog.Info("Successfully insertEvents",
			slog.Int64("affected_records", affectedRecs),
		)
		return nil
	})

	if err != nil {
		slog.Error("insertEvents batch process failed",
			slog.Any("err", err),
		)
	}
}

func (p *PipelineService) Close() {
	// close pipeline job
	close(p.pipelineJobChan)
	slog.Debug("close pipelineJobChan DONE")

	// wait all of ongoing storeWithRetry
	p.wgStoreRetry.Wait()
	slog.Debug("p.wgStoreRetry.Wait DONE")

	// wait ongoing job distributor process
	p.wgJobDistributor.Wait()
	slog.Debug("p.wgJobDistributor.Wait DONE")

	close(p.workerPoolChan)
	slog.Debug("close workerPoolChan DONE")

	// draining worker pool, close  job channel and terminate each worker
	for ew := range p.workerPoolChan {
		close(ew.jobChan)
		ew.cancelFunc(fmt.Errorf("shutdown pipeline"))
	}
	slog.Debug("draining workerPoolChan DONE")

	// close(p.workerPoolChan)
	// slog.Debug("close workerPoolChan DONE")

	// wait all worker to be terminated
	p.wgWorker.Wait()
	slog.Debug("p.wgWorker.Wait DONE")

	// close batch channel & will trigger ticker to stop
	close(p.batchChan)
	slog.Debug("close batchChan DONE")

	// wait batch process to terminated
	p.wgBatch.Wait()
	slog.Debug("p.wgBatch.Wait DONE")

}

type EnricherWorker struct {
	jobChan           chan *domain.Event
	enrichmentService port.DataEnricher
	cancelFunc        context.CancelCauseFunc
}

func (e *EnricherWorker) DataEnricherProcess(ctx context.Context, id int, batchChan chan<- *domain.Event, workerPool chan<- *EnricherWorker, wg *sync.WaitGroup) {
	defer wg.Done()

	slog.Info("EnricherWorker initiating",
		slog.Int("id", id),
	)

	inPool := false
	for {
		if !inPool  {
			select {
			case workerPool <- e:
				inPool = true
			case <-ctx.Done():
				slog.Info("EnricherWorker interrupted during entering workerPool",
					slog.Int("id", id),
					slog.Any("caused_by", context.Cause(ctx)),
				)
			}
		}

		select {
		case event, ok := <-e.jobChan:
			if !ok {
				slog.Info("EnricherWorker has complete its duty, shuting down the worker now",
					slog.Int("id", id),
				)
				return
			}
			start := time.Now()
			enrichData, err := e.enrichmentService.EnrichIp(ctx, event.ClientIP)
			if err != nil {
				slog.Error("Failed to fetch enrich data",
					slog.Int("id", id),
					slog.String("event_id", event.ID.String()),
					slog.String("client_ip", event.ClientIP),
					slog.Any("err", err),
				)
			} else {
				event.EnrichedData = enrichData
			}
			slog.Debug("EnricherWorker finish enriching data",
				slog.Int("id", id),
				slog.String("event_id", event.ID.String()),
				slog.Duration("duration", time.Since(start)),
			)
			batchChan <- event
			inPool = false
		case <-time.After(5 * time.Second):
			slog.Debug("EnricherWorker done sleeping, still waiting for job...",
				slog.Int("id", id),
			)
		case <-ctx.Done():
			slog.Info("EnricherWorker interrupted",
				slog.Int("id", id),
				slog.Any("caused_by", context.Cause(ctx)),
			)
			return
		}
	}
}
