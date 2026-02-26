package service

import (
	"analytics-aggregator/internal/config"
	"analytics-aggregator/internal/core/domain"
	"analytics-aggregator/internal/core/port"
	"context"
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
}

func NewPipelineService(ctx context.Context, txManager port.TxManager, de port.DataEnricher, cfg *config.Config) *PipelineService {
	// initiating pipeline service
	s := &PipelineService{
		txManager:       txManager,
		dataEnricher:    de,
		enrichWorkers:   make([]*EnricherWorker, cfg.EnricherWorkerNum),
		workerPoolChan:  make(chan *EnricherWorker, cfg.EnricherWorkerNum),
		batchChan:       make(chan *domain.Event),
		pipelineJobChan: make(chan *domain.Event),
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
		s.workerPoolChan <- w
		s.wgWorker.Add(1)
		go w.DataEnricherProcess(workerCtx, i, s.batchChan, &s.wgWorker)
	}

	// initiating job distributor
	s.wgJobDistributor.Add(1)
	go s.jobDistributor(ctx)

	// initiating batch process
	s.wgBatch.Add(1)
	go s.batchInsert(ctx, &s.wgBatch)
	return s
}

func (p *PipelineService) ProcessAndStore(ctx context.Context, e *domain.Event) (int64, error) {

	// for i := range events {
	// 	enrichData, err := p.dataEnricher.EnrichIp(ctx, events[i].ClientIP)
	// 	if err != nil {
	// 		slog.Error("Failed to fetch enrich data",
	// 			slog.String("event_id", events[i].ID.String()),
	// 			slog.String("client_ip", events[i].ClientIP),
	// 			slog.Any("err", err))
	// 	} else {
	// 		events[i].EnrichedData = enrichData
	// 	}
	// }

	// recs := int64(0)
	// err := p.txManager.WithTx(ctx, func(aar port.AnalyticsAggregatorRepository) error {

	// 	affectedRecs, err := aar.Event().CreateEvents(ctx, events)
	// 	if err != nil {
	// 		slog.Error("create events error", slog.Any("err", err))
	// 		return err
	// 	}

	// 	recs = affectedRecs
	// 	return nil
	// })
	// max_retry:
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

func (p *PipelineService) jobDistributor(ctx context.Context) {
	defer p.wgJobDistributor.Done()

	time.Sleep(10 * time.Second)
	for {
		select {
		case e, ok := <-p.pipelineJobChan:
			if !ok {
				slog.Debug("jobDistributor mechanism is closed",
					slog.Any("err", context.Cause(ctx)),
				)
				return
			}
			p.assignWorker(ctx, e, &p.wgWorker)
		case <-ctx.Done():
			slog.Debug("jobDistributor mechanism interrupted",
				slog.Any("err", context.Cause(ctx)))
		}
	}
}

func (p *PipelineService) assignWorker(ctx context.Context, e *domain.Event, wg *sync.WaitGroup) {
	defer wg.Done()

	time.Sleep(8 * time.Second)
	slog.Debug("assignWorker processing event",
		slog.Any("event", e),
	)
	ew := <-p.workerPoolChan
	ew.jobChan <- e
}

func (p *PipelineService) batchInsert(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(500 * time.Millisecond)
	slog.Info("Initiating batchInsert with ticker...",
		slog.Any("ticker", ticker),
	)

	for {
		select {
		case <-ticker.C:
			events := make([]domain.Event, 0)
			select {
			case e, ok := <-p.batchChan:
				if !ok {
					slog.Info("Batch channel in pipeline is closed...")
					return
				}
				events = append(events, *e)
				slog.Debug("trigger batch insert")
				p.insertEvents(ctx, events)
			default:
				slog.Debug("no data in the batch channel")
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

type EnricherWorker struct {
	jobChan           chan *domain.Event
	enrichmentService port.DataEnricher
	cancelFunc        context.CancelCauseFunc
}

func (e *EnricherWorker) DataEnricherProcess(ctx context.Context, id int, batchChan chan<- *domain.Event, wg *sync.WaitGroup) {
	defer wg.Done()

	slog.Info("EnricherWorker initiating",
		slog.Int("id", id),
	)
	for {
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
